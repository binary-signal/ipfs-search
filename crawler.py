import ipfsapi
import pika
from pika.exceptions import ChannelClosed, ConnectionClosed, ChannelError
from pika.credentials import PlainCredentials
import sys
import queue
import threading
import logging
import signal
import time
import json
from timeit import default_timer as timer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s  [%(levelname)-5.5s]  %(message)s",
                    datefmt='%Y-%m-%d %H:%M:%S', )


def post(channel):  # runs in poster thread
    while True:
        if not log_queque.empty():
            entry = log_queque.get(block=False)
            json_task = json.dumps(entry)
            try:
                channel.basic_publish(exchange='', body=json_task, routing_key='ipfs',
                                      properties=pika.BasicProperties(delivery_mode=2))  # make message persistent
            except (ChannelError, ChannelClosed, ConnectionClosed):
                logging.error("Channel error")
            else:
                logging.info("send to rabbitmq: {}".format(entry))
                connection.sleep(0.2)
        else:  # when queue empty wait for 5 seconds
            connection.sleep(5)


class Job(threading.Thread):
    """ represent a thread task that runs a job """

    def __init__(self, name, target, args):
        threading.Thread.__init__(self, name=name, target=target, args=args)

        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self.shutdown_flag = threading.Event()
        # ... Other thread setup code here ...

    def run(self):
        try:
            if self._target:
                self._target(*self._args, **self._kwargs)
        finally:
            del self._target, self._args, self._kwargs


class Watcher(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self, name=name)

        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self.shutdown_flag = threading.Event()
        # ... Other thread setup code here ...

    def run(self):

        # Connect to local node
        try:
            api = ipfsapi.connect('127.0.0.1', 5001)
            print(api)
        except ipfsapi.exceptions.ConnectionError as ce:
            print(str(ce))
            sys.exit(-1)

        for event in api.log_tail():
            # add to queue only dht event's
            if event['event'] == 'handleAddProvider':
                log_queque.put(event, block=False)



class ServiceExit(Exception):
    """ Custom exception which is used to trigger the clean exit
    of all running threads and the main program. """
    pass


def service_shutdown(signum, frame):
    logging.info("------ * Inter Planetary Seach Indexer received a SIGINT, shutting down gracefully * ------\n\n")
    raise ServiceExit


if __name__ == '__main__':
    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGQUIT, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)

    t_start = timer()

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host='localhost',
            port='5672',
            credentials=PlainCredentials(username="ouzo",
                                         password="ironman")))
    channel = connection.channel()
    channel.queue_declare(queue='ipfs', durable=True)
    logging.info("Declared ipfs queue")

    log_queque = queue.Queue()

    watcher = Watcher(name='Watcher')
    watcher.setDaemon(True)
    poster = Job(name='Poster', target=post, args=(channel,))
    poster.setDaemon(True)

    watcher.start()
    poster.start()
    logging.info(" ------ System online ------- ")
    try:
        while True:  # Keep the main thread running, otherwise signals are ignored.
            time.sleep(60)
            logging.info("ipfs log queue size is {}".format(log_queque.qsize()))

    except ServiceExit:
        # Terminate the running threads.
        # Set the shutdown flag on each thread to trigger a clean shutdown of each thread.
        watcher.shutdown_flag.set()
        poster.shutdown_flag.set()
        connection.close()
        logging.info("Total uptime {} sec".format(round(timer() - t_start, 1)))

    except Exception as e:
        logging.info(e)
        sys.exit(-1)
