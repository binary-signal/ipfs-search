#!/usr/bin/env python
# -*- coding: utf-8 -*-


from datetime import datetime
from timeit import default_timer as timer
import pika
from pika.credentials import PlainCredentials
import redis
from redis.exceptions import ConnectionError
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s  [%(levelname)-5.5s]  %(message)s",
                    datefmt='%Y-%m-%d %H:%M:%S', )


class IPSIndexer:
    def __init__(self):
        self.uptime = 0
        self.index_count = 0

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5672,
                credentials=PlainCredentials(username='ouzo',
                                             password='ironman')))

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='ipfs', durable=True)
        logging.info("Attached to queue ipfs")

        self.cache = redis.Redis(host='localhost', port=6379, db=10)
        logging.info("Connected to Redis")

    def callback(self, ch, method, properties, body):
        """ this function is called when worker receives a new article task to be processed """
        try:
            event = json.loads(str(body, 'utf-8'))
        except ValueError:
            logging.error('parsing json object failed [{}]'.format(event))
        else:
            c0 = self.cache.get(event['key'])  # first check cache
            if c0:  # cache  hit
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logging.info("cache hit: {} ".format(event['key']))
                return

            # FIXME parse event to json object before insertion
            # self.mongo.insert(event)

        finally:
            self.cache.set(event['key'], 'ok')
            #save to mongo db
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.index_count += 1
            logging.info("indexed file {}".format(event['key']))

    def go(self):
        self.uptime = timer()
        self.channel.basic_qos(prefetch_count=10)
        self.channel.basic_consume(self.callback, queue='ipfs')

        try:
            self.channel.start()
        except KeyboardInterrupt:
            logging.info("------ * Inter Plantetary Indexer received a SIGINT, shutting down gracefully * ------\n\n")
            self.channel.stop_consuming()
        finally:
            logging.info("Closing connection with RabbitMQ")
            self.connection.close()

            logging.info("Indexed {} files".format(self.index_count))
            logging.info("Uptime: {}".format(round(timer() - self.uptime, 1)))
            logging.info("Indexing rate ~ {} files/sec".format(
                round(float(self.index_count) / round(timer() - self.uptime, 1), 1)))


if __name__ == "__main__":
    indexer = IPSIndexer()

    logging.info("------ System online ------- {}".format(datetime.now()))
    try:
        indexer.go()
    except Exception as e:
        logging.info(e)
        sys.exit(-1)
