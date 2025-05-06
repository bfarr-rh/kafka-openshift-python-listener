import argparse
import logging
import os
import time

from kafka import KafkaConsumer


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.delay = int(get_arg('DELAY', args.delay))
    args.cgroup = get_arg('CONSUMER_GROUP', args.cgroup)
    return args


def main(args):
    consumer = KafkaConsumer(args.topic, group_id=args.cgroup, bootstrap_servers=args.brokers)
    for msg in consumer:
        out = msg.value if msg.value is not None else ""
        logging.info('received: ' + str(msg.value, 'utf-8'))
        time.sleep(args.delay)
    logging.info('exiting')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting kafka-python-listener')
    parser = argparse.ArgumentParser(
            description='listen for some stuff on kafka')
    parser.add_argument(
            '--brokers',
            help='The bootstrap servers, env variable KAFKA_BROKERS',
            default='localhost:9092')
    parser.add_argument(
            '--topic',
            help='Topic to publish to, env variable KAFKA_TOPIC',
            default='bones-brigade')
    parser.add_argument(
            '--delay',
            help='Seconds to delay between processing messages from topic',
            default='1')
    parser.add_argument(
            '--cgroup',
            help='Consumer Group',
            default='') 
    args = parse_args(parser)
    main(args)
