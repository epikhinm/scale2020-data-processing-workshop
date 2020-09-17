import os
import json
import time
import logging
import requests
from retrying import retry
from confluent_kafka import Producer


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s :: %(levelname)s :: %(message)s')


@retry(wait_exponential_multiplier=1000,
       wait_exponential_max=10000,
       stop_max_attempt_number=4)
def get_last_samples():
    """Get sensors data for last 5 minutes"""
    r = requests.get('https://data.sensor.community/static/v1/data.json',
                     timeout=15)
    r.raise_for_status()
    return r.json()


def delivery_callback(err, msg):
    """Log errors"""
    if not err:
        return
    logging.error(f'Message failed delivery: {err}')


def produce():
    """Load and export new samples"""
    logging.info('Loading samples')
    samples = get_last_samples()
    count = len(samples)
    logging.info(f'Loaded {count} samples')

    producer = Producer({
        'bootstrap.servers': os.environ['KAFKA_BROKERS'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'SCRAM-SHA-512',
        'sasl.password': os.environ['KAFKA_PASS'],
        'sasl.username': os.environ['KAFKA_USER'],
        'ssl.ca.location': '/usr/share/ca-certificates/mozilla/YandexCA.crt',
    })

    count = 0
    for sensor in samples:
        value = json.dumps(sensor).encode('utf-8')
        producer.produce('raw', value, on_delivery=delivery_callback)
        count = count + 1
    producer.flush()
    logging.info(f'Produced {count} events')


def main():
    """Event-Loop"""
    while True:
        start = time.time()
        produce()
        end = time.time()
        time_diff = (end - start)
        sleep_time = int(60.0 - time_diff)
        if sleep_time > 0:
            logging.info(f'Sleeping for {sleep_time}s')
            time.sleep(sleep_time)


if __name__ == '__main__':
    main()
