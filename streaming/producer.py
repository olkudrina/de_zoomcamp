import sys
import io
import csv
import time
import gzip
import json
import logging
from kafka import KafkaProducer
import requests
import pandas as pd

SERVER = "localhost:9092"
TOPIC_NAME = "green-trips"
BATCH_SIZE = 1000
DATA = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
COLUMNS_TO_SEND = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
]


def set_logger(log_level=logging.INFO):
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],  # Send logs to stdout
    )
    return logging.getLogger(__name__)


def get_data(data: str):
    try:
        response = requests.get(data, timeout=600)
        logger.info("Response code: %s", response.status_code)
        return response.content
    except Exception as e:
        logger.error("failed to retrieve the data = %s", e)
        raise e


def read_and_filter_data(r_content, columns):
    with gzip.open(io.BytesIO(r_content), "rt") as f:
        reader = csv.DictReader(f)
        df = pd.DataFrame(list(reader))

        df = df.reindex(columns, axis=1)
        logger.info("size of data is %i", df.shape[0])

    return df.to_dict("records")


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


if __name__ == "__main__":
    logger = set_logger()

    producer = KafkaProducer(
        bootstrap_servers=[SERVER], value_serializer=json_serializer
    )

    if producer.bootstrap_connected():
        logger.info("Successfully connected")

    content = get_data(DATA)
    data = read_and_filter_data(content, COLUMNS_TO_SEND)

    # start sending data
    t0 = time.time()

    for i in data:
        message = {"test_data": i, "event_timestamp": time.time() * 1000}
        producer.send(TOPIC_NAME, value=message)

    producer.flush()
    t1 = time.time()
    logger.info("took to send data: %f", t1 - t0)
