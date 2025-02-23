import requests
from google.cloud import storage
import logging

BUCKET_NAME = "analytics-engineering-zoomcamp"
CREDENTIALS_FILE = "extended-argon-450312-b0-6f510748e526.json"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
# green, yellow, fhv
DATA_TYPE = "fhv"

logger = logging.getLogger(__name__)


def generate_urls(
    base_url: str,
    dataset: str,
    start_year: int,
    end_year: int,
    start_month: int,
    end_month: int,
) -> list:
    """
    Generate the urls for each required month and year
    """
    url_list = []

    for year in range(start_year, end_year + 1):
        for month in range(start_month, end_month + 1):
            # Format the month to ensure two digits
            month_str = f"{month:02d}"
            url_ = f"{base_url}{dataset}/{dataset}_tripdata_{year}-{month_str}.csv.gz"
            url_list.append(url_)

    return url_list


if __name__ == "__main__":

    urls = generate_urls(BASE_URL, DATA_TYPE, 2019, 2019, 1, 12)
    logger.info("defined list of urls: %s", ", ".join(urls))

    storage_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
    bucket = storage_client.bucket(BUCKET_NAME)

    gcs_files = []
    for url in urls:
        file_name = url.split("/")[-1]
        gcs_blob = bucket.blob(f"{DATA_TYPE}/{file_name}")

        logger.info("Downloading %s and uploading to GCS as %s", url, file_name)
        try:
            response = requests.get(url, timeout=300)
            gcs_blob.upload_from_string(response.content)
            gcs_files.append(f"gs://{BUCKET_NAME}//{file_name}")
        except Exception as e:
            logger.info("failed on extraction: %s", e)
            raise e
