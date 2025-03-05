import logging
import sys


def set_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],  # Send logs to stdout
    )
    return logging.getLogger(__name__)
