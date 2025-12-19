import os
import logging

BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379/0")
QUEUE_NAME = "events_queue"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator")
