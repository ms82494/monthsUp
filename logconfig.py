import os
import sys
import logging

log_format = (
    '[%(asctime)s] %(levelname)-8s %(name)-13s %(message)s')
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[logging.StreamHandler(sys.stdout)]
)
