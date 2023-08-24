# copilot, please create logger.py
# - logger.py exposes function called 'get_logger(name: str)' that returns a 'logger instance'
# - i am not familiar with python logging, so i will leave the implementation to you
# - you can use the 'logging' module from the python standard library
# - the 'logger instance' should print logs to stderr, like this:
#   [2021-10-10 12:00:00] [INFO] [download_mysql] Downloading MySQL distribution
# - logger instance should provide the following log levels: DEBUG, INFO, WARN, ERROR

from datetime import datetime
import logging
import sys
def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s')
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

