import logging
from logging.handlers import TimedRotatingFileHandler
import datetime

class PyLogger:
    def __init__(self, log_filename='logs/2070H_googleads.log'):
        # Set up logging
        self.log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        # Create a TimedRotatingFileHandler that rotates daily
        self.log_handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=7, encoding='utf-8')
        self.log_handler.setFormatter(self.log_formatter)

        # Create a logger and add the handler
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.log_handler)

    def log_info(self, message):
        self.logger.info(message)

    def log_warning(self, message):
        self.logger.warning(message)

    def log_error(self, message):
        self.logger.error(message)

   
    def close_logger(self):
        # Close the log handler to release the file lock (optional)
        self.log_handler.close()