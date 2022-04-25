import logging
import os

from logging import handlers

def get_ingestion_logger(data_source):
    """Returns logger for ingestion-scripts 
    :param data_source: Name of the source data used in ingestion (rss, reddit, twitter)
    :type data_source: str
    """
    log_base_dir = "/consumers/logs"
    log_dir = os.path.join(log_base_dir, data_source)
    log_filename = f'{data_source}_ingestion.log'
    log_complete_path = os.path.join(log_dir, log_filename)

    logger = logging.getLogger(f'ingestion_logger_{data_source}')

    # Cut log file when it reaches 1MB in size, keep 2 Backups
    log_handler = handlers.RotatingFileHandler(log_complete_path, maxBytes=1000000, backupCount=2)
    log_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt='%(asctime)s | LEVEL: %(levelname)s | %(message)s', datefmt='%Y-%m-%d,%H:%M:%S')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    
    return logger, log_handler