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
    logging.basicConfig(filename=log_complete_path,
                        filemode='w',
                        level=logging.INFO, 
                        format='%(asctime)s | LEVEL: %(levelname)s | %(message)s', 
                        datefmt='%Y-%m-%d,%H:%M:%S')
    logger = logging.getLogger(f'ingestion_logger_{data_source}')

    # Cut log file when it reaches 1MB in size, keep 2 Backups
    logHandler = handlers.RotatingFileHandler(log_complete_path, maxBytes=1000000, backupCount=2)
    logHandler.setLevel(logging.INFO)
    logger.addHandler(logHandler)

    return logger