import logging
import os

from dotenv import load_dotenv
load_dotenv()
BASE_DIR_OF_LOGGER = os.getenv('BASE_DIR_OF_LOGGER')

def setup_logger(name, log_file, level):
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s',  datefmt='%m-%d-%Y %I:%M')
    
    """To setup as many loggers as you want"""
    file_handler = logging.FileHandler(log_file)        
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)

    return logger

info_logger = setup_logger('logger', f'{BASE_DIR_OF_LOGGER}/status_log.log', logging.INFO)
error_logger = setup_logger('error_logger', f'{BASE_DIR_OF_LOGGER}/error_log.log', logging.ERROR)