import logging
from time import perf_counter


LOG_FILE = './logfile.log'
LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL, filename=LOG_FILE, filemode='w',
					format='%(asctime)-15s %(levelname)-8s %(message)s')
logger = logging.getLogger('logger')


def func_status(func):
    def wrapper(*args, **kwargs):
        logger.info(f'Entered function: {func.__name__}')
        time_start = perf_counter()
        wrapped_func = func(*args, **kwargs)
        time_end = perf_counter() - time_start
        logger.info(f"""Exited function: {func.__name__} 
                     with running time: {time_end:0.4} seconds""")
        return wrapped_func
    return wrapper