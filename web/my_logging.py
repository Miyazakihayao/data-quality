import logging
from logging.handlers import RotatingFileHandler as RFHandler
import os


def configureLogger(logger, log_file, log_level, console_ouput=True):
    '''
    Only the root logger need to configure.  
    '''
    # create RotatingFileHandler and set level to debug
    global root_logger
    root_logger = logger
    if (log_file is not None):
        print("log_file=%s" % (log_file))
        fh = RFHandler(filename=log_file, mode='a', maxBytes=100 * 1024 * 1024, backupCount=10, encoding='utf-8')

        formatter = logging.Formatter(
            fmt='%(asctime)s,pid=%(process)d,%(name)s，%(levelname)s:%(message)s', datefmt="%H:%M:%S")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.setLevel(log_level)

    # create consoleHanlder
    if (console_ouput):
        ch = logging.StreamHandler()
        shortFormatter = logging.Formatter(
            fmt='%(asctime)s,pid=%(process)d,%(levelname)s:%(message)s', datefmt="%H:%M:%S")
        ch.setFormatter(shortFormatter)
        logger.addHandler(ch)
        logger.setLevel(log_level)


def getPathAndFileName(fullFileName):
    '''
    return (path, fileName)
    '''
    (dirName, fileName) = os.path.split(fullFileName)
    return (os.path.normpath(dirName), fileName)


def getLoggingFileName(py_main_file, log_short_path='logs'):
    (base_path, py_file_name) = getPathAndFileName(py_main_file)
    log_path = os.path.join(base_path, log_short_path)
    (log_short_name, ext) = os.path.splitext(py_file_name)
    if not os.path.exists(log_path):
        os.mkdir(log_path)
    return os.path.join(log_path, log_short_name + '.log')
