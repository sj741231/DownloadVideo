# -*- coding: utf-8 -*-
__author__ = 'shijin'

import os
import logging
import logging.handlers
import traceback


class UndefinedError(Exception):
    pass


class FunctionError(Exception):
    pass


def create_logger(filename='batch'):
    """
    Create logger
    :param filename:
    :return:
    """
    log_filename = str(filename).strip() + '.log'
    log_base_dir = os.path.dirname(os.path.abspath(log_filename))

    if not os.path.exists(log_base_dir):
        os.makedirs(log_base_dir)

    handler = logging.FileHandler(log_filename)
    fmt = '%(asctime)s|%(name)s|%(levelname)s|%(module)s.%(funcName)s|%(lineno)d|%(message)s'
    date_fmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, date_fmt)
    handler.setFormatter(formatter)
    logger = logging.getLogger(str(filename))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


nlogger = create_logger(filename='logs/normal')

slogger = create_logger(filename='logs/success')

flogger = create_logger(filename='logs/failed')
