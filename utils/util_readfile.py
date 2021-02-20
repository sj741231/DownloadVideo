# -*- coding: utf-8 -*-
__author__ = 'shijin'
"""
Read file tools
"""
from utils.util_logfile import nlogger, FunctionError
import traceback


def stream_iterator(file, chunk_size, **kwargs):
    """
    Lazy function to read a file piece by piece.
    Default chunk size: 4MB.
    """
    try:
        if chunk_size is None or not isinstance(chunk_size, int):
            chunk_size = 4096 * 1024

        with open(file, "rb") as f:
            while True:
                file_part = f.read(chunk_size)
                if file_part:
                    yield file_part
                else:
                    break
    except Exception as e:
        nlogger.error('{fn} error: {e}'.format(fn='stream_iterator', e=traceback.format_exc()))
        raise FunctionError('{fn} error: {e}'.format(fn='stream_iterator', e=repr(e)))


def file_iterator(file, encoding="utf-8"):
    """
    Lazy function to read a file line by line.
    Default encoding utf-8
    """
    try:
        if encoding is None:
            encoding = "utf-8"

        with open(file, "r", encoding=encoding) as f:
            for line in f:
                yield line
    except Exception as e:
        nlogger.error('{fn} error: {e}'.format(fn='stream_iterator', e=traceback.format_exc()))
        raise FunctionError('{fn} error: {e}'.format(fn='file_iterator', e=repr(e)))
