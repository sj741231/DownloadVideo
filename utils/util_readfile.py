# -*- coding: utf-8 -*-
__author__ = 'shijin'
"""
Read file tools
"""
import os
import sys
import filetype

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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


def guess_file_type(file_name):
    """
    Guess file type
    :param file_name:  absolute file path
    :return: info fo file type
    """
    try:
        kind = filetype.guess(file_name)
        if kind is None:
            return f'Cannot guess {file_name} file type!'
        return f'File extension: {kind.extension}; File MIME type: {kind.mime}'
    except Exception as e:
        return f'Guess file type error: {str(e)}'


if __name__ == '__main__':
    print('5c0ad4c56c85e31c9e3728e6550dbfd4_5: ',
          guess_file_type('/data/download/中保协/最佳实践实例萃取/5c0ad4c56c85e31c9e3728e6550dbfd4_5'))
    print('5c0ad4c56c52f8c63985b5dd8315f1de_5: ',
          guess_file_type('/data/download/中保协/最佳实践实例萃取/5c0ad4c56c52f8c63985b5dd8315f1de_5'))
