# -*- coding: utf-8 -*-
__author__ = 'shijin'
"""
Hash tools
"""
from utils.util_logfile import nlogger, FunctionError
import hashlib
import traceback


def get_file_md5(file, file_iterator, chunk_size=4096 * 1024, **kwargs):
    """
    Get a file md5 hash.
    Default read chunk size 4MB
    """
    try:
        md5_obj = hashlib.md5()
        for data in file_iterator(file, chunk_size, **kwargs):
            md5_obj.update(data)
        _hash = md5_obj.hexdigest()
        return str(_hash).upper()
    except Exception as e:
        nlogger.error('{fn} error: {e}'.format(fn='get_file_md5', e=traceback.format_exc()))
        raise FunctionError('{fn} error: {e}'.format(fn='get_file_md5', e=repr(e)))
