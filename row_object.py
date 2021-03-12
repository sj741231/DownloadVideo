# -*- coding:utf-8 -*-
__author__ = 'shijin'
from enum import Enum
from enum import unique


@unique
class RowStatus(Enum):  # 枚举类
    """
    Enum Class marks Row object status
    """
    INITIAL = 1
    DOWNLOAD = 2
    FORCE = 3
    SKIP = 4
    BREAKPOINT = 5
    SUCCESS = 6
    FAILURE = 7
    CANCEL = 8
    ERROR = 9
    CHECK = 10
    EXISTENCE = 11
    NONEXISTENCE = 12


class RowObject(object):
    """
    Class of Row Object
    """
    file_name = None                # Excel file name
    sheet_name = None               # Sheet name in Excel file
    column_name = None              # column name in sheet, list
    row_value = None                # row value in sheet, list
    column_value = None             # {column: value}, dict
    position = None                 # row index in sheet
    temporary_url = None            # download url
    storage_absolute_path = None    # download file storage file
    status = None                   # object status
