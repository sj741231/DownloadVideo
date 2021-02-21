# -*- coding:utf-8 -*-
__author__ = 'shijin'

import os
import sys
import getopt
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from utils.util_logfile import nlogger, flogger, traceback
from utils.util_xlsx import HandleXLSX
from utils.util_requester import BeautifulSoup, requester, RequestException
from utils.util_re import is_url
from utils.util_download import get_file_path, download_video, check_file_exist
from datetime import datetime
from settings import BASE_URL, TASK_WAITING_TIME, FORCE_DOWNLOAD, MAX_WORKERS, DL_ROOT_PATH
from row_object import RowStatus


class GetURLError(Exception):
    pass


class GetRowIterError(Exception):
    pass


class GetStorageError(Exception):
    pass


class DownloadVideoError(Exception):
    pass


class ThreadTaskError(Exception):
    pass


class WriteResultError(Exception):
    pass


def exec_func(file_name, sheet_name=None, start_point=None, end_point=None, **kwargs):
    """
    Executive Function
    :param file_name: Excel file name
    :param sheet_name: sheet name, default active sheet
    :param start_point: start row number, minimum is 2 ( row 1 is column name)
    :param end_point: end row number , maximum is the row number of sheet
    :param kwargs:
    :return:
    """
    try:
        _file_name = check_file_name(file_name, **kwargs)
        _source_xls, _row_object_iterator = get_row_object_iterator(_file_name, sheet_name, start_point, end_point,
                                                                    **kwargs)
        _download_result = download_video_thread(_row_object_iterator, **kwargs)
        write_result_to_xls(_source_xls, _download_result)
    except (GetURLError, GetRowIterError, GetStorageError, DownloadVideoError, ThreadTaskError, WriteResultError) as e:
        nlogger.error('{fn} Custom error: {e}'.format(fn='exec_func', e=repr(e)))
        print(f'Custom error: {repr(e)}')
    except AssertionError as e:
        nlogger.error('{fn} Assertion error: {e}'.format(fn='exec_func', e=repr(e)))
        print(repr(e))
    except Exception as e:
        nlogger.error('{fn} error: {e}'.format(fn='exec_func', e=traceback.format_exc()))
        print(f'Undefined error: {repr(e)}')


def check_file_name(file_name, **kwargs):
    """
    check if the file exists
    :param file_name:
    :param kwargs:
    :return:
    """
    assert file_name is not None, "Parameter file_name must be provided and is not None."
    _file_name = str(file_name).strip()
    assert os.path.exists(_file_name), "file_name {f} does not exists".format(f=_file_name)
    return _file_name


def get_row_object_iterator(file_name, sheet_name=None, start_point=None, end_point=None, **kwargs):
    """
    get iterator of row object
    :param file_name:
    :param sheet_name:
    :param start_point:
    :param end_point:
    :param kwargs:
    :return: instance of HandleXLSX object, iterator of row object
    """
    try:
        source_xls = HandleXLSX(file_name, sheet_name)
        row_object_iterator = source_xls.generate_row_object_iterator(sheet_name, start_point, end_point, **kwargs)
        return source_xls, row_object_iterator
    except Exception as e:
        nlogger.error('{fn} error: {e}'.format(fn='get_row_object_iterator', e=traceback.format_exc()))
        raise GetRowIterError('{fn} error: {e}'.format(fn='get_row_object_iterator', e=repr(e)))


def download_video_thread(row_object_iterator, **kwargs):
    """
    Download by multi Thread
    :param row_object_iterator:
    :param kwargs:
    :return: The result of download, it's list
    """
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    try:
        download_result = []
        all_task = [executor.submit(download_video_task, row_object) for row_object in row_object_iterator]

        for future in as_completed(all_task, timeout=TASK_WAITING_TIME):
            data = future.result()
            if data:
                download_result.append(data)
        nlogger.info(f'Download completed, {len(download_result)} files downloaded')
        return download_result
    except TimeoutError as e:
        nlogger.error("{fn} TimeoutError: {e}".format(fn='download_video_thread', e=repr(e)))
        executor.shutdown(False)  # 不等待future 返回直接关闭资源
        raise ThreadTaskError('{fn} TimeoutError: {e}'.format(fn='download_video_thread', e=repr(e)))
    except Exception as e:
        nlogger.error("{fn} error: {e}".format(fn='download_video_thread', e=traceback.format_exc()))
        flogger.error("{fn} error: {e}".format(fn='download_video_thread', e=repr(e)))
        raise ThreadTaskError('{fn} error: {e}'.format(fn='download_video_thread', e=repr(e)))


def download_video_task(row_object, root_path=DL_ROOT_PATH, force_download=FORCE_DOWNLOAD, **kwargs):
    """
    Download task
    :param row_object: row object
    :param root_path:  storage root path
    :param force_download: force download sign
    :param kwargs:
    :return:  The Row object that processed by task
    """
    try:
        check_row_object(row_object, **kwargs)
        _pre_row_object = get_storage_absolute_path(row_object, root_path, force_download, **kwargs)
        _mdl_row_object = get_video_url(_pre_row_object, **kwargs)
        _post_row_object = download_video_from_url(_mdl_row_object, **kwargs)
        return _post_row_object
    except AssertionError as e:
        row_object.status = RowStatus.ERROR.value
        nlogger.error("{fn} Params error: {e}".format(fn='download_video_task', e=traceback.format_exc()))

        if hasattr(row_object, 'column_value') and isinstance(row_object.column_value, dict):
            row_object.column_value['result'] = f'Params AssertionError: {str(e)}'
            _vid = row_object.column_value.get('vid', 'Unknown vid')
        else:
            setattr(row_object, 'column_value', {'result': f'Params AssertionError: {str(e)}'})
            _vid = 'Unknown vid'
        flogger.error("download_video_task failed:{f},AssertionError:{e}".format(f=_vid, e=repr(e)))
        return row_object
    except GetURLError as e:
        row_object.status = RowStatus.ERROR.value
        _vid = row_object.column_value.get('vid', 'Unknown vid')
        flogger.error("download_video_task failed:{f},GetURLError:{e}".format(f=_vid, e=repr(e)))
        row_object.column_value['result'] = f'GetURLError: {str(e)}'
        return row_object
    except GetStorageError as e:
        row_object.status = RowStatus.ERROR.value
        _vid = row_object.column_value.get('vid', 'Unknown vid')
        flogger.error("download_video_task failed:{f},GetStorageError:{e}".format(f=_vid, e=repr(e)))
        row_object.column_value['result'] = f'GetStorageError: {str(e)}'
        return row_object
    except DownloadVideoError as e:
        row_object.status = RowStatus.ERROR.value
        _vid = row_object.column_value.get('vid', 'Unknown vid')
        flogger.error("download_video_task failed:{f},DownloadVideoError:{e}".format(f=_vid, e=repr(e)))
        row_object.column_value['result'] = f'DownloadVideoError: {str(e)}'
        return row_object
    except Exception as e:
        row_object.status = RowStatus.ERROR.value
        nlogger.error("{fn} sn:{s}, vid:{v}, undefined error: {e}".format(fn='download_video_task',
                                                                          s=row_object.column_value['sn'],
                                                                          v=row_object.column_value['vid'],
                                                                          e=traceback.format_exc(), ))
        row_object.column_value['result'] = f'download_video_task undefined error: {str(e)}'
        _vid = row_object.column_value.get('vid', 'Unknown vid')
        flogger.error("download_video_task undefined failed:{f},DownloadVideoError:{e}".format(f=_vid, e=repr(e)))
        return row_object


def check_row_object(row_object, **kwargs):
    """
    check Row object property
    :param row_object:
    :param kwargs:
    :return:
    """
    assert isinstance(row_object.file_name, str) and str(
        row_object.file_name).strip(), "Invalid parameter file_name: {p}".format(p=str(row_object.file_name).strip())
    assert isinstance(row_object.sheet_name, str) and str(
        row_object.sheet_name).strip(), "Invalid parameter sheet_name: {p}".format(p=str(row_object.sheet_name).strip())
    assert row_object.column_name and isinstance(row_object.column_name,
                                                 (list, tuple)), "Parameter column_name is list and not be empty"
    assert row_object.row_value and isinstance(row_object.row_value,
                                               (list, tuple)), "Parameter row_value is list and not be empty"
    assert row_object.column_value and isinstance(row_object.column_value,
                                                  dict), "Parameter column_value is dict and not be empty"
    assert row_object.position and isinstance(row_object.position,
                                              int), "Parameter position is int and not be less than 1"
    assert row_object.status and RowStatus(row_object.status).name == 'INITIAL', "Parameter status is invalid"


def get_video_url(pre_row_object, **kwargs):
    """
    Get temporary download URL
    :param pre_row_object:
    :param kwargs:
    :return:
    """
    try:
        # File already exists, skip getting URL
        if RowStatus(pre_row_object.status).name == 'SKIP':
            pre_row_object.temporary_url = None
            return pre_row_object
        else:
            # requester = Requester(username=None, password=None, baseurl=BASE_URL, ssl_verify=True, cert=None,
            #                       timeout=(15, 60), **dict())
            # data = {"vid_text": "5c0ad4c56c85e31c9e3728e6550dbfd4_5"}
            data = {"vid_text": pre_row_object.column_value.get('vid')}

            response = requester.post_multipart_and_confirm_status(url=BASE_URL, data=data)
            soup = BeautifulSoup(response.text, "html.parser")
            # print(soup2.prettify())

            _temporary_url = soup.text.strip()
            if is_url(_temporary_url):
                pre_row_object.temporary_url = _temporary_url
                nlogger.info(
                    "Get URL success, sn:{s}, vid:{v}, url:{u}.".format(s=pre_row_object.column_value.get('sn'),
                                                                        v=pre_row_object.column_value.get('vid'),
                                                                        u=pre_row_object.temporary_url))
                return pre_row_object
            else:
                raise ValueError(
                    "Get URL invalid, sn:{s}, vid:{v}, url:{u}.".format(s=pre_row_object.column_value.get('sn'),
                                                                        v=pre_row_object.column_value.get('vid'),
                                                                        u=_temporary_url))
    except RequestException as e:
        nlogger.error("{fn} RequestException: {e}".format(fn='get_video_url', e=repr(e)))
        raise GetURLError("{fn} RequestException: {e}".format(fn='get_video_url', e=repr(e)))
    except Exception as e:
        nlogger.error("{fn} error: {e}".format(fn='get_video_url', e=traceback.format_exc()))
        raise GetURLError("{fn} error: {e}".format(fn='get_video_url', e=repr(e)))


def get_storage_absolute_path(row_object, root_path, force_download, **kwargs):
    """
    Get absolute patch of file storage
    :param row_object:
    :param root_path:
    :param force_download:
    :param kwargs:
    :return:
    """
    try:
        file_path = "{p}".format(p=row_object.column_value.get('path'))
        file_name = "{f}".format(f=row_object.column_value.get('vid'))

        _storage_absolute_path = get_file_path(file_name, root_path, file_path, **kwargs)
        row_object.storage_absolute_path = _storage_absolute_path

        _exist = check_file_exist(_storage_absolute_path, **kwargs)
        if _exist is False:
            row_object.status = RowStatus.DOWNLOAD.value
        else:
            if force_download is True:
                os.remove(_storage_absolute_path)
                row_object.status = RowStatus.FORCE.value
            else:
                row_object.status = RowStatus.SKIP.value

        return row_object
    except Exception as e:
        nlogger.error("{fn} error: {e}".format(fn='get_storage_absolute_path', e=traceback.format_exc()))
        raise GetStorageError("{fn} error: {e}".format(fn='get_storage_absolute_path', e=repr(e)))


def download_video_from_url(mdl_row_object, **kwargs):
    """
    Download video from temporary URL
    :param mdl_row_object:
    :param kwargs:
    :return:
    """
    try:
        url = mdl_row_object.temporary_url
        file_name = "{f}".format(f=mdl_row_object.column_value.get('vid'))
        absolute_path_file = mdl_row_object.storage_absolute_path

        if RowStatus(mdl_row_object.status).name == 'SKIP':
            mdl_row_object.column_value['result'] = f'Already exists: {absolute_path_file}'
            nlogger.info(
                'Download skip: {f}, its storage path is {p}.'.format(f=file_name, p=absolute_path_file))
        else:
            dl_file_name = download_video(url, absolute_path_file, file_name, **kwargs)
            if dl_file_name:
                if RowStatus(mdl_row_object.status).name == 'FORCE':
                    mdl_row_object.column_value['result'] = f'Download again: {dl_file_name}'
                else:
                    mdl_row_object.column_value['result'] = f'Download successful: {dl_file_name}'
                mdl_row_object.status = RowStatus.SUCCESS.value
            else:
                mdl_row_object.column_value['result'] = f"Download failed: {file_name}"
                mdl_row_object.status = RowStatus.FAILURE.value
        return mdl_row_object
    except Exception as e:
        nlogger.error("{fn} error: {e}".format(fn='download_video_from_url', e=traceback.format_exc()))
        raise DownloadVideoError("{fn} error: {e}".format(fn='download_video_from_url', e=repr(e)))


def write_result_to_xls(source_xls, download_result):
    """
    Write result in sheet of Excel
    :param source_xls:
    :param download_result:
    :return:
    """
    try:
        column_name_list = source_xls.get_column_name_list()
        for row_object in download_result:
            x = row_object.position
            y = column_name_list.index('result') + 1
            values = (x, y, row_object.column_value.get('result', 'unknown'))
            source_xls.write_sheet_rows_value(sheet_name=row_object.sheet_name, values=values)

        _dl_file_name = "dl_{d}.xlsx".format(d=datetime.now().strftime('%Y%m%d-%H:%M:%S'))
        source_xls.save(_dl_file_name)
        nlogger.info(f'Write result completed, output file: {_dl_file_name}')
    except Exception as e:
        nlogger.error("{fn} error: {e}".format(fn='download_video_from_url', e=traceback.format_exc()))
        raise WriteResultError("{fn} error: {e}".format(fn='download_video_from_url', e=repr(e)))


def usage():
    """
    Command help
    :return:
    """
    info = \
        """
Usage:
    python3.9 handle.py -file [ -sheet -start -end ]
    Help:
     -h --help   
     
    Mandatory options:
     -f --file    <source excel>
    
    Optional options:
     -t --sheet   <The sheet name in Excel, default active sheet in Excel>
     -s --start   <Excel start row. Must be int,default index 2>
     -e --end     <Excel end row. Must be int,default Maximum number of rows in Excel>
        """
    print(info)


def main(argv):
    """
    Entrance function
    :param argv: command parameters
    :return:
    """
    _file_name = None
    _sheet_name = None
    _start_point = None
    _end_point = None

    try:
        opts, args = getopt.getopt(argv, "hf:t:s:e:", ["help", "file", "sheet", "start", "end"])  # 短选项模式
    except getopt.GetoptError:
        print("Usage 1: python3.9 handle.py -h  -f <source excel>  -t <excel sheet>  -s <start row index>  "
              "-e <end row index> \nUsage 2: python3.9 handle.py --help  --file <source excel>  --sheet <excel sheet>  "
              "--start <start row index>  --end <end row index>")
        sys.exit(2)  # 2 Incorrect Usage

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(0)
        elif opt in ('-f', '--file'):
            _file_name = str(arg).strip()
        elif opt in ('-t', '--sheet'):
            _sheet_name = str(arg).strip()
        elif opt in ('-s', '--start'):
            _start_point = str(arg).strip()
        elif opt in ('-e', '--end'):
            _end_point = str(arg).strip()

    if _file_name is None:
        print("Invalid parameter, -f --file must be provided. \nTry '-h --help' for more information.")
        sys.exit(2)

    if _start_point is not None and not _start_point.isdigit():
        print("Invalid parameter, -s --start must be followed by integer. \nTry '-h --help' for more information.")
        sys.exit(2)

    if _end_point is not None and not _end_point.isdigit():
        print("Invalid parameter, -e --end must be followed by integer. \nTry '-h --help' for more information.")
        sys.exit(2)

    params = dict(file_name=_file_name, sheet_name=_sheet_name, start_point=_start_point, end_point=_end_point)

    exec_func(**params)


if __name__ == "__main__":
    # file_name = EXCEL_FILE
    # sheet_name = 'listing'
    # start_point = 2
    # end_point = 3
    # kwargs = dict()
    # exec_func(file_name, sheet_name=None, start_point=None, end_point=None, **kwargs)
    main(sys.argv[1:])

    # python3.9 handle.py --file 'vid-20210214.xlsx' --sheet 'listing' --start 2 --end 4
    # python3.9 handle.py -f 'vid-20210214.xlsx' -t 'listing' -s 2 -e 4

    # print("***" * 30)
    # print(f'pre_row_object: {pre_row_object}', type(pre_row_object))
    # print(f'root_path: {root_path}', type(root_path))
    # print(f'force_download: {force_download}', type(force_download))
    # print("***" * 30)
