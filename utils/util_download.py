# -*- coding: utf-8 -*-
__author__ = 'shijin'
"""
Download module
"""
from utils.util_logfile import slogger, flogger, nlogger, UndefinedError
import os
import sys
import wget
import traceback
import time


class WGetError(Exception):
    pass


def get_file_path(file_name, root_path, file_relative_path=None, **kwargs):
    """
    Get the file path, if not, create it.
    :param root_path: root path, absolute path
    :param file_relative_path: The relative path of the file
    :param file_name: The name of the file
    :param kwargs:
    :return: The absolute path of the file or raise exception
    """
    try:
        assert isinstance(file_name, str) and str(file_name).strip(), "Parameter file_name must be string " \
                                                                      "and not be empty"
        assert file_name.find('/') == -1, "Parameter file_name cannot contain a path"
        _file_name = str(file_name).strip()

        assert isinstance(root_path, str) and str(root_path).strip(), "Parameter root_path must be string " \
                                                                      "and not be empty"
        assert root_path.startswith('/') is True, "Parameter root_path must be absolute path"
        _root_path = str(root_path).strip()

        if file_relative_path is None or file_relative_path == '':
            _file_relative_path = ''
        else:
            assert isinstance(file_relative_path, str), "Parameter file_relative_path must be string"
            assert file_relative_path.startswith('/') is False, "Parameter file_relative_path must be relative path"
            _file_relative_path = str(file_relative_path).strip()

        _absolute_path = os.path.join(_root_path, _file_relative_path)
        try:
            if os.path.exists(_absolute_path) is False:
                # exist_ok = True, if directory exists, no error will be reported.
                os.makedirs(_absolute_path, exist_ok=True)
        except FileExistsError as e:
            print(f'FileExistsError: {repr(e)}')
            pass

        _absolute_path_file = os.path.join(_absolute_path, _file_name)
        return _absolute_path_file
    except Exception as e:
        nlogger.error('{fn} error: {e}'.format(fn='get_file_path', e=traceback.format_exc()))
        raise UndefinedError('{fn} error: {e}'.format(fn='get_file_path', e=repr(e)))


def check_file_exist(absolute_path_file, **kwargs):
    """
    check if the file exists
    :param absolute_path_file: absolute path of the file
    :param kwargs:
    :return: True or False
    """
    if os.path.isfile(absolute_path_file):
        return True
    else:
        return False


def download_video(download_url, absolute_path_file, file_name, **kwargs):
    """
    Use wget module to download video
    :param download_url: temporary download url
    :param absolute_path_file: storage absolute path of video file
    :param file_name: file_name of video
    :param kwargs:
    :return:
    """
    try:
        _start_time = time.time()
        nlogger.info('Download start: {f}.'.format(f=absolute_path_file))

        try:
            download_file_name = wget.download(download_url, out=absolute_path_file, bar=None)

            _end_time = time.time()
            nlogger.info(
                'Download success: {f}, it takes {t:.2f}s.'.format(f=absolute_path_file, t=_end_time - _start_time))
            slogger.info(
                'Download success: {f}, its storage path is {p}.'.format(f=file_name, p=absolute_path_file))

            return download_file_name
        except Exception as e:
            nlogger.error('Download failed: {f}, storage path is {p},WGet error: {e}.'.format(f=file_name,
                                                                                              p=absolute_path_file,
                                                                                              e=traceback.format_exc()))
            raise WGetError('Download failed: {f}, WGet error: {e}.'.format(f=file_name, e=repr(e)))

    except WGetError as e:
        flogger.error(str(e))
        return
    except Exception as e:
        nlogger.error('{fn} download {f} error: {e}'.format(fn='download_video', f=file_name, e=traceback.format_exc()))
        flogger.error("Download failed: {f}, error: {e}".format(f=file_name, e=repr(e)))
        return


if __name__ == "__main__":

    if len(sys.argv) == 1:
        print("请选择正确的参数：'normal' or 'force'")
        sys.exit(0)
        # exec_action = ['users', 'mysql', 'database', 'table', 'queryuser', 'querymysql']
    else:
        if len(sys.argv) == 2 and sys.argv[1] == 'normal':
            force_download = False
        elif len(sys.argv) == 2 and sys.argv[1] == 'force':
            force_download = True
        else:
            print("请选择正确的参数：'normal' or 'force'")
            sys.exit(0)

    url = 'http://dl.videocc.net/5c0ad4c56c/source_5c0ad4c56c85e31c9e3728e6550dbfd4_5?downloadId=5c0ad4c56c' \
          '&times=1613142804599&ran=4a43ba4740b1d1ccb962dc3ce6634fc0&sign=7d36967964856d72aa286f4c3008c447'
    file_path = '/data/download'
    filename = 'test1'
    # force_download = False

    download_video(url, file_path, filename)
