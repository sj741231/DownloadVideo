# -*- coding: utf-8 -*-
__author__ = 'shijin'
"""
Download module
"""
from utils.util_logfile import slogger, flogger, nlogger, UndefinedError
from utils.util_requester import download_file_requester
import os
import sys
import wget
import traceback
import time
import socket
import requests
from requests.exceptions import RequestException
import urllib
from urllib.error import URLError, HTTPError, ContentTooShortError
import random
from contextlib import closing

# set socket default timeout
SOCKET_TIMEOUT = 1800


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


def get_temp_file_name(absolute_path_file, **kwargs):
    return f"{absolute_path_file}.tmp"


def check_temp_file_exists(absolute_path_file, **kwargs):
    """
    check if temp file exists
    :param absolute_path_file:
    :param kwargs:
    :return: The size of temp file
    """
    _temp_file = get_temp_file_name(absolute_path_file, **kwargs)
    if os.path.isfile(_temp_file):
        _temp_size = os.path.getsize(_temp_file)
        return _temp_size
    else:
        return 0


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
        nlogger.info('Download start:{f}.'.format(f=absolute_path_file))

        for i in range(3):
            try:
                # Because wget.download use ulib.urlretrieve, it has no timeout
                # So set socket.setdefaulttimeout(3600) to prevent jamming
                socket.setdefaulttimeout(SOCKET_TIMEOUT)
                download_file_name = wget.download(download_url, out=absolute_path_file, bar=None)
                _end_time = time.time()
                nlogger.info(
                    'Download success:{f}, it takes {t:.2f}s.'.format(f=absolute_path_file, t=_end_time - _start_time))
                slogger.info(
                    'Download success:{f}, its storage path is {p}.'.format(f=file_name, p=absolute_path_file))

                return download_file_name
            except socket.timeout:
                error_msg = f"Download timeout: {file_name},storage path is {absolute_path_file},url is{download_url}"
                nlogger.error(error_msg)
                if i > 1:
                    raise WGetError(error_msg)
                else:
                    time.sleep(1)
                    continue
            except HTTPError as e:
                error_msg = "Download HTTPError:{0}, {1}, {2}, storage path is {3}".format(file_name,
                                                                                           download_url,
                                                                                           e.code,
                                                                                           absolute_path_file)
                nlogger.error(error_msg)
                if i > 1:
                    raise WGetError(error_msg)
                else:
                    time.sleep(random.randint(1, 2))
                    continue
            except URLError as e:
                error_msg = "Download URLError:{0}, {1}, {2}, storage path is {3}".format(file_name,
                                                                                          download_url,
                                                                                          e.reason,
                                                                                          absolute_path_file)
                nlogger.error(error_msg)
                if i > 1:
                    raise WGetError(error_msg)
                else:
                    time.sleep(random.randint(1, 2))
                    continue
            except Exception as e:
                nlogger.error('Download failed:{f}, storage path is {p},WGet error: {e}'.format(f=file_name,
                                                                                                p=absolute_path_file,
                                                                                                e=traceback.format_exc()))
                raise WGetError('Download failed:{f},storage path is {p},WGet error: {e}'.format(f=file_name,
                                                                                                 p=absolute_path_file,
                                                                                                 e=repr(e)))
        else:
            error_msg = 'Download failed:{f},storage path is {p},WGet retry failed'.format(f=file_name,
                                                                                           p=absolute_path_file)
            nlogger.error(error_msg)
            raise WGetError(error_msg)
    except WGetError as e:
        flogger.error(repr(e))
        return
    except Exception as e:
        nlogger.error('{fn} download {f} error: {e}'.format(fn='download_video', f=file_name, e=traceback.format_exc()))
        flogger.error("Download failed:{f}, error: {e}".format(f=file_name, e=repr(e)))
        return


"""
    1、当使用requests的get下载大文件/数据时，建议使用使用stream模式。
    当把get函数的stream参数设置成False时，它会立即开始下载文件并放到内存中，如果文件过大，有可能导致内存不足。
    当把get函数的stream参数设置成True时，它不会立即开始下载，当你使用iter_content或iter_lines遍历内容或访问内容属性时才开始下载。
    需要注意一点：文件没有下载之前，它也需要保持连接。
    iter_content：一块一块的遍历要下载的内容
    iter_lines：一行一行的遍历要下载的内容
    使用上面两个函数下载大文件可以防止占用过多的内存，因为每次只下载小部分数据
    
    2、使用with closing 来确保请求连接关闭
"""


def get_requester():
    pass


def get_download_file_size(download_url, **kwargs):
    # res_length = requests.get(download_url, stream=True)
    with closing(download_file_requester.get_url(url=download_url, stream=True)) as res:
        total_size = int(res.headers['Content-Length'])
        return total_size


def download_small_file(download_url, absolute_path_file, file_name, retry, **kwargs):
    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, '
                      'like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
    }

    for i in range(retry):
        try:
            with closing(download_file_requester.get_url(url=download_url, stream=True, headers=headers)) as res:
                with open(absolute_path_file, mode='wb') as f:
                    f.write(res.content)
            return f.name
        except RequestException as e:
            error_msg = f"Download failed::{file_name}, storage path is {absolute_path_file}," \
                        f"RequestException error: {repr(e)}"
            nlogger.error(error_msg)
            continue
        except Exception as e:
            error_msg = f"Download failed:{file_name}, storage path is {absolute_path_file}, undefined error: %s"
            nlogger.error(error_msg % (traceback.format_exc()))
            flogger.error(error_msg % (repr(e)))
            return
    else:
        error_msg = f"Download failed:{file_name}, storage path is {absolute_path_file}, {retry} retries failed"
        nlogger.error(error_msg)
        flogger.error(error_msg)
        return


def download_large_file(download_url, absolute_path_file, file_name, chunk_size, retry, **kwargs):
    _temp_size = check_temp_file_exists(absolute_path_file)

    headers = {
        'Range': 'bytes=%d-' % _temp_size,
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, '
                      'like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
    }

    for i in range(retry):
        try:
            with closing(download_file_requester.get_url(url=download_url, stream=True, headers=headers)) as res:
                with open(absolute_path_file, 'ab+') as f:
                    for chunk in res.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
            return f.name
        except RequestException as e:
            error_msg = f"Download failed::{file_name}, storage path is {absolute_path_file}," \
                        f"RequestException error: {repr(e)}"
            nlogger.error(error_msg)
            continue
        except Exception as e:
            error_msg = f"Download failed:{file_name}, storage path is {absolute_path_file}, undefined error: %s"
            nlogger.error(error_msg % (traceback.format_exc()))
            flogger.error(error_msg % (repr(e)))
            return
    else:
        error_msg = f"Download failed:{file_name}, storage path is {absolute_path_file}, {retry} retries failed"
        nlogger.error(error_msg)
        flogger.error(error_msg)
        return


def download_file(download_url, absolute_path_file, file_name, chunk_size=4096 * 1024, retry=3, **kwargs):
    _temp_size = check_temp_file_exists(absolute_path_file)
    _total_size = get_download_file_size(download_url, **kwargs)
    retry = retry if isinstance(retry, int) and retry < 10 else 5
    chunk_size = chunk_size if isinstance(chunk_size, int) and chunk_size < 4096 * 1024 else chunk_size

    if _total_size < chunk_size:
        f = download_small_file(download_url, absolute_path_file, file_name, retry, **kwargs)
    else:
        f = download_large_file(download_url, absolute_path_file, file_name, chunk_size, retry, **kwargs)


class Download(object):
    download_url = None
    storage_file_name = None
    short_file_name = None
    chunk_size = 4096 * 1024
    retry = 3

    def __init__(self, chunk_size=4096 * 1024, retry=3, **kwargs):
        self.chunk_size = chunk_size if isinstance(chunk_size, int) and chunk_size < 4096 * 1024 else chunk_size
        self.retry = retry if isinstance(retry, int) and retry < 10 else 3
        self.get_kwargs(**kwargs)

    def get_kwargs(self, **kwargs):
        try:
            for k, v in kwargs.items():
                if hasattr(self, f'get_attribute_{k}'):
                    _v = getattr(self, f'get_attribute_{k}')(**kwargs)
                    setattr(self, k, _v)
                else:
                    setattr(self, k, v)
        except:
            raise NotImplementedError(f'get_kwargs error: {traceback.format_exc()}')

    def get_attribute_download_url(self, **kwargs):
        self.download_url = str(kwargs.get('url')).strip() if kwargs.get('url') and str(kwargs.get('url')).startswith(
            "http") else None
        return self.download_url

    def get_attribute_storage_file_name(self, **kwargs):
        self.storage_file_name = str(kwargs.get('storage_file_name')).strip() if kwargs.get('storage_file_name') else ''
        return self.storage_file_name

    def get_attribute_short_file_name(self, **kwargs):
        self.short_file_name = str(kwargs.get('short_file_name')).strip() if kwargs.get('short_file_name') else None
        return self.short_file_name


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
