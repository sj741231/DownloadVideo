# -*- coding: utf-8 -*-
__author__ = 'shijin'
"""
Download module
"""

import os
import sys

# sys.path.append("/opt/dev/dlvideo/")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.util_logfile import slogger, flogger, nlogger, UndefinedError
from utils.util_requester import download_file_requester
from utils.util_re import is_url
import shutil
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
import hashlib

# set socket default timeout
SOCKET_TIMEOUT = 3600 * 12


class WGetError(Exception):
    pass


class DownloadError(Exception):
    pass


class PreDownloadError(Exception):
    pass


class PostDownloadError(Exception):
    pass


class InvalidFileError(Exception):
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
    """
    Get file size of download file from download url
    :param download_url:  download url
    :param kwargs:
    :return: file size
    """
    # res_length = requests.get(download_url, stream=True)
    with closing(download_file_requester.get_url(url=download_url, stream=True)) as res:
        total_size = int(res.headers['Content-Length'])
        return total_size


def download_small_file(download_url, absolute_path_file, file_name, retry, **kwargs):
    """
    Download small file
    :param download_url:  url
    :param absolute_path_file: the file including absolute path
    :param file_name: file name
    :param retry: retry time
    :param kwargs:
    :return: download file name or None
    """
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
    """
    Download large file from download url by breakpoint continuation
    :param download_url: url
    :param absolute_path_file: the file including absolute path
    :param file_name: file name
    :param chunk_size: chunk size
    :param retry: retry time
    :param kwargs:
    :return: file name or None
    """
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
    """
    Class of download file from url
    """
    parent_path = None
    chunk_size = 4096 * 1024
    retry = 3
    timeout = (10, 60)

    # download_url = None
    # storage_file_name = None
    # file_name = None

    def __init__(self, chunk_size=None, retry=None, **kwargs):
        self.chunk_size = chunk_size if isinstance(chunk_size,
                                                   int) and chunk_size < self.chunk_size * 2 else self.chunk_size
        self.retry = retry if isinstance(retry, int) and retry < self.retry * 2 else self.retry
        self.get_kwargs(**kwargs)

    def get_kwargs(self, **kwargs):
        """
        Get and check parameter in kwargs
        :param kwargs:
        :return:
        """
        try:
            for k, v in kwargs.items():
                if hasattr(self, f'get_attribute_{k}'):
                    _v = getattr(self, f'get_attribute_{k}')(**kwargs)
                    setattr(self, k, _v)
                else:
                    setattr(self, k, v)
        except:
            raise NotImplementedError(f'get_kwargs error: {traceback.format_exc()}')

    # def get_attribute_download_url(self, **kwargs):
    #     self.download_url = str(kwargs.get('url')).strip() if kwargs.get('url') and str(kwargs.get('url')).startswith(
    #         "http") else None
    #     return self.download_url

    def get_attribute_parent_path(self, **kwargs):
        """
        Get parent path.
        If parent_path exists, then get the absolute parent path of parent_path
        If parent_path is None, then get current absolute path of the file
        :param kwargs:
        :return:
        """
        _parent_path = str(kwargs.get('parent_path')).strip() if kwargs.get('parent_path') else None
        if _parent_path is None:
            self.parent_path = os.path.dirname(os.path.abspath(__file__))
        elif os.path.isabs(_parent_path):
            self.parent_path = _parent_path
        else:
            current_path = os.path.dirname(os.path.abspath(__file__))
            self.parent_path = os.path.join(current_path, _parent_path)
        return self.parent_path

    # def get_attribute_storage_file_name(self, **kwargs):
    #     self.storage_file_name = str(kwargs.get('storage_file_name')).strip() if kwargs.get('storage_file_name') else ''
    #     return self.storage_file_name

    # def get_attribute_file_name(self, **kwargs):
    #     self.file_name = str(kwargs.get('file_name')).strip() if kwargs.get('file_name') else None
    #     return self.file_name

    @staticmethod
    def get_temp_file_name(absolute_path_file, **kwargs):
        return f"{absolute_path_file}.tmp"

    @staticmethod
    def get_file_size(file_name):
        return os.path.getsize(file_name)

    def check_temp_file_exists(self, absolute_path_file, **kwargs):
        """
        check if temp file exists
        :param absolute_path_file:
        :param kwargs:
        :return: The size of temp file
        """
        _temp_file = self.get_temp_file_name(absolute_path_file, **kwargs)
        if os.path.isfile(_temp_file):
            _temp_size = self.get_file_size(_temp_file)
            return _temp_size
        else:
            return 0

    @staticmethod
    def get_download_file_size(download_url, absolute_path_file, file_name, retry, timeout, **kwargs):
        """
        Get file size of download file from download url
        :param download_url: url
        :param absolute_path_file: storage file including absolute path
        :param file_name: file name
        :param retry: retry times
        :param timeout: connect and wait timeout
        :param kwargs:
        :return:
        """
        # res_length = requests.get(download_url, stream=True)
        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, '
                          'like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
        }

        error_msg = None
        for i in range(retry):
            try:
                with closing(
                        download_file_requester.get_and_confirm_status(url=download_url, stream=True, headers=headers,
                                                                       timeout=timeout)) as res:
                    total_size = int(res.headers['Content-Length'])
                    return total_size
            except RequestException as e:
                error_msg = f"Download failed:{file_name}, storage path is {absolute_path_file}," \
                            f"RequestException error: {repr(e)}"
                continue
            except Exception as e:
                error_msg = f"Download failed:{file_name}, storage path is {absolute_path_file}, " \
                            f"undefined error: {traceback.format_exc()}"
                raise DownloadError(error_msg)
        else:
            if error_msg is not None:
                raise DownloadError(error_msg)
            else:
                raise DownloadError(
                    f"Download failed:{file_name}, storage path is {absolute_path_file}, unknown error")

    def get_absolute_path_file(self, storage_file_name, **kwargs):
        """
        Get absolute path of file.
        If storage_file_name is absolute path, then get storage_file_name.
        If storage_file_name is relative path and parent_path is not None, then get /parent_path/storage_file_name.
        If storage_file_name is relative path and parent_path is None, then get /current path/storage_file_name.
        :param storage_file_name: storage file path and filename
        :param kwargs:
        :return: absolute path of file or None
        """
        try:
            if os.path.isabs(storage_file_name) is False and self.parent_path:
                _absolute_path_file = os.path.join(self.parent_path, storage_file_name)
            else:
                _absolute_path_file = os.path.abspath(storage_file_name)
            return _absolute_path_file
        except Exception as e:
            print(f'Exception: {repr(e)}')

    @staticmethod
    def get_parent_path(absolute_path_file, **kwargs):
        try:
            _parent_path = os.path.dirname(os.path.abspath(absolute_path_file))

            if os.path.exists(_parent_path) is False:
                # exist_ok = True, if directory exists, no error will be reported.
                os.makedirs(_parent_path, exist_ok=True)
            return _parent_path
        except FileExistsError as e:
            print(f'FileExistsError: {repr(e)}')

    @staticmethod
    def get_file_name(absolute_path_file, **kwargs):
        _file_name = os.path.basename(absolute_path_file)
        return str(_file_name).strip()

    def get_download_params(self, storage_file_name, **kwargs):
        """
        Prepare parameter for download
        :param storage_file_name: storage file including absolute path
        :param kwargs:
        :return:
        """
        kwargs['absolute_path_file'] = self.get_absolute_path_file(storage_file_name, **kwargs)
        assert kwargs.get('absolute_path_file'), f"storage_file_name {storage_file_name} invalid"
        # kwargs['chunk_size'] = chunk_size if isinstance(chunk_size,
        #                                                 int) and chunk_size < 4096 * 1024 else self.chunk_size
        # kwargs['retry'] = retry if isinstance(retry, int) and retry < 10 else self.retry

        kwargs['temp_size'] = self.check_temp_file_exists(**kwargs)
        kwargs['file_name'] = self.get_file_name(**kwargs)
        return kwargs

    @staticmethod
    def get_download_url(download_url, **kwargs):
        """
        Get and check download url
        :param download_url:
        :param kwargs:
        :return:
        """
        _download_url = str(download_url).strip() if download_url and str(download_url).strip() else ''
        if _download_url.startswith('http') is False:
            _download_url = f"http://{_download_url}"
        assert is_url(_download_url) is True, \
            f"Download {_download_url} invalid, storage_file_name {kwargs.get('storage_file_name')}"
        kwargs['download_url'] = _download_url
        return kwargs

    def get_total_size(self, **kwargs):
        """
        Get file size
        :param kwargs:
        :return:
        """
        try:
            kwargs['total_size'] = self.get_download_file_size(**kwargs)
            return kwargs
        except:
            raise

    def pre_download(self, **kwargs):
        """
        prefix handle download
        :param kwargs:
        :return:
        """
        try:
            _download_params_kwargs = self.get_download_params(**kwargs)
            download_url_kwargs = self.get_download_url(**_download_params_kwargs)
            return self.get_total_size(**download_url_kwargs)
        except Exception as e:
            error_msg = f"Download failed: download_url {kwargs.get('download_url')}, " \
                        f"storage_file_name {kwargs.get('storage_file_name')}, PreDownloadError: {repr(e)}"
            raise PreDownloadError(error_msg)

    def download_small_file(self, download_url, absolute_path_file, file_name, retry, timeout, **kwargs):
        """
        Download small file
        :param download_url:  url
        :param absolute_path_file: the file including absolute path
        :param file_name: file name
        :param retry: retry time
        :param timeout: connect and wait timeout
        :param kwargs:
        :return: download file name or raise Exception
        """
        headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, '
                          'like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
        }

        error_msg = None
        for i in range(retry):
            try:
                with closing(
                        download_file_requester.get_and_confirm_status(url=download_url, stream=True, headers=headers,
                                                                       timeout=timeout)) as res:
                    with open(self.get_temp_file_name(absolute_path_file), mode='wb') as temp_file:
                        temp_file.write(res.content)

                if self.get_file_size(temp_file.name) != kwargs.get('total_size'):
                    raise InvalidFileError

                return temp_file.name
            except InvalidFileError:
                error_msg = f"Download failed:{file_name}, storage path is {temp_file.name}," \
                            f" InvalidFileError: incomplete or corrupt downloaded file."
                os.remove(temp_file.name)
                continue
            except RequestException as e:
                error_msg = f"Download failed::{file_name}, storage path is {absolute_path_file}," \
                            f"RequestException error: {repr(e)}"
                continue
            except Exception as e:
                error_msg = f"Download failed:{file_name}, storage path is {absolute_path_file}, " \
                            f"undefined error: {traceback.format_exc()}"
                raise DownloadError(error_msg)
        else:
            if error_msg is not None:
                raise DownloadError(error_msg)
            else:
                raise DownloadError(
                    f"Download failed:{file_name}, storage path is {absolute_path_file}, unknown error")

    def download_large_file(self, download_url, absolute_path_file, file_name, chunk_size, retry, timeout, temp_size,
                            **kwargs):
        """
        Download large file from download url by breakpoint continuation
        :param download_url:  url
        :param absolute_path_file: the file including absolute path
        :param file_name: file name
        :param chunk_size: file chunk size
        :param retry: retry time
        :param timeout: connect and wait timeout
        :param temp_size: temp file size
        :param kwargs:
        :return: download file name or raise Exception
        """
        headers = {
            'Range': 'bytes=%d-' % temp_size,
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, '
                          'like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
        }

        error_msg = None
        for i in range(retry):
            try:
                with closing(
                        download_file_requester.get_url(url=download_url, stream=True, headers=headers,
                                                        timeout=timeout)) as res:
                    with open(self.get_temp_file_name(absolute_path_file), mode='ab+') as temp_file:
                        for chunk in res.iter_content(chunk_size=chunk_size):
                            if chunk:
                                temp_file.write(chunk)
                                # temp_file.flush()

                if self.get_file_size(temp_file.name) != kwargs.get('total_size'):
                    raise InvalidFileError

                return temp_file.name
            except InvalidFileError:
                error_msg = f"Download failed:{file_name}, storage path is {temp_file.name}," \
                            f" InvalidFileError: incomplete or corrupt downloaded file."
                os.remove(temp_file.name)

                headers = {
                    'Connection': 'keep-alive',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, '
                                  'like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
                }
                continue
            except RequestException as e:
                error_msg = f"Download failed:{file_name}, storage path is {absolute_path_file}," \
                            f"RequestException error: {repr(e)}"
                continue
            except Exception:
                error_msg = f"Download failed:{file_name}, storage path is {absolute_path_file}, " \
                            f"undefined error:{traceback.format_exc()}"
                raise DownloadError(error_msg)
        else:
            if error_msg is not None:
                raise DownloadError(error_msg)
            else:
                raise DownloadError(
                    f"Download failed:{file_name}, storage path is {absolute_path_file}, unknown error")

    def handle_download(self, **kwargs):
        """
        Handle download task
        :param kwargs:
        :return:
        """
        try:
            if kwargs.get('total_size', 0) < kwargs.get('chunk_size', self.chunk_size):
                kwargs['temp_file'] = self.download_small_file(**kwargs)
            else:
                kwargs['temp_file'] = self.download_large_file(**kwargs)
                # kwargs['temp_file'] = self.download_small_file(**kwargs)
            return kwargs
        except DownloadError as e:
            raise
        except Exception as e:
            error_msg = f"Download failed: download_url {kwargs.get('download_url')}, " \
                        f"absolute_path_file {kwargs.get('absolute_path_file')}, DownloadError {repr(e)}"
            raise DownloadError(error_msg)

    @staticmethod
    def filename_fix_existing(absolute_path_file, **kwargs):
        """Expands name portion of absolute_path_file with numeric ' (x)' suffix to
        return absolute_path_file that doesn't exist already.
        """
        dirname = u'.'
        name_ext = absolute_path_file.rsplit('.', 1)
        name, ext = name_ext[0], '' if len(name_ext) == 1 else name_ext
        names = [x for x in os.listdir(dirname) if x.startswith(name)]
        names = [x.rsplit('.', 1)[0] for x in names]
        suffixes = [x.replace(name, '') for x in names]
        # filter suffixes that match ' (x)' pattern
        suffixes = [x[2:-1] for x in suffixes if x.startswith(' (') and x.endswith(')')]
        indexes = [int(x) for x in suffixes if set(x) <= set('0123456789')]
        idx = 1
        if indexes:
            idx += sorted(indexes)[-1]
        if ext:
            return '%s (%d).%s' % (name, idx, ext)
        else:
            return '%s (%d)' % (name, idx)

    def rename_temp_file(self, absolute_path_file, temp_file, **kwargs):
        """
        Rename temp file to storage file name
        :param absolute_path_file:
        :param temp_file:
        :param kwargs:
        :return:
        """
        if os.path.exists(absolute_path_file):
            absolute_path_file = self.filename_fix_existing(absolute_path_file, **kwargs)
        shutil.move(temp_file, absolute_path_file)
        return absolute_path_file

    def post_download(self, **kwargs):
        """
        Execute after download successful,
        :param kwargs:
        :return:
        """
        try:
            file_name = self.rename_temp_file(**kwargs)
            return file_name
        except Exception as e:
            error_msg = f"Download failed: download_url {kwargs.get('download_url')}, " \
                        f"absolute_path_file {kwargs.get('absolute_path_file')}, PostDownloadError: {repr(e)}"
            raise PostDownloadError(error_msg)

    def get_params_dict(self, download_url, storage_file_name, chunk_size, retry, timeout, **kwargs):
        """
        Prepare parameter for download
        :param download_url:
        :param storage_file_name:
        :param chunk_size:
        :param retry:
        :param timeout:
        :param kwargs:
        :return:
        """
        params = kwargs
        params['download_url'] = str(kwargs.get('url')).strip() if download_url is None and kwargs.get(
            'url') else download_url
        params['storage_file_name'] = str(kwargs.get('filename')).strip() if storage_file_name is None and kwargs.get(
            'filename') else storage_file_name
        params['chunk_size'] = chunk_size if isinstance(chunk_size,
                                                        int) and chunk_size < 4096 * 1024 else self.chunk_size
        params['retry'] = retry if isinstance(retry, int) and retry < 10 else self.retry
        params['timeout'] = timeout if timeout and isinstance(timeout, (int, tuple)) else self.timeout
        return params

    def download(self, download_url=None, storage_file_name=None, chunk_size=4096 * 1024, retry=3, timeout=(10, 60),
                 **kwargs):
        """
        The main method of Download task
        :param download_url:
        :param storage_file_name:
        :param chunk_size:
        :param retry:
        :param timeout:
        :param kwargs:
        :return:
        """
        try:
            params = self.get_params_dict(download_url, storage_file_name, chunk_size, retry, timeout, **kwargs)
            _params = self.pre_download(**params)
            _result = self.handle_download(**_params)
            _file_name = self.post_download(**_result)
            return _file_name
        except (PreDownloadError, DownloadError, PostDownloadError) as e:
            raise
        except Exception as e:
            raise DownloadError(
                f"Download failed: download_url {download_url}, storage_file_name {storage_file_name}, "
                f"error {repr(e)}")


if __name__ == "__main__":
    # if len(sys.argv) == 1:
    #     print("请选择正确的参数：'normal' or 'force'")
    #     sys.exit(0)
    #     # exec_action = ['users', 'mysql', 'database', 'table', 'queryuser', 'querymysql']
    # else:
    #     if len(sys.argv) == 2 and sys.argv[1] == 'normal':
    #         force_download = False
    #     elif len(sys.argv) == 2 and sys.argv[1] == 'force':
    #         force_download = True
    #     else:
    #         print("请选择正确的参数：'normal' or 'force'")
    #         sys.exit(0)
    #
    # url = 'http://dl.videocc.net/5c0ad4c56c/source_5c0ad4c56c85e31c9e3728e6550dbfd4_5?downloadId=5c0ad4c56c' \
    #       '&times=1613142804599&ran=4a43ba4740b1d1ccb962dc3ce6634fc0&sign=7d36967964856d72aa286f4c3008c447'
    # file_path = '/data/download'
    # filename = 'test1'
    # # force_download = False
    #
    # download_video(url, file_path, filename)

    import os

    print(os.path)
    input_params = {
        'download_url': 'http://dl.videocc.net/5c0ad4c56c/source_5c0ad4c56c5554d675e13b1244c483d2_5?downloadId=5c0ad4c56c&times=1615300644701&ran=647dfc4985c4cc41928adc98380b98d8&sign=f3762ce9324c67ce1964325193c7d066',
        'storage_file_name': '5c0ad4c56c5554d675e13b1244c483d2_5',
        'chunk_size': 4096 * 1024,
        'retry': 3,
        'timeout': (10, 120)
    }

    dl = Download()
    print("***" * 30)
    file = dl.download(**input_params)
    print(f"file: {file}")
    print("***" * 30)

    # headers = {
    #     'Connection': 'keep-alive',
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, '
    #                   'like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
    # }

    # r = requests.get(kwargs.get('download_url'), stream=True, headers=headers)
    # file_size = int(r.headers['content-length'])
    # print(f"file_size: {file_size}")
    # f = open("test", "wb")
    # for chunk in r.iter_content(chunk_size=1024):
    #     if chunk:
    #         f.write(chunk)

    # with closing(requests.get(kwargs.get('download_url'), stream=True, headers=headers)) as r:
    #     file_size = int(r.headers['content-length'])
    #     print(f"file_size: {file_size}")
    #     f = open("test", "wb")
    #     for chunk in r.iter_content(chunk_size=1024):
    #         if chunk:
    #             f.write(chunk)
