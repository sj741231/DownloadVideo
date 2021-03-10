# -*- coding:utf-8 -*-
"""
Class for request url
"""
import requests
from requests.exceptions import RequestException
from requests.adapters import HTTPAdapter
# import six.moves.urllib.parse as urlparse
from urllib.parse import urlparse, urlsplit, urlunsplit  # only support python3
from requests_toolbelt import MultipartEncoder
from html.parser import HTMLParser
from bs4 import BeautifulSoup
import uuid
from settings import BASE_URL, DL_BASE_URL


class UnDefineException(Exception):
    pass


class PostRequired(Exception):
    pass


class GetRequired(Exception):
    pass


"""
Module for  requester (which is a wrapper around python-requests)
"""

# import logging

# these two lines enable debugging at httplib level
# (requests->urllib3->httplib)
# you will see the REQUEST, including HEADERS and DATA, and RESPONSE
# with HEADERS but without DATA.
# the only thing missing will be the response.body which is not logged.
# import httplib
# httplib.HTTPConnection.debuglevel = 1

# you need to initialize logging, otherwise you will not see anything
# from requests
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

requests.adapters.DEFAULT_RETRIES = 3


class Requester(object):
    """
    A class which carries out HTTP requests. You can replace this
    class with one of your own implementation if you require some other
    way to access url.

    This default class can handle simple authentication only.
    """

    VALID_STATUS_CODES = [200]  # define 200 success
    AUTH_COOKIE = None

    def __init__(self, *args, **kwargs):
        username = None
        password = None
        ssl_verify = True
        cert = None
        base_url = None
        timeout = (10, 60)

        if len(args) == 1:
            username, = args
        elif len(args) == 2:
            username, password = args
        elif len(args) == 3:
            username, password, ssl_verify = args
        elif len(args) == 4:
            username, password, ssl_verify, cert = args
        elif len(args) == 5:
            username, password, ssl_verify, cert, base_url = args
        elif len(args) == 6:
            username, password, ssl_verify, cert, base_url, timeout = args
        elif len(args) > 6:
            raise ValueError("To much positional arguments given!")

        self.key = kwargs.get('appkey') if kwargs.get('appkey') is not None else None
        self.base_url = kwargs.get('url') if kwargs.get('url') is not None else None
        self.timeout = kwargs.get('timeout') if kwargs.get('timeout') and isinstance(kwargs.get('timeout'),
                                                                                     (tuple, int)) else timeout

        base_url = kwargs.get('base_url', base_url)
        # self.base_scheme = urlparse.urlsplit(
        self.base_scheme = urlsplit(base_url).scheme if base_url else None

        self.username = kwargs.get('username', username)
        self.password = kwargs.get('password', password)
        if self.username:
            assert self.password, "Please provide both username and password or do not provide them at all"
        if self.password:
            assert self.username, "Please provide both username and password or do not provide them at all"

        self.ssl_verify = kwargs.get('ssl_verify', ssl_verify)
        self.cert = kwargs.get('cert', cert)

        self.session = requests.Session()

    def get_request_dict(self, params=None, data=None, files=None, headers=None, **kwargs):
        request_kwargs = kwargs
        if self.username:
            request_kwargs['auth'] = (self.username, self.password)

        if params:
            assert isinstance(params, dict), 'params must be a dict, got %s' % repr(params)
            request_kwargs['params'] = params

        if headers:
            assert isinstance(headers, dict), 'headers must be a dict, got %s' % repr(headers)
            request_kwargs['headers'] = headers

        if self.AUTH_COOKIE:
            currentheaders = request_kwargs.get('headers', {})
            currentheaders.update({'Cookie': self.AUTH_COOKIE})
            request_kwargs['headers'] = currentheaders

        request_kwargs['verify'] = self.ssl_verify
        request_kwargs['cert'] = self.cert

        if data:
            request_kwargs['data'] = data

        if files:
            request_kwargs['files'] = files

        request_kwargs['timeout'] = request_kwargs.get('timeout') if request_kwargs.get('timeout') and isinstance(
            request_kwargs.get('timeout'), (int, tuple)) else self.timeout

        return request_kwargs

    def _update_url_scheme(self, url):
        """
        Updates scheme of given url to the one used in url base_url.
        """
        if self.base_scheme and not url.startswith("%s://" % self.base_scheme):
            # url_split = urlparse.urlsplit(url)
            url_split = urlsplit(url)
            # url = urlparse.urlunsplit(
            url = urlunsplit(
                [
                    self.base_scheme,
                    url_split.netloc,
                    url_split.path,
                    url_split.query,
                    url_split.fragment
                ]
            )
        return url

    def get_url(self, url, params=None, headers=None, allow_redirects=True, stream=False, timeout=None):
        request_kwargs = self.get_request_dict(
            params=params,
            headers=headers,
            allow_redirects=allow_redirects,
            stream=stream,
            timeout=timeout
        )
        # params = None, data = None, headers = None, cookies = None, files = None,
        # auth = None, timeout = None, allow_redirects = True, proxies = None,
        # hooks = None, stream = None, verify = None, cert = None, json = None
        return self.session.get(self._update_url_scheme(url), **request_kwargs)

    def post_url(self, url, params=None, data=None, files=None, headers=None, allow_redirects=True, timeout=None,
                 **kwargs):
        request_kwargs = self.get_request_dict(
            params=params,
            data=data,
            files=files,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
            **kwargs
        )
        # params = None, data = None, headers = None, cookies = None, files = None,
        # auth = None, timeout = None, allow_redirects = True, proxies = None,
        # hooks = None, stream = None, verify = None, cert = None, json = None
        return self.session.post(self._update_url_scheme(url), **request_kwargs)

    def post_multipart_and_confirm_status(self, url, params=None, data=None, files=None, headers=None, valid=None,
                                          allow_redirects=True, timeout=None):
        valid = valid or self.VALID_STATUS_CODES
        if not headers and not files:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:50.0) Gecko/20100101 Firefox/50.0',
                'Referer': url
            }

        assert data is not None, "Post messages must have data"
        assert isinstance(data, dict), 'data must be a dict, got {d}'.format(d=repr(data))

        boundary = '-' * 5 + uuid.uuid1().hex[15:24]
        data = MultipartEncoder(fields=data, boundary=boundary)
        headers['Content-Type'] = data.content_type

        response = self.post_url(
            url,
            params,
            data,
            files,
            headers,
            allow_redirects,
            timeout=timeout)
        if response.status_code not in valid:
            raise UnDefineException(
                'Operation failed. url={u}, data={d}, headers={h}, status={s}, text={t}'.format(
                    u=response.url,
                    d=data,
                    h=headers,
                    s=response.status_code,
                    t=response.text.encode('UTF-8'))
            )
        return response

    def post_xml_and_confirm_status(self, url, params=None, data=None, valid=None, timeout=None):
        headers = {'Content-Type': 'text/xml'}
        return self.post_and_confirm_status(url, params=params, data=data, headers=headers, valid=valid,
                                            timeout=timeout)

    def post_and_confirm_status(self, url, params=None, data=None, files=None, headers=None, valid=None,
                                allow_redirects=True, timeout=None):
        valid = valid or self.VALID_STATUS_CODES
        if not headers and not files:
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        assert data is not None, "Post messages must have data"

        response = self.post_url(
            url,
            params,
            data,
            files,
            headers,
            allow_redirects,
            timeout=timeout)
        if response.status_code not in valid:
            raise UnDefineException(
                'Operation failed. url={u}, data={d}, headers={h}, status={s}, text={t}'.format(
                    u=response.url,
                    d=data,
                    h=headers,
                    s=response.status_code,
                    t=response.text.encode('UTF-8'))
            )
        return response

    def get_and_confirm_status(self, url, params=None, headers=None, valid=None, stream=False, timeout=None):
        valid = valid or self.VALID_STATUS_CODES
        response = self.get_url(url, params, headers, stream=stream, timeout=timeout)
        if response.status_code not in valid:
            if response.status_code == 405:  # POST required
                raise PostRequired('POST required for url {u}'.format(u=str(url)))

            raise UnDefineException(
                'Operation failed. url={u}, headers={h}, status={s}, text={t}'.format(
                    u=response.url,
                    h=headers,
                    s=response.status_code,
                    t=response.text.encode('UTF-8'))
            )
        return response


requester = Requester(username=None, password=None, base_url=BASE_URL, ssl_verify=True, cert=None, timeout=(10, 60),
                      **dict())

download_file_requester = Requester(username=None, password=None, base_url=DL_BASE_URL, ssl_verify=True, cert=None,
                                    timeout=(10, 60), **dict())

if __name__ == "__main__":
    # _username = None
    # _password = None
    # _timeout = (15, 300)
    # _base_url = "http://demo.polyv.net/dlsource/5c0ad4c56c.php"
    # _ssl_verify = True
    # _cert = None
    # kwargs = dict()

    # requester = Requester(username=None, password=None, base_url=None, ssl_verify=True, cert=None, timeout=(15, 60),
    #                       **dict())

    data = {"vid_text": "5c0ad4c56c85e31c9e3728e6550dbfd4_5"}
    response = requester.post_multipart_and_confirm_status(url="http://demo.polyv.net/dlsource/5c0ad4c56c.php",
                                                           data=data)

    print(response.status_code)
    print("###" * 10)
    parser = HTMLParser()

    soup2 = BeautifulSoup(response.text, "html.parser")
    # print(soup2.prettify())
    # print(dir(soup2))
    print(soup2.text.strip())
