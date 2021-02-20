# -*- coding: utf-8 -*-
__author__ = 'shijin'
"""
re tools
"""

from io import StringIO
from datetime import datetime, date
import os
import re


def datetime_verify(value):
    """判断是否是一个有效的日期字符串"""
    try:
        if isinstance(value, (date, datetime)):
            return True

        if ":" in value:
            if "-" in value:
                if value.count(":") == 1:
                    datetime.strptime(value, "%Y-%m-%d %H:%M")
                else:
                    datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            elif "/" in value:
                if value.count(":") == 1:
                    datetime.strptime(value, "%Y/%m/%d %H:%M")
                else:
                    datetime.strptime(value, "%Y/%m/%d %H:%M:%S")
            else:
                return False

        elif "-" in value:
            if value.count("-") == 1:
                datetime.strptime(value, "%Y-%m")
            else:
                datetime.strptime(value, "%Y-%m-%d")
        elif "/" in value:
            if value.count("/") == 1:
                datetime.strptime(value, "%Y/%m")
            else:
                datetime.strptime(value, "%Y/%m/%d")
        elif str(value).isdigit() and len(str(value)) == 4:
            datetime.strptime(value, "%Y")
        else:
            return False

        return True
    except:
        return False


def re_mail(mail):
    if isinstance(mail, (str,)):
        _re_mail = re.compile('[\w]+[\-\w.]*@[0-9a-zA-Z]{1,13}\.[com,cn,net]{1,3}$', flags=re.I)  # 邮件正则
        return _re_mail.match(mail)
    elif isinstance(mail, (bytes,)):
        mail = str(mail, encoding='utf-8')
        _re_mail = re.compile('[\w]+[\-\w.]*@[0-9a-zA-Z]{1,13}\.[com,cn,net]{1,3}$', flags=re.I)  # 邮件正则
        return _re_mail.match(mail)
    else:
        return


def re_mobile(mobile):
    if isinstance(mobile, (str,)):
        _re_mobile = re.compile('^[1][3,4,5,7,8][0-9]{9}$')  # 手机号正则
        return _re_mobile.match(mobile)
    elif isinstance(mobile, (bytes,)):
        mobile = str(mobile, encoding='utf-8')
        _re_mobile = re.compile('^[1][3,4,5,7,8][0-9]{9}$')  # 手机号正则
        return _re_mobile.match(mobile)
    else:
        return


def re_uid(uidstr):
    """ check string is *uid"""
    if isinstance(uidstr, (str,)):
        _re_uid = re.compile('[\w.]*uid$', flags=re.I)  # UID正则
        return _re_uid.match(uidstr)


def isdigit(number):
    try:
        number = float(number)
        return isinstance(number, (int, float))
    except:
        return False


def re_duid(duid):
    if isinstance(duid, (str,)):
        if len(duid) != 28:
            return None
        _re_duid = re.compile('^hc(pd|vd|cloud|dev){1}[\w]{21,24}$', flags=re.I)  # 设备唯一标识正则
        return _re_duid.match(duid)


def re_ip(ip):
    if isinstance(ip, (str,)):
        _re_ip = re.compile(r'^(([1-9]?\d|1\d\d|2[0-4]\d|25[0-5])\.){3}([1-9]?\d|1\d\d|2[0-4]\d|25[0-5])$')
        return _re_ip.match(ip)


def re_like_ip(ip):
    if isinstance(ip, (str,)):
        _re_ip = re.compile(r'^(([1-9]?\d|1\d\d|2[0-4]\d|25[0-5])\.)+\d*')
        return _re_ip.match(ip)


def is_emptystring(string):
    try:
        if len(string.strip()) == 0:
            return True
        else:
            return False
    except:
        return False


def re_abbreviation(abbr):
    if isinstance(abbr, (str,)):
        _re_abbreviation = re.compile(r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')  # 英文缩写标识正则
        return _re_abbreviation.match(abbr)


def is_domain(domain):
    _re_domain = re.compile(r'(?:[A-Z0-9_](?:[A-Z0-9-_]{0,247}[A-Z0-9])?\.)+(?:[A-Z]{2,6}|[A-Z0-9-]{2,}(?<!-))\Z',
                            re.IGNORECASE)
    return _re_domain.match(domain)


def is_url(url):
    try:
        url_regex = re.compile(r'^http[s]?://', re.IGNORECASE)
        return True if url_regex.match(url) else False
    except:
        # print(str(e))
        return False


def is_ipv4(address):
    try:
        ipv4_regex = re.compile(r'(?:25[0-5]|2[0-4]\d|[0-1]?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}',
                                re.IGNORECASE)
        return True if ipv4_regex.match(address) else False
    except:
        return False
