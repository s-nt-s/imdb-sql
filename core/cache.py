import functools
import os
import time
import logging
import hashlib
import json
from core.req import R
from datetime import datetime
import re

from .filemanager import FM

logger = logging.getLogger(__name__)


def sha256_hash(*args, **kwargs) -> str:
    data = json.dumps([args, kwargs], sort_keys=True, separators=(',', ':'), indent=0)
    return hashlib.sha256(data.encode('utf-8')).hexdigest()


def to_timestamp(s):
    if not isinstance(s, str):
        return None
    return datetime(*map(int, re.findall(r"\d+", s))).timestamp()


class Cache:
    def __init__(self, file: str, *args, kwself=None, reload: bool = False, skip: bool = False, maxOld=1, loglevel=None, **kwargs):
        self.file = file
        self.func = None
        self.reload = reload
        self.maxOld = maxOld
        self.loglevel = loglevel
        self.kwself = kwself
        if maxOld is not None:
            self.maxOld = time.time() - (maxOld * 86400)
        self._kwargs = kwargs
        self.skip = skip

    def parse_file_name(self, *args, slf=None, **kwargs):
        if args or kwargs:
            return self.file.format(*args, **kwargs)
        return self.file

    def read(self, file, *args, **kwargs):
        return FM.load(file, **self._kwargs)

    def save(self, file, data, *args, **kwargs):
        if file is None:
            return
        FM.dump(file, data, **self._kwargs)

    def tooOld(self, fl):
        if fl is None:
            return True
        if not os.path.isfile(fl):
            return True
        if self.reload:
            return True
        if self.maxOld is None:
            return False
        if os.stat(fl).st_mtime < self.maxOld:
            return True
        return False

    def log(self, txt):
        if self.loglevel is not None:
            logger.log(self.loglevel, txt)

    def callCache(self, slf, *args, **kwargs):
        flkwargs = dict(kwargs)
        if isinstance(self.kwself, str):
            flkwargs[self.kwself] = slf
        fl = self.parse_file_name(*args, **flkwargs)
        if not self.tooOld(fl):
            self.log(f"Cache.read({fl})")
            data = self.read(fl, *args, **kwargs)
            if data is not None:
                return data
        data = self.func(slf, *args, **kwargs)
        if data is not None:
            self.log(f"Cache.save({fl})")
            self.save(fl, data, *args, **kwargs)
        return data

    def __call__(self, func):
        if self.skip:
            return func

        def callCache(*args, **kwargs):
            return self.callCache(*args, **kwargs)
        functools.update_wrapper(callCache, func)
        self.func = func
        setattr(callCache, "__cache_obj__", self)
        return callCache


class StaticCache(Cache):
    def callCache(self, *args, **kwargs):
        flkwargs = dict(kwargs)
        fl = self.parse_file_name(*args, **flkwargs)
        if not self.tooOld(fl):
            self.log(f"Cache.read({fl})")
            data = self.read(fl, *args, **kwargs)
            if data is not None:
                return data
        data = self.func(*args, **kwargs)
        if data is not None:
            self.log(f"Cache.save({fl})")
            self.save(fl, data, *args, **kwargs)
        return data

    def parse_file_name(self, *args, **kwargs):
        if args or kwargs:
            return self.file.format(*args, **kwargs)
        return self.file


class StaticHashCache(StaticCache):
    def parse_file_name(self, *args, slf=None, **kwargs):
        hash = sha256_hash(*args, **kwargs)
        return self.file.format(hash)


class HashCache(Cache):
    def parse_file_name(self, *args, slf=None, **kwargs):
        hash = sha256_hash(*args, **kwargs)
        return self.file.format(hash)
