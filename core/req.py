from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from socket import timeout
from functools import cache, cached_property
import logging
import json
import gzip
from io import TextIOWrapper
import csv
from http.client import HTTPResponse
from time import sleep
from json.decoder import JSONDecodeError

logger = logging.getLogger(__name__)


class Req:

    @cache
    def __get_body(self, url: str, headers: frozenset = None, data: bytes = None) -> str:
        req = Request(
            url,
            headers=dict(headers or frozenset()),
            data=data
        ) if headers or data else url
        r: HTTPResponse
        with urlopen(req) as r:
            charset: str = r.headers.get_content_charset() or 'utf-8'
            body: str = r.read().decode(charset, errors="replace")
            body = body.strip()
            if len(body):
                return body

    @cached_property
    def ip(self):
        ip = self.__get_body("https://ifconfig.me/ip")
        return ip

    def get_body(self, url: str, headers: dict = None, chances: int = 1, data: bytes = None, silent=False):
        chances = max(chances, 1)
        frz = frozenset(headers.items()) if headers else None
        for i in range(1, chances + 1):
            try:
                return self.__get_body(url, headers=frz, data=data)
            except (HTTPError, URLError, UnicodeDecodeError, timeout) as e:
                if i == chances:
                    if not silent:
                        logger.critical(f"[KO] {url} {e}")
                    return None
                sleep(10)

    def get_json(
            self,
            url: str,
            headers: dict = None,
            data: bytes = None,
            wait_if_status: dict[int, int] = None
    ) -> list | dict:
        frz = frozenset(headers.items()) if headers else None
        try:
            body = self.__get_body(url, headers=frz, data=data)
            return json.loads(body)
        except HTTPError as e:
            wait = (wait_if_status or {}).get(e.code, 0)
            if wait <= 0:
                raise
        sleep(wait)
        return self.get_json(url, headers, data, wait_if_status=tuple())

    def safe_get_json(self, url, *args, **kwargs):
        try:
            return self.get_json(url, *args, **kwargs)
        except (HTTPError, URLError, UnicodeDecodeError, timeout, JSONDecodeError) as e:
            if not isinstance(e, HTTPError) or e.code != 404:
                logger.warning(f"{url} {str(e)}")
            return None

    def iter_tsv(self, url: str):
        with urlopen(url) as r:
            with gzip.GzipFile(fileobj=r) as gz:
                stream = TextIOWrapper(
                    gz,
                    encoding='utf-8',
                    newline=''
                )
                reader = csv.reader(
                    stream,
                    delimiter='\t',
                    quoting=csv.QUOTE_NONE
                )
                yield from reader


R = Req()
