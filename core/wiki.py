from textwrap import dedent
import logging
from typing import Any, NamedTuple, Optional
from functools import cache
import re
from time import sleep
from functools import wraps
from core.git import G
from core.req import R
from collections import defaultdict
from datetime import datetime, timedelta
from core.util import iter_chunk
from urllib.error import HTTPError
from core.country import CF
import json
from requests import Session
from types import MappingProxyType


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")
LANGS = ('es', 'en', 'ca', 'gl', 'it', 'fr')

re_imdb = re.compile(r"^tt\d+$")
re_fiml = re.compile(r"^\d+$")
re_wiki_url = re.compile(r"^https://\w\.wikipedia\.org/wiki/\S+$")


def _parse_wiki_val(s):
    if not isinstance(s, str):
        return s
    s = s.strip()
    if len(s) == 0:
        return None
    if s.startswith("http://www.wikidata.org/.well-known/genid/"):
        return None
    m = re.match(r"https?://www\.wikidata\.org/entity/(Q\d+)", s)
    if m:
        return f"wd:{m.group(1)}"
    return s


class WikiError(Exception):
    def __init__(self, msg: str, query: str, http_code: int):
        super().__init__(f"{msg}\n{query}")
        self.__query = query
        self.__msg = msg
        self.__http_code = http_code

    @property
    def msg(self):
        return self.__msg

    @property
    def http_code(self):
        return self.__http_code

    @property
    def query(self):
        return self.__query


class WikiImdbCountry(NamedTuple):
    imdb: str
    main: tuple[str, ...]
    producer: dict[str, int]
    director: dict[str, int]
    casting: dict[str, int]
    writer: dict[str, int]
    country_lang: tuple[str, ...]


def retry_fetch(chunk_size=5000):
    def decorator(func):
        internal_cache: dict[tuple[str, str], Any] = {}

        @wraps(func)
        def wrapper(self: "WikiApi", *args, **kwargs):
            undone = set(args).difference({None, ''})
            if len(undone) == 0:
                return {}
            kwargs_to_json = {}
            for k, v in kwargs.items():
                if isinstance(v, re.Pattern):
                    v = (v.pattern, v.flags)
                kwargs_to_json[k] = v
            key_cache = json.dumps((func.__name__, kwargs_to_json), sort_keys=True)
            result = dict()
            for a in undone:
                val = internal_cache.get((key_cache, a))
                if val is not None:
                    result[a] = val
                    undone.discard(a)

            def _log_line(rgs: tuple, kw: dict, ck: int):
                rgs = sorted(set(rgs))
                line = ", ".join(
                    [f"{len(rgs)} ids [{rgs[0]} - {rgs[-1]}]"] +
                    [f"{k}={v}" for k, v in kw.items()] +
                    [f"chunk_size={ck}"]
                )
                return f"{func.__name__}({line})"

            error_query = {}
            count = 0
            tries = 0
            until = datetime.now() + timedelta(seconds=60*5)
            cur_chunk_size = int(chunk_size)
            while undone and (tries == 0 or (datetime.now() < until and tries < 3)):
                error_query = {}
                tries = tries + 1
                if tries > 1:
                    cur_chunk_size = max(1, min(cur_chunk_size, len(undone)) // 3)
                    sleep(5)
                logger.info(_log_line(undone, kwargs, cur_chunk_size))
                for chunk in iter_chunk(cur_chunk_size, list(undone)):
                    count += 1
                    fetched: dict = None
                    try:
                        fetched = func(self, *chunk, **kwargs) or {}
                        fetched = {k: v for k, v in fetched.items() if v}
                    except WikiError as e:
                        logger.warning(f"└ [KO] {e.msg}")
                        if e.http_code == 429:
                            sleep(60)
                        elif e.http_code is not None:
                            last_error = error_query.get(e.http_code)
                            if last_error is None or len(last_error) > len(e.query):
                                error_query[e.http_code] = str(e.query)
                    if not fetched:
                        continue
                    for k, v in fetched.items():
                        result[k] = v
                        internal_cache[(key_cache, k)] = v
                        undone.remove(k)
                    logger.debug(f"└ [{count}] [{chunk[0]} - {chunk[-1]}] = {len(fetched)} items")

            logger.info(f"{_log_line(args, kwargs, chunk_size)} = {len(result)} items")
            for c, q in error_query.items():
                logger.warning(f"STATUS_CODE {c} for:\n{q}")
            return result

        return wrapper
    return decorator


class WikiApi:
    def __init__(self):
        # https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
        self.__headers = {
            'User-Agent': f'ImdbBoot/0.0 ({G.remote}; {G.mail})',
            "Accept": "application/sparql-results+json",
            'Content-Type': 'application/sparql-query'
        }
        self.__last_query: str | None = None
        self.__tsv = Session()
        self.__tsv.headers = {
            'User-Agent': f'ImdbBoot/0.0 ({G.remote}; {G.mail})',
            "Accept": "text/tab-separated-values",
        }

    @property
    def last_query(self):
        return self.__last_query

    def query_sparql(self, query: str) -> dict:
        # https://query.wikidata.org/
        query = dedent(query).strip()
        query = re.sub(r"\n(\s*\n)+", "\n", query)
        self.__last_query = query
        try:
            return R.get_json(
                "https://query.wikidata.org/sparql",
                headers=self.__headers,
                data=self.__last_query.encode('utf-8'),
                wait_if_status={429: 60}
            )
        except Exception as e:
            code = e.code if isinstance(e, HTTPError) else None
            raise WikiError(str(e), self.__last_query, http_code=code) from e

    def query(self, query: str) -> list[dict[str, Any]]:
        data = self.query_sparql(query)
        if not isinstance(data, dict):
            raise WikiError(str(data), self.__last_query)
        result = data.get('results')
        if not isinstance(result, dict):
            raise WikiError(str(data), self.__last_query)
        bindings = result.get('bindings')
        if not isinstance(bindings, list):
            raise WikiError(str(data), self.__last_query)
        for i in bindings:
            if not isinstance(i, dict):
                raise WikiError(str(data), self.__last_query)
            if i.get('subject') and i.get('object'):
                raise WikiError(str(data), self.__last_query)
        return bindings

    def get_filmaffinity(self, *args):
        obj = self.get_dict_1_to_1(
            *args,
            key_field='wdt:P345',
            val_field='wdt:P480',
            re_k=re_imdb,
            re_v=re_fiml
        )
        return MappingProxyType({k: int(v) for k, v in obj.items()})

    def get_imdb(self, *args):
        obj = self.get_dict_1_to_1(
            *args,
            key_field='wdt:P480',
            val_field='wdt:P345',
            re_k=re_fiml,
            re_v=re_imdb,
        )
        return MappingProxyType({int(k): v for k, v in obj.items()})

    def get_director(self, *args):
        return self.get_dict_uniq_tuple(
            *args,
            key_field='wdt:P345',
            val_field='wdt:P345',
            by_field='wdt:P57'
        )

    def get_names(self, *args: str) -> dict[str, str]:
        obj = {}
        for k, v in self.get_label_dict(*args, key_field='wdt:P345').items():
            if len(v) == 1:
                obj[k] = v[0]
        return obj

    @retry_fetch(chunk_size=300)
    def get_label_dict(self, *args, key_field: str = None, lang: tuple[str] = None):
        if not lang:
            lang = LANGS

        values = " ".join(f'"{x}"' for x in args)

        lang_priority = {lg: i for i, lg in enumerate(lang, start=1)}
        lang_filter = ", ".join(f'"{lg}"' for lg in lang_priority)

        lang_case = " ".join(
            f'IF(LANG(?v) = "{lg}", {p},' for lg, p in lang_priority.items()
        ) + f"{(len(lang_priority) + 1)})" + (')'* (len(lang_priority)-1))

        query = dedent("""
            SELECT ?k ?v WHERE {
                VALUES ?k { %s }
                ?item %s ?k ;
                    rdfs:label ?v .
                FILTER(LANG(?v) IN (%s))

                {
                SELECT ?k (MIN(?pri) AS ?minPri) WHERE {
                    VALUES ?k { %s }
                    ?item %s ?k ;
                        rdfs:label ?v .
                    FILTER(LANG(?v) IN (%s))
                    BIND(%s AS ?pri)
                }
                GROUP BY ?k
                }

                BIND(%s AS ?pri)
                FILTER(?pri = ?minPri)
            }
        """).strip() % (
            values,
            key_field,
            lang_filter,
            values,
            key_field,
            lang_filter,
            lang_case,
            lang_case,
        )
        return self.__get_dict_uniq_tuple(query)

    def __mk_query(
        self,
        *args,
        key_field: str = None,
        val_field: str = None,
        by_field: str = None
    ):
        ids = " ".join(map(lambda x: x if x.startswith("wd:") else f'"{x}"', map(str, args)))
        if by_field:
            query = dedent('''
                SELECT ?k ?v WHERE {
                    VALUES ?k { %s }
                    ?item %s ?k ;
                          %s ?b .
                       ?b %s ?v .
                }
            ''').strip() % (
                ids,
                key_field,
                by_field,
                val_field,
            )
        elif key_field is None:
            query = dedent('''
                SELECT ?k ?v WHERE {
                    VALUES ?k { %s }
                    ?k %s ?v.
                }
            ''').strip() % (
                ids,
                val_field,
            )
        else:
            query = dedent('''
                SELECT ?k ?v WHERE {
                    VALUES ?k { %s }
                    ?item %s ?k.
                    ?item %s ?v.
                }
            ''').strip() % (
                ids,
                key_field,
                val_field,
            )
        return query

    @retry_fetch(chunk_size=300)
    def get_dict_uniq_tuple(
        self,
        *args,
        key_field: str = None,
        val_field: str = None,
        by_field: str = None,
        re_k: Optional[re.Pattern] = None,
        re_v: Optional[re.Pattern] = None
    ):
        query = self.__mk_query(
            *args,
            key_field=key_field,
            val_field=val_field,
            by_field=by_field
        )
        return self.__get_dict_uniq_tuple(query, re_k=re_k, re_v=re_v)

    @retry_fetch(chunk_size=300)
    def get_dict_1_to_1(
        self,
        *args,
        key_field: str = None,
        val_field: str = None,
        by_field: str = None,
        re_k: Optional[re.Pattern] = None,
        re_v: Optional[re.Pattern] = None
    ):
        query = self.__mk_query(
            *args,
            key_field=key_field,
            val_field=val_field,
            by_field=by_field
        )
        return self.__get_dict_1_to_1(query, re_k=re_k, re_v=re_v)

    def get_alpha3(self, *args: str):
        def _get_dict(val_field: str, *vals: str):
            obj: dict[str, str] = {}
            for k, v in self.get_dict_uniq_tuple(*vals, key_field=None, val_field=val_field).items():
                set_v = set(map(CF.parse_alpha3, v))
                set_v.discard(None)
                if len(set_v) == 1:
                    obj[k] = set_v.pop()
            return obj

        done: dict[str, str] = {}
        undone = set(args)
        for val_field in (
            "wdt:P298",
            "p:P298/ps:P298",
            "wdt:P984",
            "wdt:P11897",
        ):
            undone.difference_update(done.keys())
            done.update(_get_dict(val_field, *undone))
        return done

    @cache
    def get_countries(self, *args: str):
        def _get_dict(val_field: str, by_field: str = None):
            return self.get_dict_uniq_tuple(*args, key_field="wdt:P345", val_field=val_field, by_field=by_field)

        data: dict[str, dict[str, tuple[str, ...]]] = dict(
            main=_get_dict(val_field="wdt:P495"),
            prod=_get_dict(val_field="wdt:P17", by_field="wdt:P272"),
            dire=_get_dict(val_field="wdt:P27", by_field="wdt:P57"),
            writ=_get_dict(val_field="wdt:P27", by_field="wdt:P58"),
            acto=_get_dict(val_field="wdt:P27", by_field="wdt:P161"),
            country_lang=self.__get_countries_from_lang(*args)
        )
        q_vals: set[str] = set()
        for dct in data.values():
            for vls in dct.values():
                q_vals.update(vls)
        alpha = self.get_alpha3(*q_vals)
        for k, dct in list(data.items()):
            data[k] = {
                kk: tuple(x for x in map(alpha.get, vv) if x is not None)
                for kk, vv in dct.items()
            }
        r: dict[str, WikiImdbCountry] = {}
        for imdb in args:
            director = data['dire'].get(imdb, tuple())
            producer = data['prod'].get(imdb, tuple())
            writer = data['writ'].get(imdb, tuple())
            casting = data['acto'].get(imdb, tuple())
            r[imdb] = WikiImdbCountry(
                imdb=imdb,
                main=data['main'].get(imdb, tuple()),
                producer={p: producer.count(p) for p in producer},
                director={d: director.count(d) for d in director},
                writer={w: writer.count(w) for w in writer},
                casting={c: casting.count(c) for c in casting},
                country_lang=data['country_lang'].get(imdb, tuple())
            )
        return r

    def __get_countries_from_lang(self, *imdb: str) -> dict[str, tuple[str, ...]]:
        imdb = tuple(sorted(set(imdb)))
        if len(imdb) == 0:
            return {}
        imdb_lang = self.get_dict_uniq_tuple(*imdb, key_field="wdt:P345", val_field="wdt:P364")
        q_lang: set[str] = set()
        for lg in imdb_lang.values():
            q_lang.update(lg)
        q_lang_countries = self.__get_countries_from_q_lang(*q_lang)
        obj: dict[str, set[str]] = defaultdict(set)
        for i in imdb:
            for countries in map(q_lang_countries.get, imdb_lang.get(i, tuple())):
                if countries:
                    obj[i].update(countries)
        r = {k: tuple(sorted(v)) for k, v in obj.items()}
        return r

    @retry_fetch(chunk_size=300)
    def __get_countries_from_q_lang(self, *q_lang: str):
        query = '''
        SELECT ?k ?v WHERE {
            VALUES ?k { %s }
            # O bien idioma oficial (P37)
            { ?v wdt:P37 ?k . }
            UNION
            # O bien lengua hablada aquí (P2936)
            { ?v wdt:P2936 ?k . }
            ?v wdt:P31/wdt:P279* wd:Q3624078 .
        }
        ''' % " ".join(q_lang)
        return self.__get_dict_uniq_tuple(query)

    @retry_fetch(chunk_size=1000)
    def get_wiki_url(self, *args):
        ids = " ".join(map(lambda x: f'"{x}"', args))
        order = []
        for i, lang in enumerate(LANGS, start=1):
            order.append(f'IF(CONTAINS(STR(?site), "://{lang}.wikipedia.org"), {i},')
        len_order = len(order)
        order.append(f"{len_order}" + (')' * len_order))
        order_str = " ".join(order)

        query = dedent("""
            SELECT ?k ?v WHERE {
            VALUES ?k { %s }

            ?item wdt:P345 ?k .
            ?v schema:about ?item ;
                    schema:isPartOf ?site .

            FILTER(CONTAINS(STR(?site), "wikipedia.org"))

            BIND(
                %s
                AS ?priority
            )
            {
                SELECT ?k (MIN(?priority) AS ?minPriority) WHERE {
                VALUES ?k { %s }
                ?item wdt:P345 ?k .
                ?v schema:about ?item ;
                        schema:isPartOf ?site .
                FILTER(CONTAINS(STR(?site), "wikipedia.org"))
                BIND(
                    %s
                    AS ?priority
                )
                }
                GROUP BY ?k
            }

            FILTER(?priority = ?minPriority)
            }
            ORDER BY ?k
        """ % (ids, order_str, ids, order_str)
        ).strip()
        return self.__get_dict_1_to_1(
            query,
            re_k=re_imdb,
            re_v=re_wiki_url
        )

    def get_imdb_filmaffinity(self):
        query = dedent("""
        SELECT ?k ?v WHERE {
            ?item wdt:P345 ?k .
            ?item wdt:P480 ?v .
            FILTER(REGEX(?v, "^[0-9]+$"))
        }
        GROUP BY ?item ?k ?v
        HAVING (COUNT(?v) = 1)
        """).strip()
        obj = self.__get_dict_1_to_1(
            query,
            re_k=re_imdb,
            re_v=re_fiml
        )
        return MappingProxyType({k: int(v) for k, v in obj.items()})

    def get_imdb_wiki_es(self):
        query = dedent("""
            SELECT ?k ?v WHERE {
                ?item wdt:P345 ?k .
                ?v schema:about ?item ;
                    schema:isPartOf <https://es.wikipedia.org/> .
                FILTER(REGEX(?v, "^https://es.wikipedia.org/wiki/.*$"))
            }
            GROUP BY ?item ?k ?v
            HAVING (COUNT(?v) = 1)
        """).strip()
        return self.__get_dict_1_to_1(
            query,
            re_k=re_imdb,
            re_v=re.compile(r"^https://es\.wikipedia\.org/wiki/\S+$")
        )

    def __iter_k_v(
        self,
        query: str,
        re_k: Optional[re.Pattern] = None,
        re_v: Optional[re.Pattern] = None
    ):
        for b in self.query(query):
            vk = b['k']
            vv = b['v']
            if None in (vk, vv):
                continue
            if not isinstance(vk, dict):
                raise ValueError(f"Invalid key: {vk}")
            if not isinstance(vv, dict):
                raise ValueError(f"Invalid value: {vv}")
            k = _parse_wiki_val(vk.get('value'))
            v = _parse_wiki_val(vv.get('value'))
            if None in (k, v):
                continue
            if not isinstance(k, str):
                raise ValueError(f"Invalid key: {k}")
            if not isinstance(v, str):
                raise ValueError(f"Invalid value: {v}")
            k = k.strip()
            v = v.strip()
            if 0 in map(len, (k, v)):
                continue
            if re_k and not re_k.match(k):
                continue
            if re_v and not re_v.match(v):
                continue
            yield k, v

    def __get_dict_uniq_tuple(
        self,
        query: str,
        re_k: Optional[re.Pattern] = None,
        re_v: Optional[re.Pattern] = None
    ):
        r: dict[str, list[str]] = defaultdict(list)
        for k, v in self.__iter_k_v(query, re_k=re_k, re_v=re_v):
            if v not in r[k]:
                r[k].append(v)
        obj = {k: tuple(v) for k, v in r.items()}
        return MappingProxyType(obj)

    def __get_dict_1_to_1(
        self,
        query: str,
        re_k: Optional[re.Pattern] = None,
        re_v: Optional[re.Pattern] = None
    ):
        obj = self.__get_dict_uniq_tuple(query, re_k=re_k, re_v=re_v)
        rev: dict[Any, set[str]] = defaultdict(set)
        for k, v in obj.items():
            for x in v:
                rev[x].add(k)
        r: dict[str, str] = dict()
        for k, v in obj.items():
            if len(v) != 1:
                continue
            val = v[0]
            if len(rev[val]) != 1:
                continue
            r[k] = val
        return MappingProxyType(r)


WIKI = WikiApi()

if __name__ == "__main__":
    import sys
    from core.config_log import config_log
    config_log("log/wiki.log")

    #data = WIKI.get_imdb_wiki_es()
    #for k, v in data.items():
    #    print(k, v)
    #sys.exit()
    if len(sys.argv) == 1:
        from core.dblite import DBlite
        db = DBlite("imdb.sqlite", quick_release=True)
        ids = db.to_tuple("select id from movie limit 100")
        ok = WIKI.get_countries(*ids)
        print(len(ok))
        sys.exit()

    result = WIKI.get_countries(*sys.argv[1:])
    for k, v in result.items():
        print(k, v)
