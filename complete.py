from core.dblite import DBlite, gW
from core.filemanager import FM
from core.wiki import WIKI
from core.config_log import config_log
from core.git import G
from core.req import R
import logging
from sqlite3 import OperationalError
from core.imdb import IMDB
from os import environ
from core.country import CF
from core.filmaffinity import FilmAffinityApi
import re
from typing import Union

config_log("log/complete_db.log")

logger = logging.getLogger(__name__)
DB = DBlite("imdb.sqlite", reload=False, quick_release=True)


def load_url(url: str):
    try:
        r = R.get_json(url)
        if isinstance(r, dict) and r:
            logger.info(f"{url} = {len(r)}")
            return r
    except Exception:
        pass
    return {}


def load_sql(sql: str):
    try:
        r = DB.get_dict(sql)
        if r:
            logger.info(f"{sql} = {len(r)}")
            return r
    except OperationalError:
        pass
    return {}


def load_files(*paths: str):
    obj = {}
    for path in paths:
        scheme = path.split("://")[0].lower()
        if scheme in ("http", "https"):
            r = load_url(path)
            obj.update(r)
            continue
        if path.split()[0].lower() == "select":
            r = load_sql(path)
            obj.update(r)
            continue
        file = FM.resolve_path(path)
        if file.is_file():
            r = FM.load(file)
            if isinstance(r, dict) and r:
                logger.info(f"{path} = {len(r)}")
                obj.update(r)
    return obj


def load_dict(name: str):
    obj = load_files(
        f"{G.page}/{name}.json",
        f"out/{name}.json",
        f"rec/{name}.json",
        f"rec/{name}.dct.txt",
        f"select movie, {name} from EXTRA where {name} is not null"
    )
    return obj


def dump_dict(name: str):
    obj = DB.get_dict(f"select movie, {name} from EXTRA where {name} is not null order by movie, {name}")
    FM.dump(f"out/{name}.json", obj)
    if name == "filmaffinity":
        FM.dump(f"rec/{name}.dct.txt", obj)
    elif name == "wikipedia":
        obj_es = {k: v for k, v in obj.items() if v.startswith("https://es.")}
        FM.dump(f"rec/{name}.dct.txt", obj_es)


def union(*args):
    s = set()
    for a in args:
        if a is None:
            continue
        if isinstance(a, dict):
            a = a.keys()
        s = s.union(a)
    return sorted(s)


DB.executescript(FM.load("sql/extra.sql"))


def complete(ids: Union[set[int], list[int], tuple[int, ...]]):
    ids = set(ids)

    logger.info(f"{len(ids)} IDS principales")

    wiki = load_dict("wikipedia")
    film = load_dict("filmaffinity")
    cntr = load_dict("countries")

    ids = set(ids).union(union(wiki, film, cntr))
    if len(ids):
        ids = DB.to_tuple(f"select id from movie where id {gW(ids)}", *ids)

    ids = set(ids)

    film = {
        **film,
        **WIKI.get_filmaffinity(*ids.difference(film.keys()))
    }
    wiki = {
        **wiki,
        **WIKI.get_wiki_url(*ids.difference(wiki.keys()))
    }
    cntr = {
        **cntr,
        **IMDB.get_countries(*ids.difference(cntr.keys()))
    }

    for i in ids.difference(film.keys()):
        year = DB.one("select year from MOVIE where id = ?", i)
        if year is None:
            continue
        titles = DB.to_tuple("select title from title where movie = ?", i)
        ff = FilmAffinityApi.search(year, *titles)
        if ff:
            film[i] = ff.id

    film_prioridad: list[tuple[str, int]] = []
    for k, f in film.items():
        old = cntr.get(k)
        if isinstance(old, str) and len(old.split()) == 1:
            film_prioridad.append((k, f))
            continue
        film_prioridad.insert(0, (k, f))

    for k, f in film_prioridad:
        fm = FilmAffinityApi.get(f)
        if fm is None:
            continue
        if fm.country is not None:
            cntr[k] = fm.country
        if fm.genres and 'Telefilm' in fm.genres:
            DB.executemany("UPDATE MOVIE SET type='tvMovie' where id = ?", (k,))
    DB.flush()

    for i in union(film, wiki, cntr):
        DB.executemany(
            "INSERT INTO EXTRA (movie, filmaffinity, wikipedia, countries) values (?, ?, ?, ?)",
            (i, film.get(i), wiki.get(i), cntr.get(i))
        )
    DB.flush()
    for field, (om_field, fm_field) in {
        'year': ('Year', 'year'),
        'duration': ('Runtime', 'duration')
    }.items():
        if len(ids) == 0:
            continue
        for i in DB.to_tuple(f"select id from movie where {field} is null and id {gW(ids)}", *ids):
            om = IMDB.get_from_omdbapi(i)
            value = om.get(om_field) if om else None
            if isinstance(value, str):
                value = value.strip()
                if value.isdecimal():
                    value = int(value)
                elif re.match(r"^\d+ min$", value):
                    value = int(value.split()[0])
            if not isinstance(value, int):
                fm = FilmAffinityApi.get(film.get(i))
                value = fm._asdict().get(fm_field) if fm else None
            if isinstance(value, int):
                DB.executemany(
                    f"UPDATE MOVIE SET {field}=? where id=?",
                    (value, i)
                )
        DB.flush()
    DB.commit()

    dump_dict('wikipedia')
    dump_dict('filmaffinity')
    dump_dict('countries')


MAIN_URLS = tuple(environ.get('SCRAPE_URLS', '').split())
complete(IMDB.scrape(*MAIN_URLS))

MAIN_FILM = FilmAffinityApi.scrape(*MAIN_URLS)
MAIN_FILM = set(MAIN_FILM).difference_update(
    DB.to_tuple("select filmaffinity from EXTRA where filmaffinity is not null")
)
MAIN_FILM_IMDB = WIKI.get_imdb(MAIN_FILM).values()
complete(MAIN_FILM_IMDB)

if CF.error:
    for e in CF.error:
        logger.critical(e)
