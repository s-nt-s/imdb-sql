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

config_log("log/complete_db.log")

logger = logging.getLogger(__name__)
DB = DBlite("imdb.sqlite", reload=False, quick_release=True)


def load_dict(name: str):
    r = None
    obj = {}
    url = f"{G.page}/{name}.json"
    file = FM.resolve_path(f"out/{name}.json")
    try:
        r = R.get_json(url)
    except Exception:
        pass
    if isinstance(r, dict) and r:
        logger.info(f"{url} = {len(r)} {name}")
        obj = {
            **obj,
            **r
        }
    if file.is_file():
        r = FM.load(file)
        if isinstance(r, dict) and r:
            logger.info(f"{file} = {len(r)} {name}")
            obj = {
                **obj,
                **r
            }
    try:
        r = DB.get_dict(f"select movie, {name} from EXTRA where {name} is not null")
        if r:
            logger.info(f"{DB.file} = {len(r)} {name}")
            obj = {
                **obj,
                **r
            }
    except OperationalError:
        pass
    return obj


def dump_dict(name: str):
    FM.dump(
        f"out/{name}.json",
        DB.get_dict(f"select movie, {name} from EXTRA where {name} is not null")
    )


def union(*args):
    s = set()
    for a in args:
        if a is None:
            continue
        if isinstance(a, dict):
            a = a.keys()
        s = s.union(a)
    return sorted(s)


wiki = load_dict("wikipedia")
film = load_dict("filmaffinity")
cntr = load_dict("countries")

ids = IMDB.scrape(*environ.get('SCRAPE_URLS', '').split())
if len(ids):
    ids = DB.to_tuple(f"select id from movie where id {gW(ids)}", *ids)

DB.executescript(FM.load("sql/extra.sql"))

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

ids = union(film, wiki, cntr)
for i in ids:
    DB.executemany(
        "INSERT INTO EXTRA (movie, filmaffinity, wikipedia, countries) values (?, ?, ?, ?)",
        (i, film.get(i), wiki.get(i), cntr.get(i))
    )
DB.flush()
DB.commit()

dump_dict('wikipedia')
dump_dict('filmaffinity')
dump_dict('countries')

if CF.error:
    for e in CF.error:
        logger.critical(e)
