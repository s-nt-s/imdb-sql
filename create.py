from core.dblite import DBlite, gW
from core.tsv import iter_tuples
import logging
from core.filemanager import FM
from core.config_log import config_log
from core.imdb import IMDB
from core.wiki import WIKI
from os import environ

config_log("log/build_db.log")

logger = logging.getLogger(__name__)

DB = DBlite("imdb.sqlite", reload=True)


def isOkTitle(isOriginalTitle: bool, language: str, region: str, *args):
    if isOriginalTitle:
        return True
    if language is not None:
        return language in ('es', 'en')
    return region == 'ES'


def main():
    DB.executescript(FM.load("sql/schema.sql"))
    MAIN_MOVIES = populate_title_basic()
    populate_title_akas()
    populate_title_ratings(MAIN_MOVIES)
    #  populate_title_crew(MAIN_MOVIES)
    #  populate_names("WORKER")
    populate_title_director(MAIN_MOVIES)
    populate_names("DIRECTOR")
    finish_clean("DIRECTOR")
    DB.executescript(FM.load("sql/fix.sql"))


def populate_title_basic():
    MAIN_MOVIES = set(IMDB.scrape(*environ.get('SCRAPE_URLS', '').split()))
    logger.info(f"{len(MAIN_MOVIES)} MAIN_MOVIES")
    MISS_MOVIES = set(MAIN_MOVIES)

    for row in iter_tuples(
        'https://datasets.imdbws.com/title.basics.tsv.gz',
        'tconst',
        'titleType',
        'startYear',
        'runtimeMinutes',
        'primaryTitle',
        'originalTitle'
    ):
        if row[1] in ('videoGame', ):
            continue
        MISS_MOVIES.discard(row[0])
        DB.executemany(
            "INSERT INTO MOVIE (id, type, year, duration) VALUES (?, ?, ?, ?)",
            row[:4]
        )
        for v in row[-2:]:
            if v is not None:
                DB.executemany(
                    "INSERT OR IGNORE INTO TITLE (movie, title) VALUES (?, ?)",
                    (row[0], v)
                )
    if len(MISS_MOVIES):
        logger.debug(f"{len(MISS_MOVIES)} películas necesitan recuperarse a mano")
        for v in map(IMDB.get, sorted(MISS_MOVIES)):
            if not v:
                continue
            MISS_MOVIES.discard(v.id)
            DB.executemany(
                "INSERT INTO MOVIE (id, type, year, duration, votes, rating) VALUES (?, ?, ?, ?, ?, ?)",
                (v.id, v.typ, v.year, v.duration, v.votes, v.rating)
            )
            if v.title:
                DB.executemany(
                    "INSERT OR IGNORE INTO TITLE (movie, title) VALUES (?, ?)",
                    (v.id, v.title)
                )
    DB.flush()
    if len(MISS_MOVIES):
        logger.warning(f"{len(MISS_MOVIES)} películas no se han podido recuperar")

    MAIN_MOVIES = tuple(sorted(MAIN_MOVIES.difference(MISS_MOVIES)))
    return MAIN_MOVIES


def populate_title_akas():
    for row in iter_tuples(
        'https://datasets.imdbws.com/title.akas.tsv.gz',
        'titleId',
        'title',
        'isOriginalTitle',
        'language',
        'region',
    ):
        if not isOkTitle(*row[2:]):
            continue
        DB.executemany(
            "INSERT OR IGNORE INTO TITLE (movie, title) VALUES (?, ?)",
            row[:2]
        )
    DB.flush()


def populate_title_ratings(MAIN_MOVIES: tuple[str, ...]):
    for row in iter_tuples(
        "https://datasets.imdbws.com/title.ratings.tsv.gz",
        'averageRating',
        'numVotes',
        'tconst',
    ):
        if row[1:] == (0, 0):
            continue
        DB.executemany(
            "UPDATE MOVIE SET rating = ?, votes = ? where id = ?",
            row
        )
    DB.flush()
    if MAIN_MOVIES:
        for v in map(
            IMDB.get,
            DB.to_tuple(f"select id from MOVIE where votes = 0 and id {gW(MAIN_MOVIES)}", *MAIN_MOVIES)
        ):
            if v and v.votes > 0 and v.rating > 0:
                DB.executemany(
                    "UPDATE MOVIE SET rating = ?, votes = ? where id = ?",
                    (v.rating, v.votes, v.id)
                )
    DB.flush()


def populate_title_director(MAIN_MOVIES: tuple[str, ...]):
    for tconst, directors in iter_tuples(
        "https://datasets.imdbws.com/title.crew.tsv.gz",
        'tconst',
        'directors',
    ):
        for d in directors:
            DB.executemany(
                "INSERT OR IGNORE INTO DIRECTOR (movie, person) VALUES (?, ?)",
                (tconst, d)
            )
    DB.flush()
    DB.commit()
    if MAIN_MOVIES:
        MISS_DIRECTOR = set(DB.to_tuple(
            f"select id from movie where id in {MAIN_MOVIES+(-1, )} and id not in (select movie from DIRECTOR)"
        ))
        if MISS_DIRECTOR:
            logger.debug(f"{len(MISS_DIRECTOR)} películas necesitan recuperar el director a mano")
            for k, directors in WIKI.get_director(*sorted(MISS_DIRECTOR)).items():
                MISS_DIRECTOR.discard(k)
                for v in directors:
                    DB.executemany(
                        "INSERT OR IGNORE INTO DIRECTOR (movie, person) VALUES (?, ?)",
                        (k, v)
                    )
    DB.flush()
    if MISS_DIRECTOR:
        logger.warning(f"{len(MISS_DIRECTOR)} películas que no se ha podido recuperar el director")


def populate_title_crew(MAIN_MOVIES: tuple[str, ...]):
    for tconst, directors, writers in iter_tuples(
        "https://datasets.imdbws.com/title.crew.tsv.gz",
        'tconst',
        'directors',
        'writers'
    ):
        for d in directors:
            DB.executemany(
                "INSERT OR IGNORE INTO WORKER (movie, person, category) VALUES (?, ?, ?)",
                (tconst, d, 'director')
            )
        if tconst not in MAIN_MOVIES:
            continue
        for w in writers:
            DB.executemany(
                "INSERT OR IGNORE INTO WORKER (movie, person, category) VALUES (?, ?, ?)",
                (tconst, w, 'writer')
            )
    DB.flush()
    for tconst, nconst, category, ordering in iter_tuples(
        "https://datasets.imdbws.com/title.principals.tsv.gz",
        'tconst',
        'nconst',
        'category',
        'ordering',
    ):
        if category != 'director':
            if tconst not in MAIN_MOVIES or ordering > 10 or category not in ('writer', 'actor', 'actress'):
                continue
        DB.executemany(
            "INSERT OR IGNORE INTO WORKER (movie, person, category) VALUES (?, ?, ?)",
            (tconst, nconst, category)
        )
    DB.flush()
    DB.commit()
    if MAIN_MOVIES:
        MISS_DIRECTOR = set(DB.to_tuple(
            f"select id from movie where id in {MAIN_MOVIES+(-1, )} and id not in (select movie from WORKER where category='director')"
        ))
        if MISS_DIRECTOR:
            logger.debug(f"{len(MISS_DIRECTOR)} películas necesitan recuperar el director a mano")
            for k, directors in WIKI.get_director(*sorted(MISS_DIRECTOR)).items():
                MISS_DIRECTOR.discard(k)
                for v in directors:
                    DB.executemany(
                        "INSERT OR IGNORE INTO WORKER (movie, person, category) VALUES (?, ?, ?)",
                        (k, v, 'director')
                    )
    DB.flush()
    if MISS_DIRECTOR:
        logger.warning(f"{len(MISS_DIRECTOR)} películas que no se ha podido recuperar el director")


def populate_names(table_use_person: str):
    for nconst, primaryName in iter_tuples(
        'https://datasets.imdbws.com/name.basics.tsv.gz',
        'nconst',
        'primaryName',
    ):
        if primaryName is None:
            continue
        DB.executemany(
            "INSERT INTO PERSON (id, name) values (?, ?)",
            (nconst, primaryName)
        )
    DB.flush()

    ids = DB.to_tuple(f"select distinct person from {table_use_person} where person not in (select id from PERSON)")
    for row in IMDB.get_names(*ids).items():
        DB.executemany(
            "INSERT INTO PERSON (id, name) values (?, ?)",
            row
        )
    DB.flush()


def finish_clean(table_use_person: str):
    DB.commit()
    for t in ('TITLE', ):
        DB.execute(
            f"DELETE FROM {t} where movie not in (select id from MOVIE)",
            log_level=logging.INFO
        )
    DB.execute(
        f"DELETE FROM {table_use_person} where movie not in (select id from MOVIE) OR person not in (select id from PERSON)",
        log_level=logging.INFO
    )
    DB.execute(
        f"DELETE FROM PERSON where id not in (select person from {table_use_person})",
        log_level=logging.INFO
    )
    DB.commit()
    DB.close()


if __name__ == "__main__":
    main()
