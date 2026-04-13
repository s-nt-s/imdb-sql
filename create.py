from core.dblite import DBlite, gW
from core.tsv import iter_tuples, iter_dict
import logging
from core.filemanager import FM
from core.config_log import config_log
from core.imdb import IMDB
from core.wiki import WIKI
from os import environ
from typing import NamedTuple

config_log("log/build_db.log")

logger = logging.getLogger(__name__)

DB = DBlite("imdb.sqlite", reload=True)
KO_GENRES = set({
    'Adult',
    'Game-Show',
    'Reality-TV',
    'Talk-Show',
})
GENRES = set({
    'Action',
    'Adventure',
    'Animation',
    'Biography',
    'Comedy',
    'Crime',
    'Documentary',
    'Drama',
    'Family',
    'Fantasy',
    'Film-Noir',
    'History',
    'Horror',
    'Music',
    'Musical',
    'Mystery',
    'News',
    'Romance',
    'Sci-Fi',
    'Short',
    'Sport',
    'Thriller',
    'War',
    'Western',
}).union(KO_GENRES)
TYPES = set({
    'movie',
    'short',
    'tvEpisode',
    'tvMiniSeries',
    'tvMovie',
    'tvPilot',
    'tvSeries',
    'tvShort',
    'tvSpecial',
    'video',
    'videoGame',
})


class Movie(NamedTuple):
    tconst: str
    titleType: str
    startYear: int
    runtimeMinutes: int
    genres: tuple[str, ...]
    isAdult: bool
    primaryTitle: str | None
    originalTitle: str | None

    @classmethod
    def build(cls, obj: dict):
        obj = {k: v for k, v in obj.items() if k in cls._fields}
        genres = obj.get('genres')
        if not genres:
            obj['genres'] = tuple()
        m = cls(**obj)
        g_ko = set(m.genres).difference(GENRES)
        if len(g_ko):
            raise ValueError(f"genres={', '.join(sorted(g_ok))}")
        if m.titleType not in TYPES:
            raise ValueError(f"titleType={m.titleType}")
        return m


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
    populate_title_director(MAIN_MOVIES)
    populate_names()
    finish_clean()
    DB.executescript(FM.load("sql/fix.sql"))


def isDisposable(m: Movie):
    if m.titleType in ('videoGame', ):
        return f"titleType={m.titleType}"
    ok_gnrs = set(m.genres).difference(KO_GENRES)
    if m.genres and m.titleType not in ('movie', 'tvMovie'):
        if len(ok_gnrs.difference({
            'News',
            'Sport',
        })) == 0:
            return f"titleType={m.titleType} genres={', '.join(m.genres)}"
        if m.titleType in ('short', 'tvShort', ):
            if len(ok_gnrs.difference({
                'Music',
            })) == 0:
                return f"titleType={m.titleType} genres={', '.join(m.genres)}"
        if m.runtimeMinutes is not None and m.runtimeMinutes < 10:
            if len(ok_gnrs.difference({
                'Music',
            })) == 0:
                return f"titleType={m.titleType} runtimeMinutes={m.runtimeMinutes} genres={', '.join(m.genres)}"
    if m.isAdult and len(ok_gnrs) == 0:
        return f"isAdult={m.isAdult} genres={', '.join(m.genres)}"
    if (m.originalTitle, m.primaryTitle) == (None, None):
        return "title=None"
    if (m.runtimeMinutes or 0) <= 0 and (m.startYear or 0) <= 1880:
        return f"runtimeMinutes={m.runtimeMinutes} startYear={m.startYear}"


def populate_title_basic():
    MAIN_MOVIES = set(IMDB.scrape(*environ.get('SCRAPE_URLS', '').split()))
    logger.info(f"{len(MAIN_MOVIES)} MAIN_MOVIES")
    MISS_MOVIES = set(MAIN_MOVIES)

    count = 0
    disposable: dict[str, int] = {}
    for m in map(Movie.build, iter_dict(
        'https://datasets.imdbws.com/title.basics.tsv.gz',
    )):
        msgDisposable = isDisposable(m)
        if msgDisposable:
            disposable[msgDisposable] = disposable.get(msgDisposable, 0) + 1
            continue
        count = count + 1
        MISS_MOVIES.discard(m.tconst)
        DB.executemany(
            "INSERT INTO MOVIE (id, type, year, duration) VALUES (?, ?, ?, ?)",
            (m.tconst, m.titleType, m.startYear, m.runtimeMinutes)
        )
        for v in (m.primaryTitle, m.originalTitle):
            if v is not None:
                DB.executemany(
                    "INSERT OR IGNORE INTO TITLE (movie, title) VALUES (?, ?)",
                    (m.tconst, v)
                )

    disp = sorted(disposable.items(), key=lambda kv: (-kv[1], kv[0]))
    total = count + (sum((kv[1] for kv in disp)) if disp else 0)
    line = "{:%sd}" % (len(str(total)))
    logger.info(line.format(total) + " movies")
    for k, v in sorted(disposable.items(), key=lambda kv: (-kv[1], kv[0])):
        logger.info(line.format(-v) + f" x {k}")
    logger.info("===============")
    logger.info(line.format(count) + " movies")

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
    MISS_DIRECTOR = tuple()
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


def populate_names():
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

    ids = DB.to_tuple(f"select distinct person from DIRECTOR where person not in (select id from PERSON)")
    for row in IMDB.get_names(*ids).items():
        DB.executemany(
            "INSERT INTO PERSON (id, name) values (?, ?)",
            row
        )
    DB.flush()


def finish_clean():
    DB.commit()
    DB.execute(
        "DELETE FROM movie where id not in (select movie from DIRECTOR)",
        log_level=logging.INFO
    )
    for t in ('TITLE',):
        DB.execute(
            f"DELETE FROM {t} where movie not in (select id from MOVIE)",
            log_level=logging.INFO
        )
    DB.execute(
        "DELETE FROM DIRECTOR where movie not in (select id from MOVIE) OR person not in (select id from PERSON)",
        log_level=logging.INFO
    )
    DB.execute(
        "DELETE FROM PERSON where id not in (select person from DIRECTOR)",
        log_level=logging.INFO
    )
    DB.commit()
    DB.close()


if __name__ == "__main__":
    main()
