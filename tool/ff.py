from core.filmaffinity import FilmAffinityApi
from core.dblite import DBlite, gW
from core.wiki import WIKI
import sys
from time import sleep


DB = DBlite("imdb.sqlite", quick_release=True)
i_f = WIKI.get_imdb_filmaffinity()
done: set[str] = set(i_f.keys()).union(
    DB.to_tuple('select movie from extra where filmaffinity is not null')
)
for x, (i, year) in enumerate(DB.select('''
    select
        id,
        year
    from
        movie
    where
        year is not null and
        duration > 60 and
        votes > 10000
    order by
        votes desc,
        rating desc,
        duration desc,
        year desc
    '''
)):
    if x < 10000:
        continue
    if i in done:
        continue
    if not FilmAffinityApi.ACTIVE:
        sys.exit()
    tt = DB.to_tuple("select title from title where movie = ?", i)
    if len(tt) == 0:
        continue
    sleep(2)
    ff = FilmAffinityApi.search(year, *tt)
    if ff:
        print("wikimovie", i, ff.id)
