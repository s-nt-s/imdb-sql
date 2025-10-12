from core.util import buildSoup
from core.cache import StaticCache
from bs4 import BeautifulSoup, Tag
from core.country import CF
import re
from typing import NamedTuple, Optional
import cloudscraper
import logging
from datetime import datetime
from urllib.parse import quote


logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")

FM_SCRAPER = cloudscraper.create_scraper()


class FilmAffinityError(ValueError):
    pass


def _get_soup(url: str):
    soup = buildSoup(url, FM_SCRAPER.get(url).text)
    title_none = "not title found"
    txt = get_text(soup.select_one("title")) or title_none
    if txt.lower() in (title_none, "too many request", ):
        raise FilmAffinityError(txt)
    return soup


@StaticCache("cache/filmaffinity/{}.html")
def _get_html_from_id(id: int):
    url = f"https://www.filmaffinity.com/es/film{id}.html"
    soup = _get_soup(url)
    return str(soup)


def get_text(n: Tag | None) -> str | None:
    if not isinstance(n, Tag):
        return None
    txt = re_sp.sub(" ", n.get_text()).strip()
    if len(txt) == 0:
        return None
    return txt


class FilmAffinityCache(StaticCache):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, maxOld=90, **kwargs)

    def read(self, file, *args, **kwargs):
        js = super().read(file, *args, **kwargs)
        return self.__toFilmAffinity(js)

    def __toFilmAffinity(self, js: dict) -> "FilmAffinity":
        if not isinstance(js, dict):
            return None
        js = {k: v for k, v in js.items() if k != '__time__' and v is not None}
        try:
            return FilmAffinity(**js)
        except TypeError:
            return None

    def save(self, file, obj: "FilmAffinity", *args, **kwargs):
        js = obj._asdict()
        if self.__toFilmAffinity(js) is None:
            logger.warning(f"Ficha FilmAffinity incompleta: {obj}")
        js['__time__'] = datetime.now().isoformat()
        super().save(file, js, *args, **kwargs)


class FilmAffinity(NamedTuple):
    id: int
    title: str
    year: int
    url: str
    reviews: int
    votes: int
    country: str
    genres: tuple[str, ...]
    poster: str = None
    rate: float = None
    duration: int = None


class FilmAffinityApi:
    ACTIVE = True

    @FilmAffinityCache("out/filmaffinity/{}.json")
    @staticmethod
    def get(id: int):
        if id is None:
            return None
        id = int(id)
        api = FilmAffinityApi.__get(id)
        if api is None:
            return None
        return api.toFilmAffinity()

    def toFilmAffinity(self):
        return FilmAffinity(
            id=self.id,
            title=self.get_title(),
            year=self.get_year(),
            url=self.url,
            rate=self.get_rate(),
            votes=self.get_votes(),
            reviews=self.get_reviews(),
            country=self.get_country(),
            genres=self.get_genres(),
            poster=self.get_poster(),
            duration=self.get_duration()
        )

    @staticmethod
    def search(title: str, year: int):
        url = "https://www.filmaffinity.com/es/search.php?stype=title&em=1&stext="+quote(title)
        soup = _get_soup(url)
        link = soup.select_one('link[rel="alternate"][hreflang="es"][href]')
        _id_ = FilmAffinityApi.__extract_id_from_link(link)
        if _id_:
            fm = FilmAffinityApi(_id_, soup)
            if fm.get_year() == year:
                return fm
        ids: set[int] = set()
        for div in soup.select("div.searchres div.card-body"):
            span = get_text(div.select_one("span.mc-year"))
            if span is None:
                continue
            if int(span) != year:
                continue
            link = div.select_one("a[href]")
            _id_ = FilmAffinityApi.__extract_id_from_link(link)
            if _id_:
                ids.add(_id_)
        if len(ids) == 1:
            return FilmAffinityApi(ids.pop())

    @staticmethod
    def __extract_id_from_link(a: Tag):
        if a is None:
            return None
        href = a.attrs.get("href")
        if not isinstance(href, str):
            return None
        m = re.search(r"/film(\d+)\.html$", href)
        if m is None:
            return None
        return int(m.group(1))

    @property
    def id(self) -> int:
        return self.__id

    @property
    def url(self) -> str:
        return f"https://www.filmaffinity.com/es/film{self.id}.html"

    @staticmethod
    def __get(id: int):
        if not FilmAffinityApi.ACTIVE:
            return None
        try:
            return FilmAffinityApi(int(id))
        except FilmAffinityError as e:
            logger.critical(f"Error fetching film {id}: {e}")
            FilmAffinityApi.ACTIVE = False
            return None

    def __init__(self, id: int, soup: Optional[Tag] = None):
        self.__id = id
        self.__soup = soup
        if self.__soup is None:
            html = _get_html_from_id(id)
            self.__soup = BeautifulSoup(html, "html.parser")

    def __get_attr(self, slc: str, attr: str) -> str | None:
        n = self.__soup.select_one(slc)
        if n is not None:
            val = re_sp.sub(" ", n.attrs.get(attr) or '')
            if len(val):
                return val
        logger.critical(f"Valor no encontrado: {slc}[{attr}] {self.url}")

    def get_poster(self) -> str:
        return self.__get_attr("#movie-main-image-container img, #main-poster img", "src")

    def get_title(self) -> str:
        return get_text(self.__soup.select_one("h1 span[itemprop='name']"))

    def get_year(self) -> str:
        y = get_text(self.__soup.select_one("dd[itemprop='datePublished'], span[itemprop='datePublished']"))
        if y and y.isdecimal():
            return int(y)

    def get_duration(self) -> str:
        y = get_text(self.__soup.select_one("dd[itemprop='duration'], span[itemprop='duration']"))
        if y and re.match(r"^\d+ min\.?$", y):
            return int(y.split()[0])

    def get_country(self) -> str | None:
        slc = "dl.movie-info span#country-img img, dl img.nflag"
        src = self.__get_attr(slc, "src")
        alt = self.__get_attr(slc, "alt")
        if src is not None:
            cod = src.split("/")[-1].split(".")[0]
            alpha3 = CF.parse_alpha3(cod, silent=True)
            if alpha3:
                return alpha3
        if isinstance(alt, str):
            alpha3 = CF.to_alpha_3(alt, silent=True)
            if alpha3:
                return alpha3
        logger.critical(f"Código alpha3 de país no encontrado: cod={cod} alt={alt} {self.url}")

    def get_rate(self) -> float | None:
        return self.__get_itemprop("ratingValue", to=float)

    def get_votes(self) -> int:
        v = self.__get_itemprop("ratingCount", to=int)
        if v is None:
            return 0
        return v

    def get_reviews(self) -> int | None:
        txt_reviews = get_text(self.__soup.select_one("#movie-reviews-box"))
        if not isinstance(txt_reviews, str) or not re.match(r"^\d+\s+.*$", txt_reviews):
            return 0
        return int(txt_reviews.split()[0])

    def get_genres(self):
        arr: list[str] = []
        for g in map(get_text, self.__soup.select(f'dd.card-genres a')):
            if g and g not in arr:
                arr.append(g)
        if len(arr) == 0:
            return None
        return tuple(arr)

    def __get_itemprop(self, name: str, to: type):
        n = self.__soup.select_one(f'*[itemprop="{name}"][content]')
        if n is None:
            return None
        c = n.attrs.get('content')
        if c is None:
            return None
        if isinstance(c, str):
            c = re_sp.sub(" ", c).strip()
            if len(c) == 0:
                return None
        return to(c) if to else c


if __name__ == "__main__":
    import sys
    from core.config_log import config_log
    config_log("log/filmaffinity.log")

    for film in map(FilmAffinityApi.get, sys.argv[1:]):
        if film:
            print(film)
