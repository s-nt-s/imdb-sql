from sparql_tsv import SparqlTsv
from core.git import G
from core.filemanager import FM
from core.config_log import config_log
from typing import Callable, Optional, Any
import logging
import re

config_log(
    "log/wiki.log",
    strmLevel=logging.DEBUG
)

logger = logging.getLogger(__name__)
re_imdb = re.compile(r"^tt\d{3,}$")


ST = SparqlTsv(
    endpoint="https://query.wikidata.org/sparql",
    user_agent=f'ImdbBoot/0.0 ({G.remote}; {G.mail})',
    max_retries=100
)


def refresh(
    name: str,
    re_v: re.Pattern,
    cs_v: Optional[Callable[[str], Any]] = None
):
    obj = FM.load(f"rec/{name}.dct.txt")
    logger.info(f"{name} = {len(obj)} items")
    for k, v in ST.query(FM.resolve_path(f"sparql/{name}.sparql")):
        if k and v and re_imdb.match(k) and re_v.match(v):
            if cs_v:
                v = cs_v(v)
            obj[k] = v
    logger.info(f"{name} = {len(obj)} items")
    FM.dump(f"rec/{name}.dct.txt", obj)


if __name__ == "__main__":
    refresh(
        "filmaffinity",
        re_v=re.compile(r"^\d{3,}$"),
        cs_v=int
    )
    refresh(
        "wikipedia",
        re_v=re.compile(r"^https://es.wikipedia.org/wiki/.+$"),
    )
    
    