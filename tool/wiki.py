from sparql_tsv import SparqlTsv
from core.git import G
from core.filemanager import FM
from core.config_log import config_log
from typing import Callable, Optional, Any
from collections import defaultdict
import logging
import re

config_log(
    "log/wiki.log",
    strmLevel=logging.DEBUG
)

logger = logging.getLogger(__name__)


ST = SparqlTsv(
    endpoint="https://query.wikidata.org/sparql",
    user_agent=f'ImdbBoot/0.0 ({G.remote}; {G.mail})',
    max_retries=3
)

def safe_query(query: str, match: re.Pattern):
    try:
        yield from ST.query(query, match=match)
    except:
        pass


def get_dict(
    query: str,
    rgx: re.Pattern,
    cs_v: Optional[Callable[[str], Any]] = None
):
    k_v: dict[str, set] = defaultdict(set)
    v_k: dict[str, set] = defaultdict(set)
    for k, v in safe_query(
        query,
        match=rgx
    ):
        if cs_v:
            v = cs_v(v)
        if v is not None:
            k_v[k].add(v)
            v_k[v].add(k)
    obj: dict[str, str] = {}
    for k, vals in k_v.items():
        if len(vals)!=1:
            continue
        v = vals.pop()
        if len(v_k[v]) != 1:
            continue
        obj[k] = v
    return obj


def refresh(
    name: str,
    rgx: re.Pattern,
    cs_v: Optional[Callable[[str], Any]] = None
):
    obj = FM.load(f"rec/{name}.dct.txt")
    logger.info(f"{name} = {len(obj)} items")
    for k, v in get_dict(
        FM.load(f"sparql/{name}.lite.sparql"),
        rgx=rgx,
        cs_v=cs_v
    ).items():
        obj[k] = v
    logger.info(f"{name} = {len(obj)} items")
    FM.dump(f"rec/{name}.dct.txt", obj)


if __name__ == "__main__":
    refresh(
        "filmaffinity",
        rgx=re.compile(r"^tt\d{3,}\t\d{3,}$"),
        cs_v=int
    )
    refresh(
        "wikipedia",
        rgx=re.compile(r"^tt\d{3,}\thttps://es.wikipedia.org/wiki/.+$"),
    )
    
    