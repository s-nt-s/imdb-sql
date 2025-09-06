import re
from os import environ
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin

re_sp = re.compile(r"\s+")
re_emb = re.compile(r"^image/[^;]+;base64,.*", re.IGNORECASE)

re_sp = re.compile(r"\s+")


def safe_num(s: str, default: int | float = None):
    if isinstance(s, (int, float)):
        return s
    if not isinstance(s, str):
        return default
    s = s.strip()
    if len(s) == 0:
        return default
    if s.isdecimal():
        return int(s)
    if re.match(r"^\d+\.\d+$", s):
        return float(s)
    m = re.match(r"^(\d+)\s+min$", s)
    if m:
        return int(m.group(1))
    return default


def safe_str(s: str, default: str = None):
    if not isinstance(s, str):
        return default
    s = re_sp.sub(" ", s).strip()
    if s in ('', 'N/A'):
        return default
    return s


def uniq(*args: str | None):
    arr: list[str] = []
    for a in args:
        if a not in (None, '') and a not in arr:
            arr.append(a)
    return arr


def tp_split(sep: str, s: str) -> tuple[str, ...]:
    if s is None:
        return tuple()
    spl = re.split(r"\s*"+re.escape(sep)+r"\s*", s)
    return tuple(uniq(*spl))


def get_env(*args: str, default: str = None) -> str | None:
    for a in args:
        v = environ.get(a)
        if isinstance(v, str):
            v = v.strip()
            if len(v):
                return v
    return default


def iter_chunk(size: int, args: list):
    arr = []
    for a in args:
        arr.append(a)
        if len(arr) == size:
            yield arr
            arr = []
    if arr:
        yield arr

def iterhref(soup: BeautifulSoup):
    """Recorre los atributos href o src de los tags"""
    n: Tag
    for n in soup.findAll(["img", "form", "a", "iframe", "frame", "link", "script", "input"]):
        attrs = ("href", ) if n.name in ("a", "link") else ("src", "data-src")
        if n.name == "form":
            attrs = ("action", )
        for attr in attrs:
            val = n.attrs.get(attr)
            if val is None or re_emb.search(val):
                continue
            if not (val.startswith("#") or val.startswith("javascript:")):
                yield n, attr, val


def buildSoup(root: str, source: str, parser="html.parser"):
    soup = BeautifulSoup(source, parser)
    for n, attr, val in iterhref(soup):
        val = urljoin(root, val)
        n.attrs[attr] = val
    return soup
