import csv
import logging
import sys
from typing import Any
import re
from core.req import R

logger = logging.getLogger(__name__)


csv.field_size_limit(sys.maxsize)


class KeyValueError(ValueError):
    def __init__(self, k: str, v):
        super().__init__(f"{k} = {v}")


def _parse_val(kv: tuple[str, Any]):
    k, v = kv
    try:
        return _parse_key_val(k, v)
    except ValueError as e:
        raise KeyValueError(k, v) from e


def _parse_key_val(k: str, v: Any):
    if isinstance(v, str):
        v = v.strip()
    if v in ('', '\\N'):
        v = None
    if v is None:
        if k in ('numVotes', 'averageRating'):
            return 0
        if k in ('directors', 'writers'):
            return tuple()
        return None
    if k in ('isOriginalTitle', ):
        if v not in ('0', '1', 1, 0, True, False):
            raise KeyValueError(f"{k} = {v}")
        return int(v) == 1
    if not isinstance(v, str):
        return v
    if k in ('directors', 'writers', 'genres'):
        return tuple(re.split(r'\s*,\s*', v))
    if k in ('averageRating', ):
        return float(v)
    if k in ('numVotes', 'ordering', 'startYear', 'endYear', 'runtimeMinutes'):
        return int(v)
    return v


def iter_list(url: str):
    logger.info(url)
    reader = R.iter_tsv(url)
    header = tuple(next(reader))
    logger.info(", ".join(map(str, header)))
    yield header
    for row in reader:
        kvs = zip(header, row)
        try:
            yield tuple(map(_parse_val, kvs))
        except KeyValueError as e:
            raise ValueError(str(row)) from e


def iter_tuples(
    url: str,
    *args: str,
):
    rows = iter_list(url)
    header = {k: i for i, k in enumerate(next(rows))}
    index = tuple(header[a] for a in args)
    for row in rows:
        vals = tuple(map(lambda i: row[i], index))
        yield vals


def iter_dict(url: str):
    rows = iter_list(url)
    header = next(rows)
    for row in rows:
        obj = dict(zip(header, row))
        yield obj
