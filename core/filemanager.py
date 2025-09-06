import json
import logging
from os import makedirs
from os.path import dirname, realpath
from pathlib import Path
from functools import cache
from core.req import R

from json.decoder import JSONDecodeError
from dataclasses import is_dataclass, asdict
import re

logger = logging.getLogger(__name__)


def myex(e, msg):
    largs = list(e.args)
    if len(largs) == 1 and isinstance(largs, str):
        largs[0] = largs[0]+' '+msg
    else:
        largs.append(msg)
    e.args = tuple(largs)
    return e


class FileManager:
    """
    Da funcionalidad de lectura (load) y escritura (dump) de ficheros
    """

    def __init__(self, root: str | Path = None):
        """
        Parameters
        ----------
        root: str | Path
            por defecto es la raiz del proyecto, es decir, el directorio ../.. al de este fichero
            se usa para interpretar que las rutas relativas son relativas a este directorio root
        """
        if root is None:
            root = Path(dirname(realpath(__file__))).parent
        elif isinstance(root, str):
            root = Path(root)

        self.__root = root

    def resolve_path(self, file: str | Path) -> Path:
        """
        Si es una ruta absoluta se devuelve tal cual
        Si es una ruta relativa se devuelve bajo la ruta root
        Si empieza por ~ se expande

        Parameters
        ----------
        file: str | Path
            Ruta a resolver
        """
        if isinstance(file, str):
            file = Path(file)

        if str(file).startswith("~"):
            file = file.expanduser()

        if file.is_absolute():
            return file

        root_file = self.__root.joinpath(file)

        return root_file

    def __get_ext(self, file: Path) -> str:
        """
        Normaliza extensiones para identificar el tipo de fichero en base a la extension
        """
        s_file = str(file).lower()
        for k, v in {
            ".dct.txt": "dct",
            ".qw.txt": "qw"
        }.items():
            if s_file.endswith(k):
                return v

        ext = file.suffix.lstrip(".")
        ext = ext.lower()
        return {
            "js": "json",
            "sql": "txt",
            "gql": "txt",
            "log": "txt",
            "html": "txt"
        }.get(ext, ext)

    def load(self, file: str | Path, *args, **kwargs):
        """
        Lee un fichero en funcion de su extension
        Para que haya soporte para esa extension ha de exisitir una funcion load_extension
        """
        file = self.resolve_path(file)

        ext = self.__get_ext(file)

        load_fl = getattr(self, "load_"+ext, None)
        if load_fl is None:
            raise Exception(
                "No existe metodo para leer ficheros {} [{}]".format(ext, file.name))

        return load_fl(file, *args, **kwargs)

    @cache
    def cached_load(self, file: str | Path, *args, **kwargs):
        return self.load(file, *args, **kwargs)

    def dump(self, file: str | Path, obj, *args, **kwargs):
        """
        Guarda un fichero en funcion de su extension
        Para que haya soporte para esa extension ha de exisitir una funcion dump_extension
        """
        file = self.resolve_path(file)
        makedirs(file.parent, exist_ok=True)

        ext = self.__get_ext(file)

        dump_fl = getattr(self, "dump_"+ext, None)
        if dump_fl is None:
            raise Exception(
                "No existe metodo para guardar ficheros {} [{}]".format(ext, file.name))

        dump_fl(file, obj, *args, **kwargs)

    def load_json(self, file, *args, **kwargs):
        for k in ('separators', 'indent'):
            if k in kwargs:
                del kwargs[k]
        with open(file, "r") as f:
            try:
                return json.load(f, *args, **kwargs)
            except JSONDecodeError as e:
                raise myex(e, str(file))

    def dump_json(self, file, obj, *args, indent=2, **kwargs):
        with open(file, "w") as f:
            json.dump(self.__parse(obj), f, *args, indent=indent, **kwargs)

    def load_txt(self, file, *args, **kwargs):
        with open(file, "r") as f:
            txt = f.read()
            if args or kwargs:
                txt = txt.format(*args, **kwargs)
            return txt

    def dump_txt(self, file, txt, *args, **kwargs):
        if args or kwargs:
            txt = txt.format(*args, **kwargs)
        with open(file, "w") as f:
            f.write(txt)

    def load_qw(self, file, *args, **kwargs):
        file = self.resolve_path(file)
        if not file.is_file():
            return tuple()
        txt = self.load_txt(file)
        if txt is None:
            return tuple()
        words = set(map(str.strip, txt.split()))
        words.discard('')
        return tuple(sorted(words))

    def load_dct(self, file, *args, **kwargs):
        file = self.resolve_path(file)
        if not file.is_file():
            return {}
        txt = self.load_txt(file)
        if txt is None:
            return {}
        obj = {}
        for tp in map(str.split, map(str.strip, re.split(r"\n+", txt.strip()))):
            if len(tp) != 2:
                continue
            obj[tp[0]] = tp[1]
        if all(map(str.isdecimal, obj.keys())):
            obj = {int(k): v for k, v in obj.items()}
        return obj

    def dump_dct(self, file, obj, *args, **kwargs):
        if not isinstance(obj, dict):
            return
        lns = []
        for k, v in sorted(obj.items()):
            if v is not None:
                lns.append(f"{k} {v}")
        txt = "\n".join(lns)
        self.dump_txt(file, txt)

    def __parse(self, obj):
        if getattr(obj, "_asdict", None) is not None:
            obj = obj._asdict()
        if is_dataclass(obj):
            obj = asdict(obj)
        if isinstance(obj, (list, tuple, set)):
            return tuple(map(self.__parse, obj))
        if isinstance(obj, dict):
            obj = {k: self.__parse(v) for k, v in obj.items()}
        return obj

    def rm(self, file: str | Path):
        file = self.resolve_path(file)
        if not file.exists():
            return
        if file.is_file():
            file.unlink()
        elif file.is_dir():
            file.rmdir()

    def dwn_json(self, url: str, target: str, default: list | dict):
        target = self.resolve_path(target)
        json = R.safe_get_json(url)
        if json is None:
            json = default
        self.dump(target, json)
        return json


# Mejoras dinamicas en la documentacion
for mth in dir(FileManager):
    slp = mth.split("_", 1)
    if len(slp) == 2 and slp[0] in ("load", "dump"):
        key, ext = slp
        mth = getattr(FileManager, mth)
        if mth.__doc__ is None:
            if key == "load":
                mth.__doc__ = "Lee "
            else:
                mth.__doc__ = "Guarda "
            mth.__doc__ = mth.__doc__ + "un fichero de tipo "+ext

FM = FileManager()
