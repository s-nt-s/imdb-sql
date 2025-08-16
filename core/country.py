from pycountry.db import Country as DBCountry
from pycountry import countries as DBCountries, historic_countries
import logging
import re

logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")

CUSTOM_ALIASES = {
    "FRG": ("Alemania Occidental", "West Germany"),
    "DDR": ("Alemania Oriental", "East Germany"),
    "SUN": ("Soviet Union", "Unión soviética", "URSS"),
    "PSE": ("Occupied Palestinian Territory",),
    "YUG": ("Yugoslavia", "Yugoslavia, (Socialist) Federal Republic of"),
    "TUR": ("Turkey", "Türkiye"),
    "RUS": ("Russia", "Russian Federation"),
    "GBR": ("UK", "United Kingdom"),
    "TWN": ("ROC", "TAI", "Taiwán"),
    "DEU": ("GER", "Alemania")
}


class CountryFinder:
    def __init__(self):
        self.__error: list[str] = []

    def __log_error(self, msg: str):
        if msg not in self.__error:
            logger.critical(msg)
            self.__error.append(msg)

    def __parse_alpha3(self, cod: str):
        cod = cod.strip().upper()
        if DBCountries.get(alpha_3=cod) is not None:
            return cod
        if historic_countries.get(alpha_3=cod) is not None:
            return cod
        if cod in CUSTOM_ALIASES.keys():
            return cod
        for k, v in CUSTOM_ALIASES.items():
            if cod in v:
                return k
        return None

    def parse_alpha3(self, cod: str):
        if cod in (None, '', 'N/A'):
            return None
        c = self.__parse_alpha3(cod)
        if c is not None:
            return c
        self.__log_error(f"Códio alpha3 de país no encontrado: {cod}")

    def __search_country_by_name(self, name: str):
        c: DBCountry = DBCountries.get(name=name)
        if c is not None:
            return c
        lw_name = name.lower()
        for c in DBCountries:
            for f in ("name", "official_name", "common_name"):
                if hasattr(c, f):
                    value = getattr(c, f)
                    if not isinstance(value, str):
                        continue
                    if lw_name == value.lower():
                        return c
        for c in historic_countries:
            for f in ("name", "official_name", "common_name"):
                if hasattr(c, f):
                    value = getattr(c, f)
                    if not isinstance(value, str):
                        continue
                    if lw_name == value.lower():
                        return c
        return None

    def to_alpha_3(self, s: str):
        if s is None:
            return None
        s = re_sp.sub(" ", s).strip()
        if s in ('', 'N/A'):
            return None
        for k, v in CUSTOM_ALIASES.items():
            if s in v:
                return k
        c = self.__search_country_by_name(name=s)
        if c is not None:
            return c.alpha_3.upper()
        if s == s.upper() and len(s) == 3:
            cod = self.__parse_alpha3(s)
            if cod is not None:
                return cod
        self.__log_error(f"País no encontrado: {s}")
        return None

    @property
    def error(self):
        return tuple(self.__error)


CF = CountryFinder()
