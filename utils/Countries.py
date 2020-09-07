if __name__ == '__main__':
    import sys
    sys.path.append('../')

import json
import os
from json import loads

from utils.custom_types import *
from typing import Tuple, Dict, List, Union
from utils.misc_utils import reverseDict, deepSearch, FS
from utils.custom_types import Country_Fullname, Region_Fullname, Region_Shortcode, Country_Shortcode
from utils.Filesys import generic_FileServer as FS
from utils.user_interaction_utils import choose_from_dict

# TODO (p1): Align the ways region ids are used!


countries_dict_eng: Dict[Country_Shortcode, Country_Fullname] = {
    "de": "Germany",
    "es": "Spain",
    "fr": "France",
    "it": "Italy",
    "at": "Austria",
    "ch": "Switzerland"
}

countries_dict_local: Dict[Country_Shortcode, Country_Fullname] = {
    "de": "Deutschland",
    "es": "España",
    "fr": "France",
    "it": "Italia",
    "at": 'Österreich',
    'ch': "Schweiz"
}

region_merges: Dict[Country_Shortcode, Dict[Column_Name, Dict[Region_Fullname, List[Region_Fullname]]]] = {
    'FR': {
        "ticket_geo_region_name": {
            "Grand-est": ["Alsace", "Champagne-Ardenne", "Lorraine"],
            "Hauts-de-france": ["Picardie", "Nord-Pas-de-Calais"],
            "Normandie": ['Basse Normandie', 'Haute Normandie'],
            "Bourgogne-Franche-Comté": ["Bourgogne", "Franche-Comté"],
            "Nouvelle-Aquitaine": ["Aquitaine", "Limousin", "Poitou-Charentes"],
            "Occitanie": ["Languedoc-Roussillon", "Midi-Pyrénées"],
            "Auvergne-Rhône-Alpes": ["Auvergne", "Rhône-Alpes"]
        },
        'geoName': {
            "Grand-est": ["Alsace", "Champagne-Ardenne", "Lorraine"],
            "Hauts-de-france": ["Picardie", "Nord-Pas-de-Calais"],
            "Normandie": ['Basse Normandie', 'Haute Normandie'],
            "Bourgogne-Franche-Comté": ["Bourgogne", "Franche-Comté"],
            "Nouvelle-Aquitaine": ["Aquitaine", "Limousin", "Poitou-Charentes"],
            "Occitanie": ["Languedoc-Roussillon", "Midi-Pyrénées"],
            "Auvergne-Rhône-Alpes": ["Auvergne", "Rhône-Alpes"]
        }
    }
}

merged_regions_override = {
    "Alsace": "FR-A",
    "Aquitaine": "FR-B",
    "Auvergne": "FR-C",
    "Brittany": "FR-E",
    "Burgundy": "FR-D",
    "Centre-Val de Loire": "FR-F",
    "Champagne-Ardenne": "FR-G",
    "Corsica": "FR-H",
    "Franche-Comté": "FR-I",
    "Languedoc-Roussillon": "FR-K",
    "Limousin": "FR-L",
    "Lorraine": "FR-M",
    "Lower Normandy": "FR-P",
    "Midi-Pyrénées": "FR-N",
    "Nord-Pas-de-Calais": "FR-O",
    "Pays de la Loire": "FR-R",
    "Picardy": "FR-S",
    "Poitou-Charentes": "FR-T",
    "Provence-Alpes-Côte d'Azur": "FR-U",
    "Rhone-Alpes": "FR-V",
    "Upper Normandy": "FR-Q",
    "Île-de-France": "FR-J",
    "Grand-est": 'FR-GE',
    "Hauts-de-france": 'FR-HF',
    "Normandie": "FR-NO",
    "Bourgogne-Franche-Comté": "FR-BFC",
    "Nouvelle-Aquitaine": "FR-NA",
    "Occitanie": "FR-OC",
    "Auvergne-Rhône-Alpes": "FR-AR"
}


class Country:
    """
    Represents a country. Has APIs for main Country-related actions
    """

    def __init__(self, short_name: str = '', full_name: str = ''):
        """
        Initializes a Country. Raises KeyError if country is not listed in countries_dict_eng
        One of short_name or full_name need to be filled
        Args:
            short_name: str -- optional, shortcode of the country
            full_name: str -- optional, full name of the country (English)
        """
        if len(short_name) > 0:
            given = countries_dict_eng.get(short_name.lower(), False)
            if given:
                self.Shortcode: Country_Shortcode = short_name.upper()
                self.Full_name: Country_Fullname = given
            else:
                raise KeyError(f"The short_name {short_name} is invalid")
        elif len(full_name) > 0:
            given = reverseDict(countries_dict_eng).get(full_name, False)
            if given:
                self.Shortcode = given
                self.Full_name = full_name
            else:
                raise KeyError(f"The full name {short_name} is invalid")
        else:
            raise KeyError(f"No short_name or full_name were given")
        self.Database: str = f"prontopro" if self.Shortcode.lower() == 'it' else f"prontopro_{self.Shortcode.lower()}"
        self.Local_Name: Country_Fullname = countries_dict_local.get(self.Shortcode.lower())
        self.has_merged_regions: bool = isinstance(region_merges.get(self.Shortcode, False), dict)

    def get_regions(self, include_self: bool = False) -> Union[List[Dict[str, str]], None]:
        """
        Mirrors getRegions function in misc_utils.py
        Returns:

        """
        # TODO (p1): Integrate widely
        locales_obj: dict = readInLocales()
        all_countries: list = locales_obj['children']
        for country in all_countries:
            if country['name'] == self.Full_name:
                if include_self:
                    return [{'name': self.Full_name, 'id': country['id']}].extend(country['children'])
                else:
                    return country['children']
        return None

    @property
    def bare_region_ids(self) -> List[str]:
        regs = self.get_regions()
        return [x['id'] for x in regs]

    @property
    def region_ids(self) -> List[str]:
        bare = self.bare_region_ids
        return [f"{self.Shortcode.upper()}-{bare_id}" for bare_id in bare]

    @property
    def region_ids_to_names(self) -> Dict[str, str]:
        return get_region_id_to_name_dict(self.Full_name)


def getCountry(prompt: str = None) -> Country:
    """
    Handles user interaction to select a Country object
    Returns:

    """
    if prompt:
        print(prompt)
    while True:
        try:
            c = Country(full_name=choose_from_dict(countries_dict_eng))
            return c
        except Exception as e:
            print(e)


def readInLocales() -> Dict[str, Union[str, List[Dict[str, Union[List[Dict[str, str]], str]]]]]:
    with open(FS.All_Google_Locales, "r") as f:
        allLocales = f.read()
    allLocales = json.loads(allLocales)
    return allLocales


def findLocale(name: Union[Country_Fullname, Region_Fullname, str], isRegion: bool = False,
               includeChildren: bool = False):
    locales: dict = readInLocales()
    if isRegion:
        regionId, parentId = deepSearch(name, locales, return_parents=True)
        result: Region_Shortcode = Region_Shortcode(f"{parentId}-{regionId}")
    elif includeChildren:
        r = deepSearch(name, locales, return_children=True)
        return r
    else:
        result: Union[tuple, str, bool] = deepSearch(name, locales)
    return result


def get_region_id_to_name_dict(country_name: Country_Fullname, allow_override: bool = False):
    if allow_override and country_name == 'France':
        return reverseDict(merged_regions_override)
    else:
        locales: dict = readInLocales()
        cc_id, region_ids_dict = deepSearch(country_name, locales, return_children=True, include_children_names=True)
        return region_ids_dict


def make_region_id_name_dict(regions_dict: List[Dict[str, str]]) -> Dict[Region_Shortcode, Region_Fullname]:
    region_ids = {r['id']: r['name'] for r in regions_dict}
    return region_ids


def generateRegionIds(country_name: Country_Fullname, sort: bool = True, override: bool = True) -> List[Region_Shortcode]:
    """
    Called by generateSummaries.py
    Args:
        override:
        country_name:
        sort:

    Returns:

    """
    if country_name == 'France' and override:
        override_list = [
            "FR", "FR-E", "FR-F", "FR-H", "FR-R", "FR-U", "FR-J",
            'FR-GE', 'FR-HF', "FR-NO", "FR-BFC", "FR-NA",
            "FR-OC", "FR-AR"
        ]
        return override_list
    ccInfo = findLocale(country_name, includeChildren=True)
    regionIds = [f"{ccInfo[0]}-{x}" for x in ccInfo[1]]
    if sort:
        regionIds = sortRegionIds(regionIds, ccInfo[0])
    locales_list = [ccInfo[0]] + regionIds
    return locales_list


def sortRegionIds(regionIds: list, cc: Country_Shortcode) -> list:
    with open(os.path.join(FS.Statics, "ordered_regions.json"), "r") as f:
        dictionary = loads(f.read())
    ids = dictionary.get(cc, False)
    if ids:
        return ids
    else:
        print(f'No presorted region-ids available for {cc}')
        return regionIds


regions_map_english_to_local = {
    "Bavaria": 'Bayern',
    'Hesse': 'Hessen',
    'Lower Saxony': 'Niedersachsen',
    'Mecklenburg-Vorpommern': 'Mecklenburg-Vorpommern',
    'North Rhine-Westphalia': 'Nordrhein-Westfalen',
    'Rhineland-Palatinate': 'Rheinland-Pfalz',
    'Saxony': 'Sachsen',
    'Saxony-Anhalt': 'Sachsen-Anhalt',
    'Thuringia': 'Thüringen',
    'Germany': 'Deutschland',
    "Andalusia": "Andalucía",
    "Aragon": "Aragón",
    "Balearic Islands": "Illes Balears",
    "Basque Country": "País Vasco",
    "Canary Islands": "Islas Canarias",
    "Castile and León": "Castilla y León",
    "Castile-La Mancha": "Castilla-La Mancha",
    "Catalonia": "Cataluña",
    "Community of Madrid": "Madrid",
    "Extremadura": "Extremadura",
    "Navarre": "Navarra",
    "Region of Murcia": "Murcia",
    "Valencian Community": "Valencia",
    "Spain": "España",
    "Brittany": "Bretagne",
    "Burgundy": "Bourgogne",
    "Centre-Val de Loire": "Centre-Val de Loire",
    "Corsica": "Corse",
    "Lower Normandy": "Basse Normandie",
    "Midi-Pyrénées": "Midi-Pyrénées",
    "Picardy": "Picardie",
    "Provence-Alpes-Côte d'Azur": "Provence-Alpes-Côte d'Azur",
    "Rhone-Alpes": "Rhône-Alpes",
    "Upper Normandy": "Haute Normandie",
    "Aosta": "Valle d'Aosta",
    "Tuscany": "Toscana",
    "Trentino-Alto Adige/South Tyrol": "Trentino-Alto Adige",
    "Sicily": "Sicilia",
    "Apulia": "Puglia",
    "Sardinia": "Sardegna",
    "Piedmont": "Piemonte",
    "Friuli-Venezia Giulia": "Friuli Venezia Giulia",
    "Italy": "Italia",
    "Lombardy": 'Lombardia'
}

if __name__ == '__main__':
    print(get_region_id_to_name_dict(Country('de').Full_name))
