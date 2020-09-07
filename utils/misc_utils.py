if __name__ == '__main__':
    import sys
    sys.path.append('../')

import datetime
import os
import time
import pandas as pd
from random import randint
from typing import Union, Dict
from json import dumps
from utils.custom_types import *
from utils.Filesys import generic_FileServer


FS = generic_FileServer


def getRandomDelay(minimum: int, maximum: int) -> int:
    r = randint(minimum, maximum)
    return r


def getToday() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d")


# TODO (p1): DEPRECATE
def saveData(data: pd.DataFrame, name: str):
    save_csv(data, name)


def sleep(seconds: Union[None, int, float]):
    if seconds == 0 or seconds is None:
        return
    for i in range(seconds // 1):
        if i % 5 == 0:
            print(f"Sleeping - about {seconds - (i)} seconds left")
        time.sleep(1)


def deepSearch(name: Union[Country_Fullname, Region_Fullname, str], obj: Union[dict, list],
               return_children: bool = False, return_parents: bool = False, path: list = [],
               include_children_names: bool = False) -> Union[tuple, str, bool, dict]:
    if len(path) > 0:
        parent = path[-1]
    else:
        parent = ''
    if isinstance(obj, list):
        for item in obj:
            r = deepSearch(name, item, path=path, return_children=return_children, return_parents=return_parents,
                           include_children_names=include_children_names)
            if r:
                return r
    else:
        r = obj.get('name', False)
        match = r == name
        if match:
            if return_parents:
                return obj['id'], parent
            elif return_children:
                children: Union[bool, list] = obj.get('children', False)
                if children:
                    if not include_children_names:
                        children_ids = [x['id'] for x in children]
                    else:
                        children_ids = {f"{obj['id']}-{x['id']}": x['name'] for x in children}
                    return obj['id'], children_ids
                else:
                    return obj['id'], False
            else:
                return obj['id']
        else:
            children = obj.get('children', False)
            if children:
                new_path = [x for x in path]
                new_path.append(obj['id'])
                return deepSearch(name, children, path=new_path, return_children=return_children,
                                  return_parents=return_parents, include_children_names=include_children_names)
            else:
                return False


# TODO (p1): DEPRECATE
def getRegions(country_name: Country_Fullname, locales_obj: dict) -> Union[None, list]:
    if country_name == 'France':
        override_list = [
            {"name": "Brittany", "id": "E"},
            {'name': "Centre-Val de Loire", 'id': "F"},
            {'name': "Corsica", 'id': "H"},
            {'name': "Pays de la Loire", 'id': "R"},
            {'name': "Provence-Alpes-Côte d'Azur", 'id': "U"},
            {'name': "Île-de-France", 'id': "J"},
            {'name': "Grand-est", 'id': 'GE'},
            {'name': "Hauts-de-france", 'id': 'HF'},
            {'name': "Normandie", 'id': "NO"},
            {'name': "Bourgogne-Franche-Comté", 'id': "BFC"},
            {'name': "Nouvelle-Aquitaine", 'id': "NA"},
            {'name': "Occitanie", 'id': "OC"},
            {'name': "Auvergne-Rhône-Alpes", 'id': "AR"}
        ]
        return override_list
    all_countries: list = locales_obj['children']
    for country in all_countries:
        if country['name'] == country_name:
            return country['children']
    return None


def getDirectory(path_steps: list) -> str:
    cwd = FS.cwd
    directory = cwd
    for step in path_steps:
        directory = os.path.join(directory, step)
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def generateRegionPrioJSON():
    print(f"{lcol.FAIL}THIS FUNCTION IS OUTDATED: generateRegionPrioJSON{lcol.ENDC}")
    all = {}
    ccs = ['AT', 'CH', 'DE', 'ES', 'FR', 'IT']
    for cc in ccs:
        filename = f"GEO_{cc}_['google'].csv"
        cc_data = pd.read_csv(filename)
        print(cc_data.head())
        cc_data.sort_values('google', axis=0, ascending=False, inplace=True)
        print(cc_data.head())
        all[cc] = cc_data['geoCode'].tolist()
    with open(FS.Ordered_Regions, "w+") as file:
        file.write(dumps(all))


def reverseDict(obj: Dict[Union[str, int], Union[str,int]]) -> Dict[Union[str, int], Union[str,int]]:
    return {obj[key]: key for key in obj}


class lcol:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def deduplicateColumns(df: pd.DataFrame, extra: str = None) -> pd.DataFrame:
    columns = [col.split("_")[0] if len(col.split("_")) > 1 else col.strip() for col in df.columns]
    to_drop = [(item, columns.count(item)) for item in columns if columns.count(item) > 1]
    all_columns = df.columns
    dropped = []
    for i, col in enumerate(all_columns):
        for item, count in to_drop:
            if item in col and item not in dropped:
                try:
                    df = df.drop(col, axis=1)
                    dropped.append(item)
                except Exception as e:
                    print(e)
                    if isinstance(e, KeyError):
                        print(df.columns)
    if extra:
        for col in df.columns:
            if extra in col:
                df = df.drop(col, axis=1)
    for col in df.columns:
        if "_" in col:
            df.rename(mapper={col: col.split("_")[0]}, inplace=True, axis=1)
    return df


def translate_item(item: str, translations: dict):
    return translations.get(item, item)


def translate_arr(arr: list, translations: dict):
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(translate_arr(i, translations))
        elif isinstance(i, dict):
            new_arr.append(translate_dict(i, translations))
        else:
            new_arr.append(translate_item(i, translations))
    return new_arr


def translate_dict_entry(key: str, val, translations: dict):
    if isinstance(val, list):
        new_val = translate_arr(val, translations)
    elif isinstance(val, dict):
        new_val = translate_dict(val, translations)
    else:
        new_val = translate_item(val, translations)
    return translate_item(key, translations), new_val


def translate_dict(dictionary: dict, translations: dict):
    new = {}
    for key, val in dictionary.items():
        new_key, new_val = translate_dict_entry(key, val, translations)
        new[new_key] = new_val
    return new


def save_csv(df: pd.DataFrame, filepath: Filepath, **kwargs):
    base_dir = os.path.split(filepath)[0]
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    df.to_csv(filepath, **kwargs)
    print(f"Saved file: file://{filepath}")


if __name__ == '__main__':
    # generateRegionPrioJSON()
    parts = os.path.split(FS.cwd)
    print(parts)


def rescale_comparison(df: pd.DataFrame, scale: int = 1) -> pd.DataFrame:
    df = df.apply(lambda s: (s / df.max(axis=0, numeric_only=True).max()) * scale)
    return df