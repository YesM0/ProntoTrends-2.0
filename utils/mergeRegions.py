if __name__ == '__main__':
    import sys

    sys.path.append('../')

from typing import Union, Dict, List, Tuple
import os
import pandas as pd
from utils.misc_utils import reverseDict, translate_dict, deduplicateColumns, rescale_comparison
from utils.user_interaction_utils import choose_from_dict, choose_multiple_from_dict
from utils.custom_types import *
from utils.Countries import region_merges, regions_map_english_to_local, getCountry
from utils.Filesys import generic_FileServer as FS

eng_code = {
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

override_list = [
    {"name": "Brittany", "id": "FR-E"},
    {'name': "Centre-Val de Loire", 'id': "FR-F"},
    {'name': "Corsica", 'id': "FR-H"},
    {'name': "Pays de la Loire", 'id': "FR-R"},
    {'name': "Provence-Alpes-Côte d'Azur", 'id': "FR-U"},
    {'name': "Île-de-France", 'id': "FR-J"},
    {'name': "Grand-est", 'id': 'FR-GE'},
    {'name': "Hauts-de-france", 'id': 'FR-HF'},
    {'name': "Normandie", 'id': "FR-NO"},
    {'name': "Bourgogne-Franche-Comté", 'id': "FR-BFC"},
    {'name': "Nouvelle-Aquitaine", 'id': "FR-NA"},
    {'name': "Occitanie", 'id': "FR-OC"},
    {'name': "Auvergne-Rhône-Alpes", 'id': "FR-AR"}
]


def doMerges_Geo(df: pd.DataFrame, cc_merges: Dict[Column_Name, Dict[Region_Fullname, List[Region_Fullname]]],
                 index_col='means', add_geo_code: bool = False):
    global eng_code
    if 'geoCode' in df.columns:
        df = df.drop(columns=['geoCode'])
    if index_col == -1:
        index_col = df.columns[-1]
    elif index_col is None:
        index_col = [col for col in df.columns if col != 'geoCode' and col != 'geoName']
    existent_cols = [col_name for col_name in cc_merges if col_name in df.columns]
    for col_name in existent_cols:
        items = cc_merges[col_name]
        ixcols = [index_col, col_name] if isinstance(index_col, str) else index_col.append(col_name)
        non_index_cols = [col for col in df.columns if col not in ixcols]
        exclude_subqueries = []
        for label, subitems in items.items():
            query = "|"
            subqueries = []
            for item in subitems:
                query_col_name = col_name if not ' ' in col_name else f"`{col_name}`"
                subqueries.append(f"{query_col_name} == '{item}'")
                exclude_subqueries.append(f"{query_col_name} != '{item}'")
            query = query.join(subqueries)
            selection = df.query(query)
            if len(non_index_cols) > 0:
                averaged = selection.groupby(non_index_cols).mean()
            else:
                averaged = selection.mean()
            averaged[col_name] = label
            # print(averaged)
            df = df.append(averaged, ignore_index=True)
        df = df.query(" & ".join(exclude_subqueries))
            # print(df)
    if isinstance(index_col, str):
        maximum = df[index_col].max()
        if not maximum == 100:
            df[index_col] = df[index_col].apply(lambda x: (x / maximum) * 100)
    else:
        maximum = df[index_col].max().max()
        if not maximum == 100:
            for col in index_col:
                df[col] = df[col].apply(lambda x: (x / maximum) * 100)
    if add_geo_code and len(existent_cols) > 0:
        df['geoCode'] = df[existent_cols[0]].apply(lambda x: eng_code.get(x, x))
        if isinstance(index_col, str):
            df = df[[existent_cols[0], 'geoCode', index_col]]
        else:
            order = [existent_cols[0], 'geoCode'].extend(index_col)
            df = df[order]
    return df


def do_merges_Time(folder: Folderpath,
                   cc_merges: Dict[Column_Name, Dict[Region_Shortcode, List[Region_Shortcode]]]) -> Tuple[
    List[Filepath], List[Filepath]]:
    new_files = []
    unnecessary_files = []
    for column, combinations in cc_merges.items():
        for rg_short, combis in combinations.items():
            merge_region_df: Union[pd.DataFrame, None] = None
            for rg in combis:
                try:
                    all_files = os.listdir(folder)
                    files: Union[bool, List[str]] = list(filter(lambda x: rg in x.split("_"), all_files))
                    if len(files) == 0:
                        file: Union[bool, str] = False
                    else:
                        file = os.path.join(folder, files[0])
                except Exception as e:
                    print(e)
                    file: bool = False
                if file:
                    unnecessary_files.append(file)
                    df = pd.read_csv(file)
                    df['date'] = pd.to_datetime(df.date)
                    df = df.set_index(df.date)
                    df = df.drop(columns=list(filter(lambda x: x if x in ['isPartial', 'date'] else None, df.columns)))
                    df = deduplicateColumns(df, extra='isPartial')
                    # if df.shape[1] <= 3:
                    #     df = df.iloc[:, 1:] # drop index if needed
                    if merge_region_df is None:
                        merge_region_df = df.copy()
                    else:
                        col_merge_on = 'date'
                        try:
                            merge_region_df = merge_region_df.combine(df, func=avg, overwrite=False)
                        except Exception as e:
                            print(f"Something went wrong trying to combine {file} with the merged df")
                            print(e)
            if merge_region_df is not None:
                merge_region_df = rescale_comparison(merge_region_df)
                new_name = unnecessary_files[-1].replace(rg, rg_short)
                merge_region_df.to_csv(new_name)
                new_files.append(new_name)
    return new_files, unnecessary_files


def merge_for_scraper(directory: Folderpath, country_shortcode: Country_Shortcode = 'FR'):
    """
    Conducts all merges for the scraper. Takes an directory and cycles through everything
    Args:
        directory:
        country_shortcode:

    Returns:

    """
    cc_merges = region_merges.get(country_shortcode.upper(), False)
    if cc_merges:
        print("Merging region-files")
        cc_merges_region_codes = translate_dict(translate_dict(cc_merges, reverseDict(regions_map_english_to_local)),
                           eng_code)  # translates from French to English to Code
        new_files, unnecessary_files = do_merges_Time(directory, cc_merges_region_codes)
        print(f"Created new files: {new_files}\nThese files are not necessary anymore: {unnecessary_files}")
        geoFiles: List[str] = list(
            filter(lambda x: country_shortcode.lower() in x.lower() and "geo" in x.lower(), os.listdir(directory)))
        for geoFile in geoFiles:
            try:
                path = os.path.join(directory, geoFile)
                if 'Aggregated' in directory:
                    usecols = [1, 2]
                    merge_col = 'means'
                    add_geo_code = False
                    keep_index = True
                elif 'out' in directory:
                    usecols = [0, 2]  # drops geoCode pos 1 -> add
                    merge_col = -1
                    add_geo_code = True
                    keep_index = False
                elif 'comparisons' in directory:
                    usecols = None
                    merge_col = None
                    add_geo_code = True
                    keep_index = False
                else:
                    usecols = [0, 2]  # drops geoCode pos 1 -> add
                    merge_col = -1
                    add_geo_code = True
                    keep_index = False
                df = pd.read_csv(path, usecols=usecols)
                cc_merges_eng_region_names = translate_dict(cc_merges, reverseDict(regions_map_english_to_local))
                new = doMerges_Geo(df, cc_merges_eng_region_names, merge_col, add_geo_code=add_geo_code)
                new = new.fillna(0)
                new.to_csv(path, index=keep_index)
                print(f"Updated file: file://{path}")
            except Exception as e:
                print(e)


def avg(s1: pd.Series, s2: pd.Series) -> pd.Series:
    val = (s1 + s2) / 2
    print(s1, s2)
    return val


if __name__ == '__main__':
    country = getCountry()
    choice = choose_from_dict({i: k for i, k in enumerate(['Comparisons', 'Aggregated', 'Out'])}, label="Folders",
                              request_description="In which of the following Folders do you want to merge regions?")
    directories = FS.Comparisons
    if choice == 'Comparisons':
        directories = []
        for item in os.listdir(FS.Comparisons):
            full_path = os.path.join(FS.Comparisons, item)
            if os.path.isdir(full_path) and not item.startswith("."):
                directories.append(full_path)
        directories = list(map(lambda x: os.path.join(FS.Comparisons, x), choose_multiple_from_dict(list(map(lambda x: os.path.split(x)[1], directories)), 'Groups', request_description='Which of these groups do you want to merge the regions in?')))
    elif choice == 'Aggregated':
        directories = [os.path.join(FS.Aggregated, country.Full_name)]
    elif choice == 'Out':
        directories = [os.path.join(FS.Kwd_Level_Outs, x) for x in os.listdir(FS.Kwd_Level_Outs) if os.path.isdir(os.path.join(FS.Kwd_Level_Outs, x)) and not x.startswith(".")]
    for directory in directories:
        print(f"Working on directory: {directory}")
        merge_for_scraper(directory, country.Shortcode)
