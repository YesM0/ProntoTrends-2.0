import os
import sys

if __name__ == '__main__':
    sys.path.extend(['../', './'])

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Union, Callable
from utils.custom_types import *
from utils.Countries import readInLocales, regions_map_english_to_local
from utils.misc_utils import deduplicateColumns, lcol, \
    reverseDict, getRegions
from utils.user_interaction_utils import getChosenCountry, binaryResponse, choose_from_dict, \
    choose_multiple_from_dict, defineList, int_input
from utils.Filesys import generic_FileServer

short_codes = {
    "Germany": "DE",
    "France": 'FR',
    'Spain': 'ES',
    "Italy": 'IT',
    'Switzerland': 'CH',
    'Austria': 'AT'
}

FS = generic_FileServer

# Used for adapting value names
col_remap = {
    'Spend_type': {
        'hochzeit günstig': 'standard',
        'hochzeit premium': 'premium'
    },
    'ticket_geo_region_name': regions_map_english_to_local,
    'Wed_type': {
        'standesamtliche Hochzeit': 'standesamtlich',
        "Heiraten kirchlich": 'kirchlich'
    },
    'Loc_type': {
        'hochzeit hotel': 'Hotel/ Restaurant',
        'hochzeit villa': 'Villa',
        'hochzeit schloss': 'Schloss',
        'hochzeit landhaus': 'Landhaus'
    },
    'Prof_type': {
        'Hochzeitsdeko': 'Dekorateur für Hochzeiten',
        'Hochzeitsfotos': 'Hochzeitsfotograf',
        'Hochzeit Tanzkurs': 'Tanzkurs',
        'Musik Hochzeit': 'Musiker für Hochzeit'
    },
    'ticket_taxonomy_tag_name': {
        "ORIGINAL": "REPLACEMENT"
    },
    'sub_type': {
        'Hochzeit günstig': 'Günstige Hochzeit',
        'Hochzeit Kosten': 'Normale Hochzeit',
        'Musik Hochzeit': 'Musiker für Hochzeit',
        'Hochzeit Buffet': 'Buffet',
        'Hochzeit Essen': 'Catering',
        'Hochzeit Bar': 'Bar',
        'hochzeit hotel': 'Hotel',
        'hochzeit schloss': 'Schloss',
        'hochzeit villa': 'Villa'
    }
}


def read_csv_utility(filepath: Filepath, country: Country_Fullname = 'Spain', **kwargs) -> pd.DataFrame:
    """
    Utility to cycle through possible encodings until successful
    Args:
        filepath:
        country:
        **kwargs:

    Returns:

    """
    encodings = ['utf-8']
    if country == 'Spain':
        encodings = encodings + ['cp1252', 'latin_1']
    for ecd in encodings:
        try:
            df = pd.read_csv(filepath, encoding=ecd, **kwargs)
            return df
        except FileNotFoundError as e:
            print(e)
            raise FileNotFoundError(e)
        except Exception as e:
            print(e)
            print(f"Trying a different encoding")
    raise Exception(f"Could not parse file: {filepath}")


def createCategoryRegionYearFile(country, category, options_column_label='Type'):
    comparisons_path, regions = getSetUp(country)
    initial_df = read_csv_utility(
        os.path.join(comparisons_path, category, f"Time_{country}_{short_codes[country]}_{category}.csv"), country)
    initial_df['date'] = pd.to_datetime(initial_df['date'])
    categories = [col for col in initial_df.columns if 'date' not in col and 'isPartial' not in col]
    years = list(set([val.year for val in initial_df['date'].array]))
    out = []
    for i, r in enumerate(regions):
        region_name = r['name']
        region_id = f"{short_codes[country]}-{r['id']}" if i > 0 else r['id']
        file = os.path.join(comparisons_path, category, f"Time_{country}_{region_id}_{category}.csv")
        if os.path.exists(file):
            print(f"Reading files {os.path.split(file)[1].split('_')[-2:]}", end=' ') if i == 0 else print(
                f"{os.path.split(file)[1].split('_')[-2:]}", end=" ")
            if i == len(regions) - 1:
                print("\n")
            df = read_csv_utility(file, country=country)
            # print(df)
            df = deduplicateColumns(df, extra='isPartial')
            df['date'] = pd.to_datetime(df['date'])
            df = df.resample('M', on='date').mean()
            """
                GROUPING
            """
            grouped = df.groupby(df.index.year)
            for year, group in grouped:
                mean = group.mean(0)
                summe = mean.sum()
                mean = mean.apply(lambda x: x / summe)
                # print(year)
                # print(mean)
                diction = mean.to_dict()
                # print(diction)
                for key in diction:
                    out.append([year, region_name, key, diction[key]])
        else:
            # create empty rows
            for year in years:
                for cat in categories:
                    out.append([year, region_name, cat, 0])
    final_df = pd.DataFrame(out, columns=['Year', 'ticket_geo_region_name', options_column_label, 'Distribution'])
    # print(final_df)
    final_df = final_df.sort_values(by=['Year', 'ticket_geo_region_name'], ascending=True).round(2).fillna('NA')
    return final_df


def Sort(sub_li, reverse=False):
    sub_li.sort(key=lambda x: x[1], reverse=reverse)
    return sub_li


def combineColumns(df: pd.DataFrame, combination: list, new_name: str):
    curr_comb = []
    to_drop = []
    while len(combination) > 0:
        while len(curr_comb) < 2 and len(combination) > 0:
            item = combination.pop()
            if item in df.columns:
                curr_comb.append(item)
                if item != new_name:
                    to_drop.append(item)
        if len(curr_comb) > 0:
            df[new_name] = df[curr_comb].mean(1)
        curr_comb = [new_name]
    if len(to_drop) > 0:
        df = df.drop(to_drop, axis=1)
    return df


def createTop5Csv(country: Country_Fullname, category: str, category_combinations: list = None,
                  month_name_dict: dict = None, allow_user_interaction: bool = True):
    comparisons_path, regions = getSetUp(country)
    # select only cc level
    regions = [regions[0]]
    initial_df = read_csv_utility(
        os.path.join(comparisons_path, category, f"Time_{country}_{short_codes[country]}_{category}.csv"), country)
    if category_combinations and isinstance(category_combinations, dict):
        for combination in category_combinations:
            initial_df = combineColumns(initial_df, category_combinations[combination], combination)
    initial_df['date'] = pd.to_datetime(initial_df['date'])
    possibilities = [col for col in initial_df.columns if 'date' not in col and 'isPartial' not in col]
    years = list(set([val.year for val in initial_df['date'].array]))
    out = []
    for i, r in enumerate(regions):
        region_name = r['name']
        region_id = f"{short_codes[country]}-{r['id']}" if i > 0 else r['id']
        file = os.path.join(comparisons_path, category, f"Time_{country}_{region_id}_{category}.csv")
        if os.path.exists(file):
            df = read_csv_utility(file, country)
            if category_combinations and isinstance(category_combinations, dict):
                for combination in category_combinations:
                    df = combineColumns(df, category_combinations[combination], combination)
            # print(df)
            df = deduplicateColumns(df, extra='isPartial')
            df['date'] = pd.to_datetime(df['date'])
            df = df.resample('M', on='date').mean()
            """
                GROUPING
            """
            grouped = df.groupby(df.index.year)
            for year, group in grouped:
                year_highs = []
                year_entries = {}
                for possibility in possibilities:
                    if possibility in group.columns:
                        max_id = group[possibility].idxmax()
                        min_id = group[possibility].idxmin()
                        max_val = group.loc[max_id, possibility]
                        min_val = group.loc[min_id, possibility]
                        seasonality = round(max_val / min_val, 2) if min_val > 0 else round(max_val / 1, 2)
                        max = max_id.month - 1 if not month_name_dict else month_name_dict[max]
                        min = min_id.month - 1 if not month_name_dict else month_name_dict[max]
                        year_highs.append((possibility, max_val))
                    else:
                        max = None
                        min = None
                        seasonality = None
                        year_highs.append((possibility, 0))
                    year_entries[possibility] = [region_name, possibility, year, 0, max, min, seasonality]
                print(
                    f"{lcol.WARNING}Here are the summaries we found. First the list of possibilities for the current year {year} with their respective highest value")
                print(year_highs)
                print(f"These would be the top5 entries for each option")
                print(year_entries)
                print(lcol.ENDC)
                for i, item in enumerate(Sort(year_highs, True)):
                    if i < 5:
                        poss = item[0]
                        entry = year_entries[poss]
                        entry[3] = i + 1
                        out.append(entry)
        else:
            # create empty rows
            for year in years:
                for i, possibility in enumerate(possibilities):
                    if i < 5:
                        out.append([region_name, possibility, year, None, None, None, None])
    final_df = pd.DataFrame(out,
                            columns=["ticket_geo_region_name", "ticket_taxonomy_tag_name", "year", "Rank", "Max", "Min",
                                     'Demand_factor_max_to_min'])
    # print(final_df)
    final_df = final_df.sort_values(by=['year', 'ticket_geo_region_name', 'Rank'], ascending=True).round(1).fillna('NA')
    if allow_user_interaction:
        final_df = adjust_Top5_Data(final_df, country)
    return final_df


def adjust_Top5_Data(final_df: pd.DataFrame, country: Country_Fullname):
    print(f"Working on making the Top5 Data even more precise")
    cache = {}
    fixed = 0
    for ind, row in final_df.iterrows():
        if row['Demand_factor_max_to_min'] <= 2:
            try:
                tag_name: str = cache.get(row['ticket_taxonomy_tag_name'], row['ticket_taxonomy_tag_name'])
                original_tag_name = tag_name
                country_shortcode = short_codes.get(country, country[:2])
                folder: Filepath = os.path.join(FS.Aggregated, country)
                files_in_folder: list = os.listdir(folder)
                files_in_folder: list = list(filter(lambda f: len(f.split("_")) > 2, files_in_folder))
                num_try = 0
                do_skip = False
                while True:
                    files: list = list(filter(
                        lambda x: tag_name in x and country_shortcode in x and '-' not in x[
                                                                                          :4] and 'geo' not in x.lower(),
                        files_in_folder))
                    num_try += 1
                    if len(files) > 0:
                        filepath = os.path.join(folder, files[0])
                        cache[original_tag_name] = tag_name
                        break
                    else:
                        if num_try > 1:
                            if binaryResponse('Do you want to skip this item?'):
                                do_skip = True
                                break
                        all_tags = list(set(map(lambda x: x.split("_")[2], files_in_folder)))
                        tags_available = {i: tag for i, tag in enumerate(all_tags)}
                        tags_available["Skip"] = "Skip"
                        tag_name = choose_from_dict(dictionary=tags_available, label='Tags',
                                                    request_description=f"Which of these tags is the same as {lcol.OKGREEN}{original_tag_name}{lcol.ENDC}?")
                        if tag_name == 'Skip':
                            do_skip = True
                            break
                if do_skip:
                    continue
                else:
                    df = read_csv_utility(filepath, country, usecols=[1, 2])
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.resample("M", on="date").mean()
                    grouped = df.groupby(df.index.year)
                    for year, group in grouped:
                        if year == row['year']:
                            max_id = group['means'].idxmax()
                            min_id = group['means'].idxmin()
                            max_val = group.loc[max_id, 'means']
                            min_val = group.loc[min_id, 'means']
                            seasonality = round(max_val / min_val, 2) if min_val > 0 else round(max_val / 1, 2)
                            maxi = max_id.month - 1
                            mini = min_id.month - 1
                            final_df.loc[ind, 'Max'] = maxi
                            final_df.loc[ind, 'Min'] = mini
                            final_df.loc[ind, 'Demand_factor_max_to_min'] = seasonality
                            fixed += 1
            except Exception as e:
                print(
                    f"{lcol.WARNING}There occurred an error in adjust_Top5_Data. This may not be important as this is an optional function.\n{'-' * 6}Error Message{'-' * 6}\n{e}{lcol.ENDC}")
    print(f"Fixed {fixed} entries by looking up the Tag-Level data")
    return final_df


def createMainSectionCsv(country: Country_Fullname, files: List[str], campaign_shortname: str = 'Wed') -> pd.DataFrame:
    final_folder: Folderpath = os.path.join(FS.Final, country, campaign_shortname)
    # comparisons_path, regions = getSetUp(country)
    out = []
    for file in files:
        filepath: Filepath = os.path.join(final_folder, file)  # file is only the filename
        df = read_csv_utility(filepath, country)
        typ = file.split("-")[1].split("_")[0]
        try:
            df['Distribution'] = pd.to_numeric(df['Distribution'], errors='coerce')
        except KeyError as e:
            print(e)
            print(
                f"{lcol.OKGREEN}Probably, In the file file://{file} no header called 'Distribution' exists. Please check!\nThe category will be skipped now.{lcol.ENDC}")
            continue
        y = 'Year' if 'Year' in df.columns else 'year'
        # years = df[y].unique().tolist()
        t = [item for item in df.columns if 'type' in item.lower()][0]
        # categories = df[t].unique().tolist()
        g = [item for item in df.columns if '_geo' in item.lower()][0]
        grouped = df.groupby([y, g])
        for name, group in grouped:
            year, region = name
            # print(grouped)
            mx = group['Distribution'].max(skipna=True)
            mxidx = group['Distribution'].idxmax()
            maxVal = mx if mx > 0 else np.NaN
            maxType = group.loc[mxidx, t] if pd.notna(maxVal) else np.NaN
            out.append([year, region, typ, maxType, maxVal])
    final_df = pd.DataFrame(out,
                            columns=['Year', "ticket_geo_region_name", 'Type', 'sub_type', 'Percentage'])
    print(final_df)
    final_df = final_df.sort_values(by=['Year', 'ticket_geo_region_name'], ascending=True).round(2).fillna('NA')
    return final_df


def getSetUp(country: Country_Fullname) -> Tuple[Filepath, List[Dict[str, Union[Country_Fullname, str]]]]:
    regions = [{'name': country, 'id': short_codes[country]}] + getRegions(country, readInLocales())
    comparisons_path = FS.Comparisons
    return comparisons_path, regions


def getMonths(filepath):
    df = read_csv_utility(filepath)
    df['date'] = pd.to_datetime(df['date'])
    df = df.resample('M', on='date').mean()
    return [(i.month - 1, i.year) for i, row in df.iterrows()]


def createTagChartData(country: Country_Fullname, min_regions: int = 0, select_tags: bool = False,
                       selected_tags: List[str] = None, min_year: int = 2019):
    """
    Creates data for requests-trend.csv / Chart_Data.csv. A yearly
    Args:
        selected_tags:
        country:
        min_regions:
        select_tags:

    Returns:

    """
    comparisons_path, regions = getSetUp(country)
    region_ids = {r['id']: r['name'] for r in regions}

    final_df = gather_base_data_chart(country, min_regions, region_ids, regions, select_tags, selected_tags)
    final_df = final_df[final_df.Year >= min_year]

    final_df = scale_within_ticket_cc_selected(final_df)
    other = final_df.copy(deep=True)
    other = other[other.Country_chosen == 0]
    other = calculate_cc_chosen_regions(final_df, other, country=country, scale_by='sum-regions-normalized-to-country')
    final_df = pd.concat([final_df, other], ignore_index=True)
    # final_df = final_df.sort_values(by=['Country_chosen', 'ticket_taxonomy_tag_name']).round(2)
    # print(final_df)
    # final_check = final_df.groupby(['ticket_taxonomy_tag_name', 'Country_chosen'])['Index'].max()
    # print(final_df)
    # compare_vals = final_df.groupby(["ticket_taxonomy_tag_name", 'Country_chosen','ticket_geo_region_name','Year','Month']).max()
    final_df = final_df.fillna('NA')
    return final_df


def calculate_cc_chosen_regions(final_df: pd.DataFrame, other: pd.DataFrame, country: Country_Fullname,
                                scale_by: str = 'sum-regions') -> pd.DataFrame:
    """
    Re-Calculates the index for region data when CC chosen
    Args:
        country:
        final_df: pd.DataFrame -- the original DataFrame to use as reference
        other: pd.DataFrame -- to be adjusted DataFrame
        scale_by: str -- "sum-regions" or "country-value" -> chooses which value to scale data by

    Returns: DataFrame of regions when country_chosen == 1

    """
    scalars = {}
    if scale_by == 'sum-regions':
        final_grouped = final_df.query('ticket_geo_region_name != @country').groupby(
            ['ticket_taxonomy_tag_name', 'Year', 'Month'])
        for name, group in final_grouped:
            summe = group['Index'].sum()
            num = group['Index'].count()
            identity = f"{name[0]}|{name[1]}|{name[2]}"
            scalars[identity] = summe
    elif scale_by == 'country-value':
        grouped = final_df.query('ticket_geo_region_name == @country').groupby(
            ['ticket_taxonomy_tag_name', 'Year', 'Month'])
        for name, group in grouped:
            identity = f"{name[0]}|{name[1]}|{name[2]}"
            if group.shape[0] > 1:
                print(f"{lcol.WARNING}There seem to be more than 1 row for {identity}{lcol.ENDC}")
            summe = group['Index'].sum()
            num = group['Index'].count()
            scalars[identity] = summe
    elif scale_by == 'sum-regions-normalized-to-country':
        final_grouped = final_df.query('ticket_geo_region_name != @country').groupby(
            ['ticket_taxonomy_tag_name', 'Year', 'Month'])
        for name, group in final_grouped:
            summe = group['Index'].sum()
            num = group['Index'].count()
            identity = f"{name[0]}|{name[1]}|{name[2]}"
            scalars[identity] = summe
        grouped = final_df.query('ticket_geo_region_name == @country').groupby(
            ['ticket_taxonomy_tag_name', 'Year', 'Month'])
        for name, group in grouped:
            identity = f"{name[0]}|{name[1]}|{name[2]}"
            if group.shape[0] > 1:
                print(f"{lcol.WARNING}There seem to be more than 1 row for {identity}{lcol.ENDC}")
            summe = group['Index'].sum()
            num = group['Index'].count()
            scalars[identity] = scalars.get(identity, 1) / summe
    else:
        raise ValueError(f"The mode you selected to calculate cc_chosen regions is invalid: {scale_by}")

    def scaleRow(row, scalars: dict):
        identifier = f"{row['ticket_taxonomy_tag_name']}|{row['Year']}|{row['Month']}"
        scalar = scalars.get(identifier, False)
        if scalar is not False:
            row['Index'] = (row['Index'] / scalar) if pd.notna(row['Index']) else "NA"
        row['Country_chosen'] = 1
        return row

    other = other.apply(scaleRow, axis=1, args=(scalars,))
    return other


def scale_within_ticket_cc_selected(final_df: pd.DataFrame) -> pd.DataFrame:
    # scale so that we have 1 for at least one value in a tag -> Find overall maximum value in the tag (across regions) and then scale this to 1 -> apply same scalar to all other values
    g = final_df.groupby(['ticket_taxonomy_tag_name', 'Country_chosen'])
    tag_maxes = g['Index'].max()

    def scale(row, tag_maxes):
        max = tag_maxes.loc[(row['ticket_taxonomy_tag_name'], row['Country_chosen'])]
        row['Index'] = row['Index'] / max
        return row

    final_df = final_df.apply(scale, args=(tag_maxes,), axis=1)
    return final_df


def gather_base_data_chart(country: Country_Fullname, min_regions: int, region_ids_to_name: dict, regions: list,
                           select_tags: bool, selected_tags: List[str] = None) -> pd.DataFrame:
    """
    Access all tag-Time-files in Aggregated, resamples to Months and puts them into one df
    Args:
        selected_tags:
        country: Country_Fullname
        min_regions: int -- minimum number of regions needed for a tag to be considered -> passed into filter_tags
        region_ids_to_name: dict -- dict of region_id to region_name (ENG)
        regions: List[str] -- list of region ids (only respective id)
        select_tags: bool -- whether the user wants to select the tags themselves

    Returns: DataFrame with columns: ['ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month', 'Index', 'Country_chosen']

    """
    folder = os.path.join(FS.Aggregated, country)
    all_files = os.listdir(folder)
    all_files = list(filter(lambda x: len(x.split("_")) > 2, all_files))
    hasSufficientFiles: bool = check_sufficient_files(all_files)
    print(f"In the folder {folder} there are {len(all_files)} files.")
    if not hasSufficientFiles:
        print(
            f"{lcol.FAIL}There are too few adjusted files. Please make sure you have run the adjust files function in file://{os.path.join(FS.cwd, 'Datapipeline', 'generateSummaries.py')}{lcol.ENDC}")
        if not binaryResponse("Is this on purpose?"):
            sys.exit()
    tags: Dict[str, int] = filter_tags(all_files, min_regions, select_tags, selected_tags)
    months = getMonths(os.path.join(folder, list(filter(lambda x: ('Time' in x and 'Adjusted' in x), all_files))[0]))
    columns = ['ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month', 'Index',
               'Country_chosen']
    out = []
    for tag, tag_id in tags.items():
        for ind, region_id in enumerate(list(region_ids_to_name.keys())):
            region_code = f"{regions[0]['id']}-{region_id}" if ind > 0 else region_id  # regions[0] => Country
            adjusted = '_Adjusted' if len(region_code) > 2 else ''
            eligible_files = [f for f in all_files if
                              f"{region_code}_" in f and tag in f and str(
                                  tag_id) in f and 'Time' in f and adjusted in f]
            file = os.path.join(folder, eligible_files[0]) if len(eligible_files) > 0 else False
            if file:
                df = read_csv_utility(os.path.join(folder, file), country)
                df['date'] = pd.to_datetime(df['date'])
                df = df.resample('M', on='date').mean()
                for i, row in df.iterrows():
                    year = i.year
                    month = i.month - 1
                    val = row['means']
                    country_chosen = 0 if len(
                        region_code) > 2 else 1  # if region code like "CC-AA" -> region else if "CC" -> country
                    out.append([tag, region_ids_to_name[region_id], year, month, val / 100, country_chosen])
            else:
                for month, year in months:
                    out.append([tag, region_ids_to_name[region_id], year, month, None, 0])
    final_df = pd.DataFrame(out, columns=columns)
    return final_df


def check_sufficient_files(list_of_files: List[str]) -> bool:
    num_adjusted = len([file for file in list_of_files if 'Adjusted' in file])
    if num_adjusted < 5:
        return False
    else:
        return True


def filter_tags(all_files: List[str], min_regions: int, select_tags: bool, selected_tags: List[str] = None) -> Dict[
    str, int]:
    tags = {file.split('_')[2]: int(file.split('_')[1]) for file in all_files}
    tag_names = [file.split("_")[2] for file in all_files if 'Adjusted' in file]
    tag_counts = {tag: tag_names.count(tag) for tag in tags}
    if min_regions > 0 or select_tags:
        to_drop = []
        if min_regions > 0:
            to_drop = [tag for tag in tags if tag_counts[tag] < min_regions] if min_regions > 0 else []
        if selected_tags is not None:
            to_drop.extend([t for t in tag_names if t not in selected_tags])
        if select_tags:
            if binaryResponse(
                    "Do you want to input a list of allowed tags (by Id) (y) or do you want to select individualy (n)?"):
                choices = choose_multiple_from_dict([tag for tag in list(tags.keys()) if tag in to_drop], 'Tags')
                rev = reverseDict(tags)
                tag_names_to_keep = choices
                to_drop = to_drop.extend(list(set([tname for tname in tag_names if tname not in tag_names_to_keep])))
            else:
                for tag_name, tag_id in tags.items():
                    if tag_name not in to_drop:
                        if binaryResponse(f"Do you want to {lcol.UNDERLINE}drop{lcol.ENDC} the tag '{tag_name}'?"):
                            to_drop.append(tag_name)
            print(f"Gonna drop {len(to_drop)} tags out of {len(tags)}")
        for tag in to_drop:
            tags.pop(tag)
            print(
                f"Dropped {lcol.OKGREEN}'{tag}'{lcol.ENDC} from Chart because we only had {lcol.OKGREEN}{tag_counts[tag]}{lcol.ENDC} regions for it")
    return tags


def createTableData(country: Country_Fullname, campaign_short_code: str) -> pd.DataFrame:
    ccs = {
        "Germany": 'Deutschland',
        "France": "France",
        "Italy": 'Italia',
        "Spain": 'España',
        "Austria": 'Österreich',
        "Switzerland": 'Schweiz'
    }
    # ['ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month', 'Index', 'Country_chosen']
    filepath = os.path.join(FS.Final, country, campaign_short_code,
                            f'{campaign_short_code}_Chart_Data_{country}.csv')
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"The file {filepath} could not be found. Ensure that it exists.")
    else:
        df = read_csv_utility(filepath, country)
        translate = ccs.get(country, country)
        df = df.query('Country_chosen == 0 | ticket_geo_region_name == @translate | ticket_geo_region_name == @country')
        grouped = df.groupby(['ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year'])
        year_tag_region_sums = grouped['Index'].sum()

        def scale(row, year_tag_region_sums):
            summe = year_tag_region_sums[(row.ticket_taxonomy_tag_name, row.ticket_geo_region_name, row.Year)]
            row['Index'] = row['Index'] / summe
            return row

        df = df.apply(scale, args=(year_tag_region_sums,), axis=1)
        df = df.rename(columns={'Index': 'Distribution_of_tickets'})
        return df


def createMapData(country: Country_Fullname, campaign_short_code: str, useChart: bool = True) -> pd.DataFrame:
    if useChart:
        # find Chart Data
        filepath = os.path.join(FS.Final, country, campaign_short_code,
                                f'{campaign_short_code}_Chart_Data_{country}.csv')
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"The file {filepath} could not be found. Ensure that it exists.")
        # group by tag, year & region
        df = read_csv_utility(filepath, country)
        ccs = {
            "Germany": 'Deutschland',
            "France": "France",
            "Italy": 'Italia',
            "Spain": 'España',
            "Austria": 'Österreich',
            "Switzerland": 'Schweiz'
        }
        translate = ccs.get(country, country)
        df = df.query(
            '(Country_chosen == 0 | ticket_geo_region_name == @country | ticket_geo_region_name == @translate) & Year == 2019').drop(
            columns=[col for col in ['Month', 'No_of_tickets', 'Country_chosen', 'Year'] if col in df.columns])
        sum_ticket_region = df.groupby(['ticket_taxonomy_tag_name', "ticket_geo_region_name"]).sum()
        # correct score
        sum_ticket_region = sum_ticket_region.reset_index()
        max_region = sum_ticket_region.groupby('ticket_taxonomy_tag_name')['Index'].max()

        def scl(row, maxes):
            mx = maxes.loc[row.ticket_taxonomy_tag_name]
            row['Index'] = (row['Index'] / mx) * 10
            return row

        sum_ticket_region = sum_ticket_region.apply(scl, args=(max_region,), axis=1)
        sum_ticket_region = sum_ticket_region.rename(columns={'Index': 'Score'})
        return sum_ticket_region
    else:
        folder = os.path.join(FS.Aggregated, country)
        files = os.listdir(folder)
        geo_files = filter(lambda f: 'Geo' in f, files)
        overall = False
        for file in geo_files:
            df = read_csv_utility(os.path.join(folder, file), country, usecols=[1, 2])
            if not df.empty:
                df['ticket_taxonomy_tag_name'] = file.split("_")[2]
                df['means'] = df['means'].apply(lambda x: x / 10)
                if overall is False:
                    overall = df.copy()
                else:
                    overall = pd.concat([overall, df])
        overall = overall.rename(columns={'geoName': 'ticket_geo_region_name', 'means': 'Score'})
        order = ['ticket_taxonomy_tag_name', "ticket_geo_region_name", 'Score']
        overall = overall[order]
        overall = overall.dropna(axis=0, how="any")
        return overall


def remapColumns(df: pd.DataFrame, column_remappings: dict) -> pd.DataFrame:
    for col_name in column_remappings:
        obj = column_remappings[col_name]
        if col_name in df.columns:
            try:
                df[col_name] = df[col_name].apply(lambda x: obj.get(x, x))
            except Exception as e:
                print(e)
    return df


def get_category_overview_settings() -> Tuple[List[str], List[str]]:
    print(
        f"{lcol.OKGREEN}Please define the categories for which to create overviews. The names need to match folders in the 'comparisons' folder{lcol.ENDC}")
    overview_cats = defineList(
        ['Spend', 'Reception Location', 'Tags', 'Services', 'Food', 'Wedding Style'])
    if overview_cats == ['Spend', 'Reception Location', 'Tags', 'Services', 'Food', 'Wedding Style']:
        overview_cat_cols = ['Spend_type', 'Loc_type', 'Prof_type', 'Prof_type', 'Food_type', 'Style_type']
    else:
        print(
            f"{lcol.OKGREEN}Please define the corresponding category column names which will be used in the final CSV for the following categories:\n{overview_cats}\nThey usually contain '_type' and need to be as many as there are categories.{lcol.ENDC}")
        while True:
            overview_cat_cols = defineList()
            if len(overview_cat_cols) == len(overview_cats):
                break
            else:
                print(f"The count of column names ({len(overview_cat_cols)}) don't match the amount of categories ({len(overview_cats)}). Please try again.")
    return overview_cats, overview_cat_cols


def get_user_settings() -> Tuple[
    Country_Shortcode, Country_Fullname, str, Union[List[str], None], int, Union[None, List[str]], Union[
        None, List[str]], bool, Union[None, List[str]], bool]:
    campaign_short_code = 'Wed'
    min_region_count = 0
    overview_cat_cols = None
    overview_cats = None
    select_tags_manually = False
    top_5_cats = None
    useChartData = None
    s, country = getChosenCountry(action='create files')
    if binaryResponse(f"Do you have a different campaign short code than '{campaign_short_code}'?"):
        campaign_short_code = input("What is the short code of the current campaign?\n").strip()
    chosenActions = choose_multiple_from_dict(
        {1: 'Create Category Overviews', 2: 'Create Top5', 3: 'create Main Section', 4: 'create Chart Data',
         5: 'create Table Data', 6: 'create Map Data', 7: 'create Chart Data (for custom operations)'}, label='actions')
    if 'create Chart Data' in chosenActions or 'create Chart Data (for custom operations)' in chosenActions:
        if binaryResponse("Do you want to set a minimum region count for the Chart?"):
            min_region_count = int_input("What is the limit you choose?\n")
        select_tags_manually = binaryResponse("Do you want to manually select the tags to be included?")
    if 'create Map Data' in chosenActions:
        useChartData = binaryResponse('Do you want to calculate the scores based on Chart Data?')
    if 'Create Category Overviews' in chosenActions:
        overview_cats, overview_cat_cols = get_category_overview_settings()
    if 'Create Top5' in chosenActions:
        print(
            f"{lcol.OKGREEN}Please define the categories to source data for the Top 5 for. The names need to match folders in the 'comparisons' folder{lcol.ENDC}")
        top_5_cats = defineList(['Tags', 'Services'])
    return s, country, campaign_short_code, chosenActions, min_region_count, overview_cat_cols, overview_cats, select_tags_manually, top_5_cats, useChartData


def dialog():
    # global country, campaign_short_code
    s, country, campaign_short_code, chosenActions, min_region_count, overview_cat_cols, overview_cats, select_tags_manually, top_5_cats, useChartData = get_user_settings()
    final_folder = os.path.join(FS.Final, country, campaign_short_code)
    if not os.path.exists(final_folder):
        os.makedirs(final_folder)
    for chosenAction in chosenActions:
        print(f"DOING: {chosenAction}")
        if chosenAction == 'Create Category Overviews':
            all_cats = binaryResponse("Do you want to create summaries for all categories?")
            for category, category_col_name in zip(overview_cats, overview_cat_cols):
                do_cat = binaryResponse(f"Do you want to create {category}?") if not all_cats else True
                if do_cat:
                    try:
                        result = createCategoryRegionYearFile(country, category, category_col_name)
                        result = remapColumns(result, col_remap)
                        fname = os.path.join(final_folder, f"{campaign_short_code}-{category}_{country}.csv")
                        result.to_csv(
                            fname,
                            index=False)
                        print(f'Saved: "file://{fname}"')
                        if country == 'Italy':
                            with pd.ExcelWriter(
                                    os.path.join(final_folder, f"{campaign_short_code}-{category}_{country}.xlsx"),
                                    mode='w') as excel:
                                result.to_excel(excel)
                            print(f'Saved {excel}')
                    except Exception as e:
                        print(e)
        elif chosenAction == 'Create Top5':
            for category in top_5_cats:
                try:
                    category_combinations = None
                    if country == 'France':
                        category_combinations = {
                            "Wedding planner": ["Wedding planner", "wedding planner"],
                            "Maquillage": ["Maquillage", "Maquillage pour mariage"]
                        }
                    elif country == 'Spain':
                        category_combinations = {
                            'Maquillaje': ["Maquillaje", "Maquillaje"]
                        }
                    result = createTop5Csv(country, category, category_combinations=category_combinations)
                    result = remapColumns(result, col_remap)
                    fname = os.path.join(final_folder, f"{campaign_short_code}-Top5_Tags_{category}_{country}.csv")
                    result.to_csv(
                        fname,
                        index=False)
                    print(f'Saved: "file://{fname}"')
                    if country == 'Italy':
                        with pd.ExcelWriter(os.path.join(final_folder,
                                                         f"{campaign_short_code}-Top5_Tags_{category}_{country}.xlsx"),
                                            mode='w') as excel:
                            result.to_excel(excel)
                        print(f'Saved {excel}')
                except FileNotFoundError as e:
                    print(e)
        elif chosenAction == 'create Main Section':
            chosen_files: List[str] = choose_multiple_from_dict(
                [x for x in os.listdir(os.path.join(FS.Final, country, campaign_short_code)) if not x.startswith(".")],
                request_description='Which of these categories do you want to include in the Main.csv?', label='Files')
            # choices = [x.split("-")[1].split("_")[0] for x in choices]
            result = createMainSectionCsv(country,
                                          chosen_files, campaign_shortname=campaign_short_code)
            result = remapColumns(result, col_remap)
            fname = os.path.join(final_folder, f'{campaign_short_code}_Main_Section_{country}.csv')
            result.to_csv(fname, index=False)
            print(f'Saved: "file://{fname}"')
        elif chosenAction == 'create Chart Data':
            result = createTagChartData(country, min_regions=min_region_count, select_tags=select_tags_manually)
            result = remapColumns(result, col_remap)
            fname = os.path.join(final_folder, f'{campaign_short_code}_Chart_Data_{country}.csv')
            result.to_csv(fname, index=False)
            print(f'Saved: "file://{fname}"')
        elif chosenAction == 'create Table Data':
            result = createTableData(country, campaign_short_code)
            result = result.fillna('NA')
            fname = os.path.join(final_folder, f'{campaign_short_code}_Table_Data_{country}.csv')
            result.to_csv(fname, index=False)
            print(f'Saved: "file://{fname}"')
        elif chosenAction == 'create Map Data':
            result = createMapData(country, campaign_short_code, useChart=useChartData)
            result = result.fillna('NA')
            fname = os.path.join(final_folder, f'{campaign_short_code}_Map_Data_{country}.csv')
            result.to_csv(fname, index=False)
            print(f'Saved: "file://{fname}"')
        elif chosenAction == 'create Chart Data (for custom operations)':
            min_year = int_input("What is the base year you want to get the data for?")
            result = createTagChartData(country, min_regions=min_region_count, select_tags=select_tags_manually, min_year=min_year)
            result = remapColumns(result, col_remap)
            fname = os.path.join(final_folder, f'{campaign_short_code}_Chart_Data_{country}.csv')
            result.to_csv(fname, index=False)
            print(f'Saved: "file://{fname}"')
        print(f'FINISHED {chosenAction}')
    print('FINISHED ALL')


def api_start(settings: Dict, logging_func: Callable):
    print(settings)
    # min_region_count, overview_cat_cols, overview_cats, select_tags_manually, top_5_cats, useChartData
    country = settings.get("country_full_name", None)
    campaign_short_code = settings.get("campaign_shortcode", None)
    chosenActions = settings.get("chosenActions", None)
    final_folder = os.path.join(FS.Final, country, campaign_short_code)
    if not os.path.exists(final_folder):
        os.makedirs(final_folder)
    for chosenAction in chosenActions:
        print(f"DOING: {chosenAction}")
        if chosenAction == 'Create Category Overviews':
            for category, category_col_name in zip(
                    settings.get('category_overview_settings', {}).get('category_names', None),
                    settings.get('category_overview_settings', {}).get('category_column_names', None)):
                try:
                    result = createCategoryRegionYearFile(country, category, category_col_name)
                    result = remapColumns(result, col_remap)
                    fname = os.path.join(final_folder, f"{campaign_short_code}-{category}_{country}.csv")
                    result.to_csv(
                        fname,
                        index=False)
                    logging_func(f"Saved category overview for {category}")
                    logging_func(fname)
                except Exception as e:
                    logging_func(f"ERROR: {e}")
        elif chosenAction == 'Create Top5':
            for category in settings.get('top5_settings', {}).get("folders_to_use", None):
                try:
                    category_combinations = None
                    result = createTop5Csv(country, category, category_combinations=category_combinations, allow_user_interaction=False)
                    result = remapColumns(result, col_remap)
                    fname = os.path.join(final_folder, f"{campaign_short_code}-Top5_Tags_{category}_{country}.csv")
                    result.to_csv(
                        fname,
                        index=False)
                    logging_func(f"Saved Top5 for {category}")
                except FileNotFoundError as e:
                    logging_func(f"ERROR: {e}")
        elif chosenAction == 'create Main Section':
            chosen_files: List[str] = [x for x in os.listdir(os.path.join(FS.Final, country, campaign_short_code)) if
                                       not x.startswith(".") and any(map(lambda y: y in x,
                                                                         settings.get("main_section_settings", {}).get(
                                                                             "categories_to_include", None)))]
            result = createMainSectionCsv(country,
                                          chosen_files, campaign_shortname=campaign_short_code)
            result = remapColumns(result, col_remap)
            fname = os.path.join(final_folder, f'{campaign_short_code}_Main_Section_{country}.csv')
            result.to_csv(fname, index=False)
            logging_func(f"Saved Main Section")
        elif chosenAction == 'create Chart Data':
            result = createTagChartData(country,
                                        min_regions=settings.get("chart_settings", {}).get("min_region_count", None),
                                        select_tags=False,
                                        selected_tags=settings.get("chart_settings", {}).get("tags_selected", None))
            result = remapColumns(result, col_remap)
            fname = os.path.join(final_folder, f'{campaign_short_code}_Chart_Data_{country}.csv')
            result.to_csv(fname, index=False)
            logging_func(f"Saved Chart Data")
        elif chosenAction == 'create Table Data':
            result = createTableData(country, campaign_short_code)
            result = result.fillna('NA')
            fname = os.path.join(final_folder, f'{campaign_short_code}_Table_Data_{country}.csv')
            result.to_csv(fname, index=False)
            logging_func(f"Saved Table Data")
        elif chosenAction == 'create Map Data':
            result = createMapData(country, campaign_short_code,
                                   useChart=settings.get("map_settings", {}).get("use_chart_data", True))
            result = result.fillna('NA')
            fname = os.path.join(final_folder, f'{campaign_short_code}_Map_Data_{country}.csv')
            result.to_csv(fname, index=False)
            logging_func(f"Saved Map Data")
        logging_func(f'FINISHED {chosenAction}')
    logging_func('FINISHED ALL')


if __name__ == '__main__':
    dialog()

# TODO (p0): Write tutorial function which displays requirements for each type of function
