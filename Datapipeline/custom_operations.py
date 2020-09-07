if __name__ == '__main__':
    import sys

    sys.path.append('../')

import os
from typing import List, Dict

import pandas as pd

from utils.Countries import Country, getCountry
from utils.Filesys import generic_FileServer as FS
from utils.custom_types import *
from utils.user_interaction_utils import chooseFolder, defineFilename


def prepare_budget_file(country: Country) -> pd.DataFrame:
    """
    Uses Comparisons Data to generate a file where all tags
    Returns:

    """
    # 1. Determine where to find the comparisons file
    folder: Folderpath = chooseFolder(request_str="Please choose the folder where to take the data from.",
                                      base_folder=FS.Comparisons)
    # 2. read in files
    country_files: List[str] = list(
        filter(lambda filename: country.Full_name in filename and 'geo' not in filename.lower(), os.listdir(folder)))
    eligible_files: List[str] = list(
        filter(lambda filename: filename.split("_")[2] in (country.region_ids + [country.Shortcode.upper()]),
               country_files))
    region_id_to_name: Dict[Region_Shortcode, Region_Fullname] = country.region_ids_to_names
    region_id_to_name[country.Shortcode.upper()] = country.Full_name
    merged_df = None
    for file in eligible_files:
        filepath: Filepath = os.path.join(folder, file)
        region: Region_Fullname = region_id_to_name.get(file.split("_")[2], file.split("_")[2])
        df = pd.read_csv(filepath)
        if 'date' not in df.columns:
            continue
        # 3. Group by Year
        df['date'] = pd.to_datetime(df['date'])
        df = df.resample('Y', on='date').mean()
        df = df.reset_index()
        df['ticket_geo_region_name'] = region
        if merged_df is not None:
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        else:
            merged_df = df.copy()
    # 3. Melt from broad to narrow df
    if merged_df is not None and not merged_df.empty:
        # merged_df = merged_df.reset_index()
        merged_df = merged_df.melt(id_vars=['date', 'ticket_geo_region_name'], value_name='Index',
                                   var_name='ticket_taxonomy_tag_name')
        # 4. Scale Data
        year_sums = merged_df.resample('Y', on='date').sum()
        year_sums['year'] = year_sums.index.year
        year_sums = year_sums.set_index('year').to_dict(orient='index')

        def scale(row, year_sums):
            divisor_entry = year_sums.get(row['date'].year, dict())
            divisor = divisor_entry.get('Index', row['Index'])
            row['Index'] = row['Index'] / divisor
            return row

        merged_df = merged_df.apply(scale, args=(year_sums,), axis=1)
        print(merged_df)
    return merged_df


if __name__ == '__main__':
    country: Country = getCountry("What country do you want to work on?")
    final_df = prepare_budget_file(country)
    fn = defineFilename(target_ending='.csv',
                        target_folder=chooseFolder(request_str="Where do you want the output to be saved?",
                                                   base_folder=FS.Outfiles_general))
    final_df.to_csv(fn, index=False)
    print(f"Saved file: file://{fn}")
