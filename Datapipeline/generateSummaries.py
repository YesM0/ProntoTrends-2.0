if __name__ == '__main__':
    import sys

    sys.path.append('../')

import os
from typing import Union, Dict, Callable

import pandas as pd
from tqdm import tqdm as progressbar

from Data_Inspectors.inspector import getAvailableTags, getFile
from utils.Countries import get_region_id_to_name_dict, generateRegionIds, getChosenCountries
from utils.Filesys import generic_FileServer
from utils.custom_types import *
from utils.misc_utils import getDirectory, reverseDict, lcol, save_csv
from utils.user_interaction_utils import binaryResponse, choose_from_dict

FS = generic_FileServer


def readMergeInfo(country_short_code, prefix=""):
    country_short_code = country_short_code.upper()
    if len(prefix) > 0:
        prefix = prefix + "_"
    csv = pd.read_csv(os.path.join(FS.Inputs, f"{prefix}Tag_Keyword_{country_short_code}.csv"), encoding = 'latin1')
    csv.rename(axis='columns', mapper={'tag': 'tag_name'}, inplace=True)
    return csv


def findOutFile(keyword, country, region_code, dimension, folder=FS.Kwd_Level_Outs):
    not_dimension = list(filter(lambda x: x != dimension, ['Time', 'Geo']))

    expected_file_location = os.path.join(folder, keyword)
    # expected_file_pattern = f"{dimension}_{country}_{region_code}_['{keyword}']_" if dimension == 'Time' else f"{dimension}_{country}_['{keyword}']_"
    if os.path.exists(expected_file_location):
        # print(f"Expected File location {expected_file_location}")
        for filename in os.listdir(expected_file_location):
            # if expected_file_pattern.lower() in filename.lower():
            if not_dimension[0] + "_" in filename:
                continue
            if keyword in filename and region_code in filename and dimension.lower() + "_" in filename.lower():
                return os.path.join(expected_file_location, filename)
    return None


def readFile(filepath, dimension):
    cols = None if dimension == 'Geo' else [0, 1]
    df = pd.read_csv(filepath, usecols=cols)
    to_drop = []
    if 'geoCode' in df.columns:
        to_drop.append('geoCode')
    if any([True for x in df.columns if "Unnamed" in x]):
        to_drop.extend([x for x in df.columns if 'unnamed' in x.lower()])
    if len(to_drop) > 0:
        df = df.drop(columns=to_drop)
    return df


def remap(value, maximum):
    if pd.isna(value) or pd.isna(maximum) or isinstance(maximum, str):
        return 0
    else:
        if maximum != 0:
            v = round((value / maximum) * 100)
        else:
            v = 0
    return v


class AnalysisJob:
    instances = {}

    def __init__(self, tag_id, tag_name, keywords, country_name, region_code, dimension, folder=FS.Kwd_Level_Outs):
        if (isinstance(keywords, str)):
            keywords = [keywords]
        try:
            if isinstance(tag_id, int) or isinstance(tag_id, float):
                tag_id = int(tag_id)
        except:
            print(f"The tag Id '{tag_id}' could not be coerced to an Int")
        self.tag_id = tag_id
        self.tag_name = tag_name if "/" not in tag_name else tag_name.replace("/", " ")
        self.keywords = keywords
        self.country_name = country_name
        self.region_code = region_code
        self.dimension = dimension
        self.result = None
        self.folder = folder
        self.id = f"{self.tag_id}-{self.region_code}-{self.dimension}"
        AnalysisJob.instances[self.id] = self

    def addKeyword(self, keyword):
        self.keywords.append(keyword)

    def __repr__(self):
        return f"{self.dimension}-Analysis Job for {self.tag_name} ({self.tag_id}); Keywords: {self.keywords}"

    def aggregate(self):
        # find all files associated
        merge_col = 'date' if self.dimension == 'Time' else 'geoName'
        files = []
        # if self.dimension == 'Geo':
        #     print('Its a geography')
        for keyword in self.keywords:
            file = findOutFile(keyword, self.country_name, self.region_code, self.dimension, folder=self.folder)
            if file:
                files.append(file)
        if len(files) > 0:
            df = readFile(files[0], self.dimension)
            # print(df.head)
            for i in range(1, len(files)):
                df2 = readFile(files[i], self.dimension)
                try:
                    df = df.merge(df2, how='outer', on=merge_col)
                except KeyError as e:
                    print(e)
                    print(f"The key {merge_col} could not be found in the df of file: file://{files[i]}")
                    print(f"{lcol.OKGREEN}Here's the merge df{lcol.ENDC}")
                    print(df.head())
                    print(f"{lcol.OKGREEN}Here's the new dataframe that should be added{lcol.ENDC}")
                    print(df2.head())
                    continue
                # print(df.head)
            try:
                df['means'] = df.mean(1, skipna=True, numeric_only=True)
                maximum = df['means'].max()
                df['means'] = df['means'].apply(remap, maximum=maximum)
                endResult = df[[merge_col, 'means']]
                self.result = endResult
                return endResult
            except KeyError as e:
                print(f"{lcol.WARNING}{e}{lcol.ENDC}")
                return None
        else:
            return None

    # class method to access the get method without any instance
    @classmethod
    def get(cls, tag_id, region_code, dimension, fallback_val=False):
        return cls.instances.get(f"{tag_id}-{region_code}-{dimension}", fallback_val)


def buildJobs(mergeInfo, country, is_other_folder=False, only_country_level=False, source_folder=FS.Kwd_Level_Outs):
    dimensions = ['Time', 'Geo']
    region_codes = generateRegionIds(country)
    if only_country_level:
        region_codes = [region_codes[0]]
    all_jobs = []
    folder = source_folder
    for dimension in dimensions:
        for index, row in progressbar(mergeInfo.iterrows(), total=mergeInfo.shape[0]):
            tag_id = row['tag_id']
            tag_name = row['tag_name']
            if tag_name != tag_name:
                continue
            keyword = row['keyword']
            for ind, region_code in enumerate(region_codes):
                if dimension == 'Geo' and ind > 0:
                    continue
                else:
                    job: Union[bool, AnalysisJob] = AnalysisJob.get(tag_id=tag_id, region_code=region_code,
                                                                    dimension=dimension,
                                                                    fallback_val=False)
                    if job:
                        job.addKeyword(keyword)
                    else:
                        new_job = AnalysisJob(tag_id, tag_name, keyword, country, region_code, dimension, folder=folder)
                        all_jobs.append(new_job)
    return all_jobs


def execute_and_save(country: str, all_jobs: list, adjusted_directory=None, logging_func: Callable = print):
    directory = getDirectory(["Output_Files", 'Aggregated', country]) if not adjusted_directory else getDirectory(
        adjusted_directory)
    count_saved = 0
    count_failes = 0
    for job in progressbar(all_jobs):
        result = job.aggregate()
        if result is not None:
            file_path = os.path.join(directory, f"{job.region_code}_{job.tag_id}_{job.tag_name}_{job.dimension}.csv")
            save_csv(result, file_path, logging_func=logging_func)
            logging_func(f"Saved file: {file_path}")
            count_saved += 1
        else:
            logging_func(f"No Output for job: {job}")
            count_failes += 1
    logging_func(f"{lcol.WARNING}Saved {count_saved} files. Failed: {count_failes}{lcol.ENDC}")


def read_in_scaling_factor(country_name: Country_Fullname, county_shortcode: Country_Shortcode, tag_id: Union[str, int] = None, tag_name: str = None):
    tag_id = str(tag_id) if not isinstance(tag_id, str) else tag_id
    expected_file_location = os.path.join(FS.Aggregated, country_name)
    filepath = ''
    poss_files = os.listdir(expected_file_location)
    for i, filename in enumerate(poss_files):
        # if i % 10 == 0:
        #     print(f'Checking file {i + 1} of {len(poss_files)}\n{filename}')
        components = filename.split("_")
        if len(components) >= 4 and components[0] == county_shortcode.upper() and components[3] == "Geo.csv" and (components[1] == tag_id or components[2] == tag_name):
            # print("Found file")
            filepath = os.path.join(expected_file_location, filename)
            break
    if filepath:
        df = pd.read_csv(filepath, usecols=[1, 2])
        region_id_to_region_name = get_region_id_to_name_dict(country_name, allow_override=True)
        region_name_to_region_id = reverseDict(region_id_to_region_name)
        df['geoName'] = df['geoName'].map(region_name_to_region_id)
        obj = df.set_index('geoName').to_dict(orient='dict')
        return obj['means']


def correct_values(country_name: Country_Fullname, short_code: Country_Shortcode, logging_func: Callable = print):
    all_tags: Dict[Union[str, int], str] = getAvailableTags(country_name, short_code, 'Geo')
    for i, (tag_name, tag_id) in enumerate(all_tags.items()):
        logging_func(f"Working on tag {tag_name} ({i + 1}/{len(all_tags)})")
        scaling_factors = read_in_scaling_factor(country_name, short_code, tag_id=tag_id, tag_name=tag_name)
        corrected_count = 0
        for region_id in scaling_factors:
            if pd.isna(region_id):
                continue
            logging_func(f"Trying to find the file for {region_id}")
            file = getFile(country_name, tag_name, 'Time', region_id)
            if file:
                logging_func(f"Adjusting the file for region {region_id}")
                df = pd.read_csv(file)
                df['means'] = df['means'].apply(lambda x: x * (scaling_factors[region_id] / 100))
                df.to_csv(os.path.join(FS.Aggregated, country_name,
                                       f"{region_id}_{tag_id}_{tag_name}_Time_Adjusted.csv"))
                logging_func(f"{lcol.OKGREEN}Saved file{lcol.ENDC}")
                corrected_count += 1
            else:
                continue
        logging_func(f"{lcol.OKBLUE}Adjusted {corrected_count} files for {tag_name} {lcol.ENDC}")
    return


def dialog():
    choice = choose_from_dict({1: 'merge Data', 2: 'create Adjusted Files'}, label='actions')
    ccs_todo = getChosenCountries()
    if choice == 'merge Data':
        prefix = input(
            "What is the prefix to your file? e.g. '[All]_Tags_Keywords_DE.csv'\n").strip() if binaryResponse(
            "Do you have a prefix to your file_name?") else ""
        diff_folder = binaryResponse("Do you want to aggregate a different folder than 'out'?")
        only_country = binaryResponse("Do you want to only work on the country level?")
        if diff_folder:
            save_folder = input(f"What is the folder to {lcol.UNDERLINE}save into?{lcol.ENDC}\n").strip()
        source_folder = FS.Kwd_Level_Outs if not diff_folder else input(
            "What is the name of your folder to source the data from?\n").strip()
        for short, country in ccs_todo:
            mergeInfo = readMergeInfo(short, prefix=prefix)
            other_save_location = None if not diff_folder else [save_folder, country]
            print(f"Building merge jobs...")
            jobs = buildJobs(mergeInfo=mergeInfo, country=country, is_other_folder=diff_folder,
                             only_country_level=only_country, source_folder=source_folder)
            execute_and_save(country=country, all_jobs=jobs, adjusted_directory=other_save_location)
        print(
            f"{lcol.OKBLUE}Merging data is done. Normally you'd want to run the data adjustments too{lcol.ENDC}")
        if binaryResponse("Do you want to create Adjusted Files too?"):
            for short, country in ccs_todo:
                correct_values(country, short)
            print(
                f"{lcol.OKBLUE}Adjusting data is done. To continue the pipeline, use finalCSVgenerator.py to generate files like the chart, table or map{lcol.ENDC}")
    elif choice == 'create Adjusted Files':
        for short, country in ccs_todo:
            correct_values(country, short)
        print(
            f"{lcol.OKBLUE}Adjusting data is done. To continue the pipeline, use finalCSVgenerator.py to generate files like the chart, table or map{lcol.ENDC}")
    else:
        pass
    print("_" * 10 + "Done" + "_" * 10)


if __name__ == '__main__':
    dialog()
