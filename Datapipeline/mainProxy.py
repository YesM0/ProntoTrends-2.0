import json
import logging
import os
import random
import sys
if __name__ == '__main__':
    sys.path.extend(['../', './'])
import time
from pprint import pprint
from queue import Queue
from threading import Thread, Lock
from typing import List, Tuple, Union, Dict

import pandas as pd
from matplotlib import pyplot as plt
from pytrends.request import TrendReq

from utils.custom_types import *
from utils.misc_utils import getToday, saveData, sleep, getDirectory, lcol, \
    deduplicateColumns, reverseDict
from utils.user_interaction_utils import binaryResponse, choose_from_dict, chooseFile, defineFilename
from utils.Filesys import generic_FileServer
try:
    from utils.Filesys import GDrive_FileServer
except ImportError:
    pass
from utils.Countries import countries_dict_eng, generateRegionIds
from utils.mergeRegions import merge_for_scraper

if __name__ == '__main__' and locals().get("GDrive_FileServer", False):
    c = choose_from_dict(['Save locally', 'Save to GDrive'])
    FS = generic_FileServer if c == 'Save locally' else GDrive_FileServer
else:
    FS = generic_FileServer

SLEEPTIME: int = 0
ABORT_EARLY: bool = False
ABORT_AT: int = 5  # requests without data
NUM_PROXIES: int = 30
NUM_THREADS: int = 15
Lock = Lock()
logging.basicConfig(
    format=f'{lcol.OKBLUE}%(threadName)s:%(levelname)s - %(module)s - %(funcName)s - {lcol.ENDC}%(message)s',
    level=logging.INFO, )


def getProxies(num_proxies: int = 15) -> List[str]:
    """
    Retrieves/ Creates the proxy addresses to be used.
    Args:
        num_proxies: int -- number of proxies to be used

    Returns: list of proxies

    """
    if not isinstance(num_proxies, int):
        num_proxies = 15
    items = []
    for i in range(num_proxies):
        sId = random.random()
        exit_country = random.choice(["de", "it", "fr", "es", "at", "ch", "us", "gb"])
        item = f"http://lum-customer-prontopro-zone-datacenter_de-country-{exit_country}-session-{sId}:pkjwkauqjsq2@zproxy.lum-superproxy.io:22225"
        items.append(item)
    logging.info(f"Using {len(items)} proxies")
    return items


def prepareTrendReqObj(num_proxies: int = 15) -> TrendReq:
    """
    Sets up a PyTrends Requests Object
    Args:
        num_proxies: int -- number of proxies to be used

    Returns: a PyTrends Requests Object

    """
    proxies = getProxies(num_proxies)
    pytrends = TrendReq(hl='en-US', tz=-120, timeout=(10, 25), proxies=proxies, retries=2, backoff_factor=0.5)
    return pytrends


def readInKeywords(short: Country_Shortcode, prefix: str = "", file_path: str = None) -> pd.DataFrame:
    """
    Reads in Keywords from a file of the pattern: Keywords_{short.upper()}.csv
    Has to be in current working directory
    Args:
        file_path: str -- option: path to file if specified
        prefix: str -- optional: prefix to add
        short: short-code of the country to retrieve keywords for

    Returns: a Dataframe of keywords with the respective Keyword Ids

    """
    if file_path:
        return pd.read_csv(file_path)

    if len(prefix) > 0:
        prefix = prefix + "_"
    params = pd.read_csv(os.path.join(FS.Inputs, f"{prefix}Keywords_{short.upper()}.csv"))
    # params = params['Keyword']
    return params


def readInComparisonItems(cc: Country_Shortcode, file_path: Filepath = None) -> Dict[str, Dict[str, List[str]]]:
    """
    Reads in Comparison Items in a JSON File named in the pattern: ProntoPro Trends_Questions_{cc.upper()}.json
    (has to be in Input_Files directory) or passed in as file_path
    Structure of JSON object: {category1: {option1: [keyword1, keyword2], option2: [keyword1, keyword2]}, category2: {option1: [keyword1, keyword2]}}
    Args:
        file_path: Filepath -- optional: choose file of different name
        cc: the country code of the country to retrieve Comparison items for

    Returns: a dictionary of the JSON file

    """
    if file_path:
        with open(file_path, "r") as file:
            string = file.read()
    else:
        try:
            with open(os.path.join(FS.Inputs, f"ProntoPro_Trends_Questions_{cc.upper()}.json"), "r") as file:
                string = file.read()
        except FileNotFoundError as e:
            logging.error(e, exc_info=True)
            with open(os.path.join(FS.Inputs, f"ProntoPro_Trends_Questions_{cc.upper()}.json"), "r") as file:
                string = file.read()
    dictionary = json.loads(string)
    return dictionary


def comparisonRequest(kwds, cc, pytrends, dimension='time', desiredReturn='data') -> Union[
    Tuple[List[pd.DataFrame], List[str], str], Tuple[str, List[pd.DataFrame], List[str], str]]:
    """
    Conducts a request for x amount of keywords. Chains and merges requests if more keywords that can be processed in one request
    Args:
        kwds: list -- keywords to scrape
        cc: str -- country short-code to retrieve data for
        pytrends: PyTrends Instance -- to request data with
        dimension: str -- the desired dimension (can be 'geo' or 'time'). Default: 'time'
        desiredReturn: str -- stated output (can be 'data' or 'most_popular_keyword'). Default: 'data'

    Returns: Depending on desiredReturn: list(results), list(highest_result_timeline), dimension if 'data' else best choice keyword if 'most_popular_keyword'

    """
    logging.info(f"Scraping KW {kwds} for {lcol.OKBLUE}{cc}{lcol.ENDC}")
    if not isinstance(kwds, list):
        kwds = [kwds]
    amountKwds = len(kwds)
    curr_ind = 0
    most_popular_kwd = ""
    curr_selection = []
    highest_result_timeline = []
    results: List[pd.DataFrame] = []
    while curr_ind < amountKwds:
        # generate list of kwds
        while len(curr_selection) < 5 and curr_ind < amountKwds:
            curr_selection.append(kwds[curr_ind])
            curr_ind += 1
        # get data
        logging.debug(f'Passing {curr_selection} into data collector')
        result = interestOverTime(curr_selection, cc.upper(), pytrends) if dimension == 'time' else interestByRegion(
            curr_selection, cc.upper(), pytrends)
        results.append(result)
        # find kwd with 100 (if multiple: higher average)
        logging.debug(f"RESULT\n{result.head()}")
        highest_kwd = identifyHighestCol(result)
        logging.debug(f"Highest Keyword: {highest_kwd}")
        if highest_kwd:
            if isinstance(highest_kwd, int):
                logging.warning("Highest column looks wrong")
            curr_selection = [highest_kwd]
            most_popular_kwd = highest_kwd
            highest_result_timeline.append(highest_kwd)
        else:
            curr_selection = []
        if curr_ind < amountKwds:
            sleep(SLEEPTIME)
    if desiredReturn == 'most_popular_keyword':
        return most_popular_kwd, results, highest_result_timeline, dimension
    elif desiredReturn == 'data':
        return results, highest_result_timeline, dimension


def containsIntCols(df: pd.DataFrame) -> bool:
    return len([col for col in df.columns if isinstance(col, int)]) > 0


def mergeResults(results: List[pd.DataFrame], highest_columns: List[str], dimension: str) -> pd.DataFrame:
    """
    Merges a list of Dataframes to one
    Args:
        results: list -- Dataframes obtained in individual scrapes
        highest_columns: list -- names of the respective highest column of each scrape
        dimension: str -- identifies the dimension of the data -> to choose the merge_column

    Returns: an overall Dataframe combining all results, scaled to fit the maximum result

    """
    merge_column = 'date' if dimension == 'time' else 'geoName'
    if len(results) == 1:
        return results[0]
    elif len(results) == 0:
        return pd.DataFrame()
    else:
        overall = results[0]
        if containsIntCols(overall):
            logging.warning("We have an issue")
        for i in range(1, len(results)):
            curr_res = results[i]
            if containsIntCols(curr_res):
                logging.warning("Res has an int col")
            if not curr_res.empty and len(highest_columns) > i:
                curr_high = highest_columns[i]
                try:
                    if curr_high != highest_columns[i - 1] and len(results) == len(highest_columns):
                        logging.debug(
                            f"Having to rescale info. Previous highest col: {highest_columns[i - 1]}. Now: {highest_columns[i]}")
                        scalar = curr_res[highest_columns[i - 1]].mean() / curr_res[curr_high].mean()
                        logging.debug(f"Scaling factor: {scalar}")
                        for column in overall.columns:
                            if isinstance(column, int):
                                logging.warning(f"There's this column: {column}")
                                logging.warning(overall.head())
                                raise RuntimeWarning("I'll not rescale")
                            overall[column] = overall[column].apply(scaleItem, args=(scalar,))
                except Exception as e:
                    logging.debug(e)
                if not overall.empty:
                    overall = overall.merge(curr_res, how='outer', on=merge_column)
                    overall = deduplicateColumns(overall, extra='isPartial')
                else:
                    overall = curr_res
        return overall


def scaleItem(item: Union[int, float], scalar: Union[int, float]):
    """
    Scales the given value by the given scalar - does nothing if the value is
    Args:
        item: the value to be scaled
        scalar: float -- the ratio by which to scale the value

    Returns: the scaled value

    """
    if not isinstance(scalar, int) and not isinstance(scalar, float):
        return item
    else:
        return item * scalar


def preparePayloads(comparisons_dict: Dict[str, Dict[str, List[str]]], cc, pytrends: TrendReq) -> List[
    Tuple[str, List[str]]]:
    """
    Creates the final payloads chosen from the possible keyword possibilities (based on highest search volume)
    Args:
        comparisons_dict: dict -- dict of structure {category1: {option1: [keyword1, keyword2], option2: [keyword1, keyword2]}, category2: {option1: [keyword1, keyword2]}}
        cc: str -- the country code of the country the function is checking popularity for
        pytrends: PyTrends Object -- to execute requests

    Returns: list -- payload-tuples (category_name, [options])

    """
    logging.info("Building Payloads")
    payloads = []
    cache = {}
    save_findings = binaryResponse("Do you want to save the findings of the keyword selection in a separate json file?")
    filepath = defineFilename(target_ending=".json", target_folder=FS.Inputs) if save_findings else None
    show_graph = binaryResponse("Do you want to view the relative averages for the keywords within the options?")
    for category, options in comparisons_dict.items():
        cat_cache = {}
        logging.info(f"Checking on category: {lcol.OKBLUE}{category}{lcol.ENDC}")
        category_keywords = []
        for option in options:
            logging.info(f'Checking for best keyword for option: {lcol.OKBLUE}{option}{lcol.ENDC}')
            kwds: List[str] = options[option]
            if len(kwds) > 1:
                most_popular_kwd, results, highest_result_timeline, dimension = comparisonRequest(kwds, cc, pytrends,
                                                                                                  dimension='time',
                                                                                                  desiredReturn='most_popular_keyword')
                logging.info(f"Found most popular KWD: {lcol.OKGREEN}{most_popular_kwd}{lcol.ENDC}")
                if show_graph:
                    merged = mergeResults(results, highest_result_timeline, dimension)
                    showAverages(merged, option_name=option)
            elif len(kwds) == 1:
                most_popular_kwd: str = kwds[0]
            else:
                continue
            if len(most_popular_kwd) > 0:
                category_keywords.append(most_popular_kwd)
                cat_cache[option] = most_popular_kwd
        if len(category_keywords) > 0:
            category_keywords = list(set(category_keywords))
            payloads.append((category, category_keywords))
            cache[category] = cat_cache
    if save_findings:
        with open(filepath, "w+") as f:
            f.write(json.dumps(cache))
        print(f"Saved findings in file:/{filepath}")
    return payloads


def showAverages(df: pd.DataFrame, option_name: str = ''):
    means = df.mean(axis=0)
    means.plot.bar(title=f'Average interest for keywords of option: {option_name}', figsize=(len(df.columns) * 0.5, 10))
    plt.show()


def identifyHighestCol(result: pd.DataFrame) -> Union[None, str]:
    """
    Identifies the column with the highest values in a Dataframe
    Args:
        result: Dataframe -- the Dataframe to work on

    Returns: the column name of the highest value column

    """
    candidates = []
    for column in result.columns:
        if column == 'date' or column == 'geoName':
            continue
        contains_100 = result[column].isin([100]).any()
        if contains_100:
            candidates.append(column)
    if len(candidates) == 0:
        return None
    else:
        higher_avg = candidates[0]
        highscore = result[higher_avg].mean()
        if len(candidates) > 1:
            for i in range(1, len(candidates)):
                candidate = candidates[i]
                avg = result[candidate].mean()
                if avg > highscore:
                    highscore = avg
                    higher_avg = candidate
        return higher_avg


def requestManager_Thread(taskQueue: Queue):
    """
    Handles the communication with the task queue. Runs scraping and saving
    Args:
        taskQueue: Queue -- the Queue which stores the respective tasks

    Returns: None

    """
    # logging.info(queue)
    while not taskQueue.empty():
        t0 = time.time()
        inputs = taskQueue.get()
        # logging.info(inputs)
        results, highest_result_timeline, dimension = comparisonRequest(inputs['kwd'], inputs['locale'],
                                                                        inputs['pytrends'],
                                                                        dimension=inputs.get('dimension', 'time'),
                                                                        desiredReturn=inputs.get('desiredReturn',
                                                                                                 'data'))
        result = mergeResults(results, highest_result_timeline, dimension)
        if result.size == 0:
            logging.info(f"{lcol.WARNING}No Data for {inputs['kwd']} in {inputs['locale']}{lcol.ENDC}")
        else:
            if inputs['isList']:
                fn = f"{inputs['dimension']}_{inputs['countryName']}_{inputs['locale']}_{inputs['comparison_label']}.csv"
            else:
                fn = f"{inputs['dimension']}_{inputs['countryName']}_{inputs['locale']}_{str(inputs['kwd'])}_{inputs['keyword_id']}.csv"
            if "/" in fn:
                fn = fn.replace("/", "-")
            filepath = os.path.join(inputs['directory'], fn)
            saveData(result, filepath)
            logging.info(
                f"{lcol.UNDERLINE}Saved Data for {inputs.get('dimension', 'time')} {inputs['kwd']} in {inputs['locale']}. Shape: {result.shape}{lcol.ENDC}")
        logging.info(f"Scraping {inputs['kwd']} for {inputs['locale']} took {time.time() - t0} seconds")


def scrape_all_regions(keyword: Union[str, List[str]], country_name: Country_Fullname, pytrends: TrendReq,
                       keyword_id: Union[str, int] = '', folder: Union[List[str], str] = ('Output_Files', 'out'),
                       comparison_label: str = '', num_threads: int = NUM_THREADS):
    """
    Scrapes time and geo data for a country and keyword/keywords. Goes through all regions for Time and country level for Geo
    Args:
        num_threads: int -- number of Threads to start - defaults to Global NUM_THREADS
        keyword: str or list -- the keywords to scrape for
        country_name: str -- full name of the country to scrape for (English, as per Google)
        pytrends: PyTrends Instance -- to scrape
        keyword_id: int -- optional - for naming the file
        folder: str or list -- the path where files should be saved
        comparison_label: str -- used to save file under a meaningful name

    Returns: False if no problems otherwise keyword if no data present

    """
    t1 = time.time()
    isList = isinstance(keyword, list)
    kwd = keyword if isList else [keyword]
    locales_list = generateRegionIds(country_name, override=False)
    path_steps = resolvePathSteps(folder, keyword, isList)
    directory = getDirectory(path_steps)

    # prepare requests:
    requestsQueue = Queue()
    for locale in locales_list:
        req = {
            "kwd": kwd,
            "locale": locale,
            "pytrends": pytrends,
            "dimension": 'time',
            'desiredReturn': 'data',
            'countryName': country_name,
            'keyword_id': keyword_id,
            'folder': folder,
            'comparison_label': comparison_label,
            'isList': isList,
            'directory': directory
        }
        requestsQueue.put(req)
    geo_req = {
        "kwd": kwd,
        "locale": locales_list[0],
        "pytrends": pytrends,
        "dimension": 'Geo',
        'desiredReturn': 'data',
        'countryName': country_name,
        'keyword_id': keyword_id,
        'folder': folder,
        'comparison_label': comparison_label,
        'isList': isList,
        'directory': directory
    }
    requestsQueue.put(geo_req)
    # start Threads
    taskThreads = []
    if isList:
        num_threads = 1
    for i in range(num_threads):
        t = Thread(target=requestManager_Thread, args=(requestsQueue,), name=f"Worker Thread {i + 1}")
        taskThreads.append(t)
        t.start()
    # time.sleep(10)
    for t in taskThreads:
        t.join()
    t2 = time.time()
    logging.info(f"Scraping {keyword} took {t2 - t1} sec")
    merge_for_scraper(directory, country_shortcode=reverseDict(countries_dict_eng).get(country_name, country_name[:2]).upper())


def scrape_shallow(keywords_df: pd.DataFrame, country_name: Country_Fullname, pytrends: TrendReq,
                   folder: Union[str, List[str]] = ("Output_Files", 'out'), comparison_label: str = '',
                   num_threads: int = NUM_THREADS):
    """
    Scrapes time and geo data for a country and keyword/keywords. Goes through all regions for Time and country level for Geo
    Args:
        num_threads: int -- number of Threads to start - defaults to Global NUM_THREADS
        keywords_df: df of keywords & kwd_ids
        country_name: str -- full name of the country to scrape for (English, as per Google)
        pytrends: PyTrends Instance -- to scrape
        folder: str or list -- the path where files should be saved
        comparison_label: str -- used to save file under a meaningful name

    Returns: False if no problems otherwise keyword if no data present

    """
    locale = generateRegionIds(country_name)[0]
    requestsQueue = Queue()
    for ind, row in keywords_df.iterrows():
        kwd = row['Keyword']
        kwd_id = row['kwd_id']
        path_steps = resolvePathSteps(folder, kwd)
        directory = getDirectory(path_steps)
        req = {
            "kwd": kwd,
            "locale": locale,
            "pytrends": pytrends,
            "dimension": 'time',
            'desiredReturn': 'data',
            'countryName': country_name,
            'keyword_id': kwd_id,
            'folder': folder,
            'comparison_label': comparison_label,
            'isList': False,
            'directory': directory
        }
        requestsQueue.put(req)
        geo_req = {
            "kwd": kwd,
            "locale": locale,
            "pytrends": pytrends,
            "dimension": 'Geo',
            'desiredReturn': 'data',
            'countryName': country_name,
            'keyword_id': kwd_id,
            'folder': folder,
            'comparison_label': comparison_label,
            'isList': False,
            'directory': directory
        }
        requestsQueue.put(geo_req)
    # start Threads
    taskThreads = []
    for i in range(num_threads):
        t = Thread(target=requestManager_Thread, args=(requestsQueue,), name=f"Worker Thread {i + 1}")
        taskThreads.append(t)
        t.start()
    # time.sleep(10)
    for t in taskThreads:
        t.join()


def interestOverTime(kwd: list, locale: Region_Shortcode, pytrends: TrendReq) -> pd.DataFrame:
    """
    Single request for interest over time
    Args:
        kwd: list -- kwds to compare
        locale: str -- locale shortname to look at
        pytrends: PyTrends Instance -- to use for scraping

    Returns: Dataframe of result [ind, date, kwd1...]

    """
    while True:
        try:
            kwd = list(set(kwd))
            with Lock:
                logging.debug(f"Building payload: {kwd}")
                pytrends.build_payload(kwd, timeframe=f'2018-01-01 {getToday()}', geo=locale, gprop='')
            result = pytrends.interest_over_time()
            break
        except Exception as e:
            logging.warning("Error in Interest over Time")
            logging.error(f"THIS ERROR: {e}", exc_info=True)
    return result


def interestByRegion(keyword: list, cc: Region_Shortcode, pytrends: TrendReq) -> pd.DataFrame:
    """
        Single request for interest by Region
        Args:
            keyword: list -- kwds to compare
            cc: str -- locale shortname to look at
            pytrends: PyTrends Instance -- to use for scraping

        Returns: Dataframe of result [ind, geoName, kwd1...]

        """
    while True:
        try:
            keyword = list(set(keyword))
            keyword = [kwd for kwd in keyword if not isinstance(kwd, int)]
            pytrends.build_payload(keyword, timeframe='today 3-m', geo=cc, gprop='')
            result = pytrends.interest_by_region(resolution='REGION', inc_low_vol=True, inc_geo_code=True)
            logging.debug(result.head())
            break
        except Exception as e:
            logging.warning(e)
    return result


def resolvePathSteps(folder: Union[List[str], str], keyword: str, isList: bool = False) -> List[str]:
    """
    Resolves the path steps into a list
    Args:
        folder: list or str -- the initial path steps
        isList: bool -- to know whether to add keyword level to the path
        keyword: str -- the keyword to add to the path

    Returns: list of the path steps

    """
    path_steps = []
    if isinstance(folder, str):
        path_steps.append(folder)
    else:
        for step in folder:
            path_steps.append(step)
    if not isList:
        path_steps.append(keyword)
    path_steps = list(map(lambda x: x.replace("/", "-") if "/" in x else x, path_steps))
    return path_steps


def getChosenCountries() -> List[Tuple[Country_Shortcode, Country_Fullname]]:
    """
    Asks user for selection of countries to scrape for
    Returns: List[Tuple[Country_Shortcode, Country_Fullname]] -- A list of the chosen ccs (validated)

    """
    chosen_ccs = []
    while True:
        pprint(countries_dict_eng)
        chosen = input(
            "What countries do you want to scrape for? Please put in the shortcodes (separated by commas)\n").strip().lower()
        for i in chosen.split(","):
            res = countries_dict_eng.get(i.strip(), None)
            if res:
                chosen_ccs.append((Country_Shortcode(i.strip()), Country_Fullname(res)))
        if len(chosen_ccs) > 0:
            return chosen_ccs
        else:
            sys.exit()


def deduplicateKeywords(keywords_df: pd.DataFrame) -> pd.DataFrame:
    paths = [x for x in [FS.Kwd_Level_Outs, os.path.join(FS.Outfiles_general, 'cc_level')] if os.path.exists(x)]
    folders = []
    for path in paths:
        path_folders = os.listdir(path)
        for fld in path_folders:
            try:
                fld_contents = os.listdir(os.path.join(path, fld))
                if len(fld_contents) > 0:
                    folders.append(fld)
            except Exception as e:
                print(e)
    c = 0
    for ind, row in keywords_df.iterrows():
        if row['Keyword'] in folders:
            keywords_df.drop(labels=ind, axis=0, inplace=True)
            c += 1
    logging.info(f"Dropped {c} keywords")
    return keywords_df


def dialog():
    tr = prepareTrendReqObj(NUM_PROXIES)
    ccs_todo = getChosenCountries()

    choice = choose_from_dict({
        1: 'Individual Keywords ("Keywords_CC.csv) - All Regions',
        2: 'Individual Keywords ("Keywords_CC.csv) - Only Country',
        3: 'Comparisons (ProntoPro_Trends_Questions_CC.csv) - All Regions'
    }, 'actions')
    doDeduplicateKeywords = False
    prefix = False
    choose_file = binaryResponse("Do you want to specify the file to use?")
    if not choose_file and (
            choice == 'Individual Keywords ("Keywords_CC.csv) - All Regions' or choice == 'Individual Keywords ("Keywords_CC.csv) - Only Country'):
        prefix = input(
            "What is the prefix to your file? e.g. '[All]_Keywords_DE.csv'\n").strip() if binaryResponse(
            "Do you have a prefix to your file_name?") else ""
        doDeduplicateKeywords = binaryResponse("Do you want to remove already scraped keywords?")

    for short, country in ccs_todo:
        file: None = None
        if choose_file:
            ft = ".csv" if "Individual Keywords" in choice else ".json"
            file: Filepath = chooseFile(filetype=ft, other_only_if_contains_selections=[short, country], base_path=FS.cwd)
        if choice == 'Individual Keywords ("Keywords_CC.csv) - All Regions':
            keywords = readInKeywords(short, prefix=prefix) if not file else readInKeywords(short, prefix=prefix,
                                                                                            file_path=file)
            if doDeduplicateKeywords:
                keywords = deduplicateKeywords(keywords)
            failedScrapes = []
            for row_index, row in keywords.iterrows():
                logging.info(f"{row_index} of {keywords.shape[0]}")
                keyword = row['Keyword'].strip()
                kwd_id = row['kwd_id']
                logging.info(f"Working on keyword {keyword}")
                scrape_all_regions(keyword, country, tr, keyword_id=kwd_id, folder=['Output_Files', 'out'])
                sleep(SLEEPTIME)
            print(
                f"{lcol.OKBLUE}Scraping is finished. To continue the pipeline, use generateSummaries.py and merge the keyword level data to tags{lcol.ENDC}")
        elif choice == 'Comparisons (ProntoPro_Trends_Questions_CC.csv) - All Regions':
            logging.info(f"{lcol.HEADER} Scraping a comparison {lcol.ENDC}")
            comparisonItems = readInComparisonItems(short, file_path=file)
            payloads = preparePayloads(comparisonItems, short, tr)
            for category, keywords in payloads:
                logging.info(f'Working on comparison for {category}')
                scrape_all_regions(keywords, country, tr, folder=['Output_Files', 'comparisons', category], comparison_label=category)
                sleep(SLEEPTIME)
            print(
                f"{lcol.OKBLUE}Scraping is finished. To continue the pipeline, use finalCSVgenerator.py{lcol.ENDC}")
        elif choice == 'Individual Keywords ("Keywords_CC.csv) - Only Country':
            keywords_df = readInKeywords(short, prefix=prefix)
            logging.info(f"{lcol.HEADER} Scraping {keywords_df.shape[0]} keywords for {ccs_todo}")
            if doDeduplicateKeywords:
                keywords = deduplicateKeywords(keywords_df)
            scrape_shallow(keywords_df, country, tr, folder='cc_level')
        logging.info("_" * 10 + "FINISHED" + "_" * 10)


if __name__ == '__main__':
    dialog()
