if __name__ == '__main__':
    import sys
    sys.path.append('../')

import json
import os
from typing import Union, List

import pandas as pd
from matplotlib import pyplot as plt
from pandas.core import resample

from utils.misc_utils import deduplicateColumns, lcol
from utils.user_interaction_utils import getChosenCountry, binaryResponse, choose_from_dict, chooseFile, \
    defineList
from utils.Filesys import generic_FileServer

TESTING = False
TEST_VALS = [
    False,
    ('DE', 'Germany'),
    False,
    "json",
    "testing_comparisons.json",
    "Time",
    "DE",
    "bar"
]

FS = generic_FileServer


def file_finder():
    shortcode, country = getChosenCountry(action="inspect")
    a = os.listdir(FS.Comparisons)
    categories = {i: cat for i, cat in enumerate(os.listdir(FS.Comparisons))}
    chosen_cat = choose_from_dict(categories, 'categories')
    chosen_dimension = choose_from_dict({1: 'Time', 2: 'Geo'}, 'dimensions')
    if chosen_dimension == 'Geo':
        return os.path.join(FS.Comparisons, chosen_cat, f"Geo_{country}_{chosen_cat}.csv")
    else:
        available_regions = {i: file.split('_')[2] for i, file in
                             enumerate(os.listdir(os.path.join(FS.Comparisons, chosen_cat))) if
                             file.split('_')[1] == country and len(file.split('_')[2]) < 6}
        chosen_region = choose_from_dict(available_regions, 'regions')
        return os.path.join(FS.Comparisons, chosen_cat, f"Time_{country}_{chosen_region}_{chosen_cat}.csv")


def analyze(file_path):
    df = pd.read_csv(file_path)
    print(df)
    if 'Time' in file_path:
        df = deduplicateColumns(df, extra='isPartial')
        mean = df.mean(0)
        summe = mean.sum()
        mean = mean.apply(lambda x: x / summe)
        print(mean.head())
        mean.plot.pie()
        plt.show()
    elif 'Geo' in file_path:
        # df = df.drop('geoCode')
        df = df.iloc[:, 1:].set_index('geoCode')
        res = df.div(df.sum(1), axis=0).dropna(axis=0)
        print(res)
        # print(res.sum(1))


def getAggregations_Manual(country, cats) -> dict:
    folder = os.path.join(FS.Aggregated, country)
    availableTags = list(filter(lambda x: "-" not in x.split("_")[0], os.listdir(folder)))
    aggregations = {}
    for cat in cats:
        print(f"{lcol.OKBLUE}What tags do you want to add to category '{cat}'?{lcol.ENDC}")
        tags = []
        final = False
        while not final:
            available = {}
            for f in availableTags:
                try:
                    if f.split("_")[2] not in tags:
                        tg_id = f.split("_")[1]
                        try:
                            tg_id = int(tg_id)
                        except Exception as e:
                            pass
                        available[tg_id] = f.split("_")[2]
                except Exception as e:
                    print(f"Had trouble parsing the filename: {f}. It may be that the file was not desired")
                    print(e)
            tags_selected = choose_from_dict(available, label='Tags',
                                             incl_end_option=True, allow_multiple_answers=True)
            if 'End Selection' in tags_selected:
                filter(lambda x: x == "End Selection", tags_selected)
                final = True
            tags += tags_selected
        aggregations[cat] = tags
    if binaryResponse("Do you want to save your choices to a file?"):
        fileName = input("What filename do you choose?").strip()
        if not "json" in fileName:
            fileName = fileName + ".json"
        with open(fileName, "w+") as f:
            f.write(json.dumps(aggregations))
        print(f"Saved categories to {fileName}")
    return aggregations


def getAggregations_File() -> dict:
    fileType = choose_from_dict({1: "json", 2: "csv"}, 'Filetypes', testing=TESTING, test_return=TEST_VALS.pop(0))
    file = chooseFile(filetype=fileType, testing=TESTING, test_return=TEST_VALS.pop(0))
    if fileType == 'json':
        with open(file, "r") as f:
            aggregations = json.loads(f.read())
    else:
        csv = pd.read_csv(file)
        aggregations = {}
        for ind, row in csv.iterrows():
            row = row.tolist()
            if aggregations.get(row[0], False):
                aggregations[row[0]].append(row[1])
            else:
                aggregations[row[0]] = [row[1]]
    print("Here are the selected categories")
    print(aggregations)
    return aggregations


def readInFiles(files: List[str], folder: str) -> pd.DataFrame:
    overall: Union[bool, pd.DataFrame] = False
    for file in files:
        tag_name = file.split("_")[2]
        csv: pd.DataFrame = pd.read_csv(os.path.join(folder, file), usecols=[1, 2])
        csv = csv.rename(columns={csv.columns[1]: tag_name})
        if overall is False:
            overall = csv.copy()
        else:
            overall = overall.merge(csv, on=overall.columns[0], how="left")
    return overall


def chooseColumns(df: pd.DataFrame, multiple_choice=False):
    options = {i: x for i, x in enumerate(df.columns)}
    options["i"] = "index"
    choice = choose_from_dict(options, "Column", allow_multiple_answers=multiple_choice)
    return choice


def resampleDateData(df: pd.DataFrame, aggregation_level: str):
    resampling_methods = {
        "Mean": resample.PeriodIndexResamplerGroupby.mean,
        'Max': resample.PeriodIndexResamplerGroupby.max,
        "Min": resample.PeriodIndexResamplerGroupby.min,
        # "Sum": resample.PeriodIndexResamplerGroupby.sum,
    }
    print("What method do you want the data to be aggregated by?")
    chosen_method = choose_from_dict({1: 'Mean', 2: 'Max', 3: "Min", 4: "Sum"}, 'methods')
    method = resampling_methods.get(chosen_method,
                                    resample.PeriodIndexResamplerGroupby.mean)
    if chosen_method == 'Mean':
        grouper = 'date'
    else:
        grouper = 'date'
    df_resampled = df.resample(aggregation_level, on=grouper)
    df = method(df_resampled)
    if "date" in df.columns:
        df = df.drop(columns=['date'])
    return df, chosen_method


def deselectColumns(df: pd.DataFrame):
    deselections = []
    while True:
        intermediate = list(filter(lambda x: x not in deselections, df.columns))
        s = choose_from_dict({i: k for i, k in enumerate(intermediate)}, label='Columns', incl_end_option=True, allow_multiple_answers=True)
        if 'End Selection' in s:
            s = list(filter(lambda x: x != "End Selection", s))
            deselections += s
            break
        else:
            deselections += s
    df = df.drop(columns=deselections)
    return df


def displayData(df: pd.DataFrame, data_type, title=""):
    colormap = plt.get_cmap('tab20') # plasma
    aggregation_level = 'M'
    if data_type == 'Geo':
        # do Geo things
        possible_graphs = ['bar']
    else:
        # offer to do time things
        df['date'] = pd.to_datetime(df['date'])
        possible_graphs = ['pie', 'line', 'bar']
        aggregation_level = {"by Month": "M", "by Year": "Y"}.get(
            choose_from_dict({1: "by Month", 2: "by Year"}, 'Aggregation level'), "M")
    chart_type = choose_from_dict({i: x for i, x in enumerate(possible_graphs)}, 'Charts', testing=TESTING, test_return=TEST_VALS.pop(0))
    # TODO (P1): Add functions to treat data and create charts
    if chart_type == 'line' and data_type == 'Time':
        while True:
            to_show_df, method = resampleDateData(df, aggregation_level)
            if binaryResponse(f"Do you want to drop a few of the columns? Current columns: {to_show_df.columns}"):
                to_show_df = deselectColumns(to_show_df)
            if binaryResponse("Do you want to view everything in one plot (or in subplots)?"):
                subplots = False
                figsize = (20, 10)
            else:
                subplots = True
                figsize = (10, 2 * len(df.columns) + 1)
            title = title + f"\nAggregation: {aggregation_level}\nMethod: {method}"
            to_show_df.plot.line(subplots=subplots, figsize=figsize, colormap=colormap, title=title)
            plt.show()
            if not binaryResponse("Do you want to try a different aggregation method?"):
                break
    elif chart_type == 'bar':
        if data_type == 'Time':
            to_show_df, method = resampleDateData(df, aggregation_level)
        else:
            to_show_df = df
        if binaryResponse("Do you want to drop a few of the columns?"):
            to_show_df = deselectColumns(df)
        title = title + f"\nAggregation: {aggregation_level}"
        to_show_df.plot.bar(title=title).legend(loc="lower center")
        plt.show()
    elif chart_type == 'pie':
        if data_type == 'Time':
            to_show_df, method = resampleDateData(df, aggregation_level)
        else:
            to_show_df = df
        def rescaleRow(row):
            for i, item in row.iteritems():
                row[i] = item / row.sum()
            return row
        to_show_df = to_show_df.apply(rescaleRow, axis=1)


def dialog():
    # global path, aggregations, inspection_type, category
    if binaryResponse(
            "Do you want to use the normal comparison inspector for investigating combined scraping results?",
            testing=TESTING, test_return=TEST_VALS.pop(0)):
        path = file_finder()
        print(path)
        analyze(path)
    else:
        print(f"This version will guide you through the grouping of tag-level data")
        shortcode, country = getChosenCountry(action="inspect", testing=TESTING, test_return=TEST_VALS.pop(0))
        if binaryResponse("Do you want to manually set up the categories?", testing=TESTING,
                          test_return=TEST_VALS.pop(0)):
            cats = defineList()
            aggregations = getAggregations_Manual(country, cats)
        else:
            print(f"We'll use a source file then.")
            aggregations = getAggregations_File()
        inspection_type = choose_from_dict({1: "Time", 2: "Geo"}, testing=TESTING, test_return=TEST_VALS.pop(0))
        folder = os.path.join(FS.Aggregated, country)
        for category in aggregations:
            cc_files = os.listdir(folder)
            pop_list = []
            for i, f in enumerate(cc_files):
                if len(f.split("_")) < 3 or f.startswith("."):
                    pop_list.append(i)
                    print(
                        f"The file: {f} does not adhere to the required format. Please change the naming or remove it from the folder")
            for x in pop_list:
                cc_files.pop(x)
            if inspection_type == 'Time':
                cat_files = list(filter(lambda f: (
                        'Adjusted' not in f and inspection_type.lower() in f.lower() and f.split("_")[2] in
                        aggregations[category]), cc_files))
            else:
                # Geo_Germany_DE_['Klimaanlage einbauen']_98
                # DE_514_Schneider für die Braut_Geo.csv
                def filter_file(file):
                    # nonlocal aggregations, category
                    if 'Geo' not in file:
                        return False
                    else:
                        for tag in aggregations[category]:
                            if tag in file:
                                return True
                        return False

                cat_files = list(filter(filter_file, cc_files))
            split_region = {}
            for file in cat_files:
                region = file.split("_")[0]
                if split_region.get(region, False):
                    split_region[region].append(file)
                else:
                    split_region[region] = [file]
            aggregations[category] = split_region
        print(f"Starting visualization")
        while True:
            chosen_cat = choose_from_dict({i: k for i, k in enumerate(aggregations.keys())}, 'Categories',
                                          incl_end_option=True, end_description="End Inspection")
            if chosen_cat == "End Inspection":
                break
            else:
                while True:
                    chosen_region = choose_from_dict({i: k for i, k in enumerate(aggregations[chosen_cat].keys())},
                                                     'Regions', incl_end_option=True,
                                                     end_description="End Inspection for this category",
                                                     testing=TESTING, test_return=TEST_VALS.pop(0))
                    if chosen_region == "End Inspection for this category":
                        break
                    else:
                        files = aggregations[chosen_cat][chosen_region]
                        df = readInFiles(files, folder)
                        displayData(df, data_type=inspection_type,
                                    title=f"Category: {chosen_cat}\nRegion: {chosen_region}")


if __name__ == '__main__':
    dialog()
