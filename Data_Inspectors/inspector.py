if __name__ == '__main__':
    import sys
    sys.path.append('../')

import os
import sys
from pprint import pprint
import pandas as pd
from matplotlib import pyplot as plt

import utils.user_interaction_utils
from utils.misc_utils import getRegions
from utils.Countries import readInLocales, generateRegionIds
from utils.Filesys import generic_FileServer

FS = generic_FileServer


def getFile(country, tag, dimension, region_shortcode):
    region_shortcode = region_shortcode.upper()
    expected_file_location = os.path.join(FS.Aggregated, country)
    for filename in os.listdir(expected_file_location):
        components = filename.split("_")
        if components[0] == region_shortcode and components[3] == f"{dimension}.csv" and (
                components[1] == tag or components[2] == tag):
            return os.path.join(expected_file_location, filename)
    return None


def remap(value, maximum):
    v = round((value / maximum) * 100)
    return v


def getAvailableTags(country, short_code, dimension):
    short_code = short_code.upper()
    expected_file_location = os.path.join(FS.Aggregated, country)
    tags = {}
    for filename in os.listdir(expected_file_location):
        components = filename.split("_")
        if components[0] == short_code and components[3] == f"{dimension}.csv":
            tags[components[2]] = components[1]
    return tags


def displayFile(filepath, dimension, displayGroupedByYear=False, df=pd.DataFrame()):
    if df.empty:
        df = pd.read_csv(filepath, index_col=0)
    if dimension == 'Time':
        if 'means' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.resample('M', on='date').mean()
            max = df['means'].max()
            df['means'] = df['means'].apply(remap, maximum=max)
        # print(df)
        filename = filepath.split("/")[-1]
        if not displayGroupedByYear:
            df.plot.line(y='means', title=f"Line plot for {filename}")
        else:
            showGroupedByYear(df, title=f"Line plot for {filename}")
    elif dimension == 'Geo':
        sort_col = [col for col in df.columns if 'geoName' not in col][0]
        df = df.sort_values(by=sort_col)
        filename = filepath.split("/")[-1]
        kw = filename.split("_")[2]
        df.plot.barh(x='geoName', title=f"Bar Chart for {kw}")
    # df.plot(by='means', bins=20, title=f"Histogram for {filename}")
    plt.show()


def mergeRegions(country, tag):
    dimension = 'Time'
    region_shortcodes = generateRegionIds(country, sort=False)
    filepath = getFile(country, tag, dimension, region_shortcodes[0])
    df = pd.read_csv(filepath, usecols=[1, 2])
    df = df.rename(axis=1, mapper={'means': region_shortcodes[0]})
    for i in range(1, len(region_shortcodes)):
        region = region_shortcodes[i]
        filepath = getFile(country, tag, dimension, region)
        if filepath:
            new_df = pd.read_csv(filepath, usecols=[1, 2]).rename(axis=1, mapper={'means': region})
            df = df.merge(new_df, on='date')
    df['date'] = pd.to_datetime(df['date'])
    df = df.resample('M', on='date').mean()
    max = df.max().max()
    for col in df.columns:
        df[col] = df[col].apply(remap, maximum=max)
    return df


def showGroupedByYear(df, *args, **kwargs):
    grouped = df.groupby(df.index.year)
    for year, group in grouped:
        if 'means' in group.columns:
            idxmax = group.idxmax()
            max = group.loc[idxmax, 'means']
            idxmin = group.idxmin()
            min = group.loc[idxmin, 'means']
            print(f"\nMAX MIN FOR {year}")
            print(f"MAX: {max.index.month.values[0]} - Value: {max.values[0]}")
            print(f"MIN: {min.index.month.values[0]} - Value: {min.values[0]}")
            seasonality = max.values[0] / min.values[0] if min.values[0] > 0 else max.values[0] / 1
            print(f"Seasonality: {seasonality}x")
        if 'title' in kwargs:
            obj = {key: value for key, value in kwargs.items()}
            title = kwargs['title'] + " " + str(year)
            obj.pop('title', None)
        else:
            title = ''
        group.plot.line(subplots=True, figsize=(10, 2 * len(group.columns) + 1), title=title, **obj)
        plt.show()


def dialog():
    # global dimension
    while True:
        shortcode, country = utils.user_interaction_utils.getChosenCountry()
        if 'y' in input("Do you want to look at a region? (y/n)\n"):
            regions = getRegions(country, readInLocales())
            pprint(regions)
            region_id = input("What region do you want to look at? Input the code\n").strip()
            shortcode = shortcode + "-" + region_id
        while True:
            dimension = "Time" if input('What dimension do you want to look at? Time: 1 or Geo: 2\n') == '1' else 'Geo'
            if dimension == 'Geo' or dimension == 'Time':
                break
        pprint(getAvailableTags(country, shortcode, dimension))
        tag = input(
            'What tag do you want to view? Id or Full Name (input "help" for a list of available tags):\n').strip()
        file = getFile(country, tag, dimension, shortcode)
        if file is not None:
            break
        else:
            print("\n\n\n\nNo file available\n\n\n")
    if dimension == 'Geo':
        displayFile(file, dimension)
        sys.exit()
    displayGroupedByYear = utils.user_interaction_utils.binaryResponse("Should the data be grouped by YEAR?")
    if utils.user_interaction_utils.binaryResponse("Do you want to see all regions at once?"):
        df = mergeRegions(country, tag)
        df.to_csv(os.path.join(FS.Aggregated, country, f"{shortcode}_{tag}_ALL_REGIONS_Time.csv"))
        displayFile(file, dimension, displayGroupedByYear=displayGroupedByYear, df=df)
    else:
        displayFile(file, dimension, displayGroupedByYear=displayGroupedByYear)


if __name__ == '__main__':
    dialog()