if __name__ == '__main__':
    import sys

    sys.path.append('../')

import os
from typing import Union, Tuple, Dict, List, Any

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from finalCSVgenerator import Sort
from utils.misc_utils import lcol, reverseDict, getDirectory, rescale_comparison, save_csv
from utils.user_interaction_utils import binaryResponse, choose_from_dict, choose_multiple_from_dict, \
    chooseFolder, chooseFile, choose_column, defineList
from utils.Filesys import generic_FileServer as FS

TESTING = True
if TESTING and __name__ == '__main__':
    print(
        f"{lcol.WARNING}You are in testing mode. If you don't want this, please change the setting in the file{lcol.ENDC}")


# TODO: Create presets


def calculateScalar(row, tag_maxes):
    tag = row.name[0]
    max = tag_maxes.loc[tag, 'Count']
    return row['Count'] / max


def createBase():
    merges = [
        ["Dj per matrimoni", "Musica per matrimonio"],
        ["wedding planner", "organizzatore di matrimoni"]
    ]
    df = pd.read_csv("../misc/Wedding_Tag_Requests_IT copy.csv", header=0,
                     names=['Tag', 'Region', 'Year', 'Month', 'Count'])
    df = df[df.Year > 2017]
    for merge in merges:
        item1, item2 = merge
        df_with_select = df.query("(Tag==@item1) | (Tag == @item2)")
        df_without = df.query("(Tag!=@item1) & (Tag != @item2)")
        grouped = df_with_select.groupby(['Region', 'Year', 'Month']).sum()
        grouped = grouped.reset_index()
        grouped['Tag'] = item1
        df = pd.concat([df_without, grouped], ignore_index=True)

        print(df.head())

    df['Month'] = df['Month'].apply(lambda x: int(x - 1))
    # across all years
    sum_tag_region = df[['Tag', 'Region', 'Count']].groupby(['Tag', 'Region']).sum()
    max_tag_region = df[['Tag', 'Region', 'Count']].groupby(['Tag', 'Region']).max()
    tag_maxes = sum_tag_region.groupby(['Tag']).max()
    sum_tag_region['scalar'] = sum_tag_region.apply(calculateScalar, args=(tag_maxes,), axis=1)
    print(f"Calculating Index")
    tag_month = df[['Tag', 'Year', 'Month', 'Count']].groupby(by=['Tag', 'Year', 'Month']).sum()
    print(tag_month)
    df['INDEX'] = df.apply(
        lambda row: round(row.Count / max_tag_region.loc[(row.Tag, row.Region), 'Count'] * sum_tag_region.loc[
            (row.Tag, row.Region), 'scalar'], 2) if sum_tag_region.loc[
                                                        (row.Tag, row.Region), 'Count'] > 100 else np.NaN,
        axis=1)
    df['Country_chosen'] = 0
    tag_month_max = tag_month.reset_index()[['Tag', 'Count']].groupby("Tag").max()
    print("Working on IT")
    italy = tag_month.copy()
    italy = italy.reset_index()

    def setRow(row, tag_month_max):
        if row.Tag:
            maximum = tag_month_max.loc[row.Tag, 'Count']
            row['INDEX'] = row[
                               'Count'] / maximum  # scales relative to the max value for a given tag (summed across all regions)
            row['Country_chosen'] = 1
            row['Region'] = 'Italia'
        return row

    italy = italy.apply(setRow, args=(tag_month_max,), axis=1)
    df = pd.concat([df, italy], ignore_index=True)
    print(df)
    incl_CC = df.query('Country_chosen == 0')
    print("Adapting data for Conjunction with CC")

    def rescaleRow(row, sum_across_regions):
        summe = sum_across_regions.query("(Tag == @row.Tag) & (Year == @row.Year) & (Month == @row.Month)")[
            'Count'].sum()
        if row['INDEX'] != 'NA' and pd.notna(row['INDEX']):
            intermediate = row['Count'] / summe
            if intermediate >= 1:
                print("Hello")
            row['INDEX'] = intermediate
            # row['Count'] = row['Count'] / 100
        row['Country_chosen'] = 1
        return row

    incl_CC = incl_CC.apply(rescaleRow, args=(italy,), axis=1)
    final_df = pd.concat([df, incl_CC])
    print("Fixing a few last things")
    final_df = final_df.sort_values(by=['Country_chosen', 'Tag', 'Region', 'Year', 'Month'], ascending=True)
    final_df = final_df.round(2)
    final_df = final_df.rename(
        columns={'Tag': 'ticket_taxonomy_tag_name', 'Region': 'ticket_geo_region_name', 'Count': 'No_of_tickets',
                 'INDEX': 'Index'})
    # final_check = final_df.groupby(['ticket_taxonomy_tag_name', 'Country_chosen'])['Index'].max()
    # compare_vals = final_df.groupby(
    #     ["ticket_taxonomy_tag_name", 'Year', 'Month', 'ticket_geo_region_name', 'Country_chosen']).max()
    # compare_vals = compare_vals.reset_index().groupby(["ticket_taxonomy_tag_name", 'ticket_geo_region_name', 'Year', 'Month'])
    final_df = final_df.fillna('NA')
    final_df.to_csv("FINAL_CHART_ITALY.csv", index=False)
    print(final_df)
    print(f"Saved file")
    ensureEnoughRows()
    fixNas()
    checkDF()


def checkDF():
    final_df = pd.read_csv("FINAL_CHART_ITALY.csv")
    # final_check = final_df.groupby(['ticket_taxonomy_tag_name', 'Country_chosen'])['Index'].max()
    # compare_vals = final_df.groupby(
    #     ["ticket_taxonomy_tag_name", 'Year', 'Month', 'ticket_geo_region_name', 'Country_chosen']).max()
    # compare_vals = compare_vals.reset_index().groupby(
    #     ["ticket_taxonomy_tag_name", 'ticket_geo_region_name', 'Year', 'Month'])
    pre = final_df[final_df.Country_chosen == 0].groupby(
        ["ticket_taxonomy_tag_name", 'Year', 'Month', 'ticket_geo_region_name', 'Country_chosen']).sum()
    post = final_df.query("Country_chosen == 1 & ticket_geo_region_name!='Italia'").groupby(
        ["ticket_taxonomy_tag_name", 'Year', 'Month', 'ticket_geo_region_name', 'Country_chosen']).sum()
    # diff = post.apply(lambda x: x['Index'] - pre.loc[x.name[:4], 'Index'], axis=1)
    # print(diff)
    # m = diff.mean()
    # max = diff.max()
    # min = diff.min()
    # sum = diff.sum()
    # print(diff)
    toGraph = final_df.query("ticket_taxonomy_tag_name == 'Wedding planner' & ticket_geo_region_name == 'Lombardia'")
    toGraph['x'] = toGraph.apply(lambda x: f"{x['Year']}_{x['Month']}", axis=1)
    toGraph.groupby(['ticket_geo_region_name', 'Country_chosen']).plot(x="x", y='Index', subplots=True)
    summe = final_df.query("Country_chosen == 1 & ticket_geo_region_name!='Italia'").groupby(
        ["ticket_taxonomy_tag_name", 'Year', 'Month', 'Country_chosen']).sum()
    # summe.reset_index().head(1000).plot.bar(x='ticket_taxonomy_tag_name', y='Index')
    plt.show()
    italy = final_df.query("Country_chosen == 1 & ticket_geo_region_name == 'Italia'")
    print(summe)
    for ind, row in summe.iterrows():
        query = italy.query(
            "ticket_taxonomy_tag_name == @ind[0] & Year == @ind[1] & Month == @ind[2] & Country_chosen == @ind[3]")[
            'Index']
        iloc_ = query.iloc[0]
        if row['Index'] != iloc_:
            print(f"Sum: {row['Index']} is not equal to {iloc_}")
            print(row)


def ensureEnoughRows():
    print("Ensuring enough rows")
    df = pd.read_csv("FINAL_CHART_ITALY.csv")
    tags = df['ticket_taxonomy_tag_name'].unique().tolist()
    regions = df['ticket_geo_region_name'].unique().tolist()
    year_months = df[['Year', 'Month']].drop_duplicates()
    combinations = []
    expected_counts = {}
    for country_selected in range(2):
        for tag in tags:
            for region in regions:
                for ind, row in year_months.iterrows():
                    year = row['Year']
                    month = row['Month']
                    if region == 'Italia' and country_selected != 1:
                        continue
                    else:
                        for item in [country_selected, tag, region, year, month]:
                            if expected_counts.get(item, False):
                                expected_counts[item] += 1
                            else:
                                expected_counts[item] = 1
                        combinations.append({
                            "Country_chosen": country_selected,
                            "ticket_taxonomy_tag_name": tag,
                            "ticket_geo_region_name": region,
                            "Year": year,
                            "Month": month
                        })
    for col in ['ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month', 'Country_chosen']:
        counts = df[col].value_counts()
        for ind, item in counts.iteritems():
            expected = expected_counts.get(ind, False)
            if expected:
                if expected <= item:
                    for i, combination in enumerate(combinations):
                        if combination.get(col, "") == ind:
                            combinations.pop(i)
    print("Checking which additions are necessary")
    additions = []
    for combination in combinations:
        tag = combination['ticket_taxonomy_tag_name']
        country_selected = combination['Country_chosen']
        region = combination['ticket_geo_region_name']
        year = combination['Year']
        month = combination['Month']
        query = df.query(
            "(ticket_taxonomy_tag_name==@tag) & (Country_chosen==@country_selected) & (ticket_geo_region_name==@region) & (Year==@year) & (Month==@month)")
        if query.shape[0] == 0:
            print(f"Need to add {[tag, region, year, month, 0, 0, country_selected]}")
            additions.append([tag, region, year, month, 0, 0, country_selected])
    print("Adding additions")
    addition = pd.DataFrame(additions, columns=['ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month',
                                                'No_of_tickets', 'Index', 'Country_chosen'])
    final_df = pd.concat([df, addition], ignore_index=True)

    final_df = final_df.sort_values(
        by=['Country_chosen', 'ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month'],
        ascending=True).fillna('NA')
    final_df = final_df.round(2)
    print("Done")
    final_df.to_csv("FINAL_CHART_ITALY.csv", index=False)
    print("Saved it all")


def generateTop5():
    df = pd.read_csv(
        "/Google_Trends/FINAL/Italy/Wed/Wed_Chart_Data_Italy.csv")
    df = df.query("ticket_geo_region_name == 'Italia'")
    tags = df['ticket_taxonomy_tag_name'].unique().tolist()
    regions = ['Italia']  # df['ticket_geo_region_name'].unique().tolist()
    years = df['Year'].unique().tolist()
    grouped = df.groupby(['ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year'])
    year_highs = {region: {year: [] for year in years} for region in regions}
    year_entries = {region: {year: {tag: [] for tag in tags} for year in years} for region in regions}
    out = []
    for name, group in grouped:
        tag, region, year = name
        max = group.max()
        max_id = group['Index'].idxmax()
        min_id = group['Index'].idxmin()
        if pd.notna(min_id):
            max_val = group.loc[max_id, 'Index']
            avg_val = group['Index'].mean()
            min_val = group.loc[min_id, 'Index']
            seasonality = round(max_val / min_val, 2) if min_val > 0 else round(max_val / 1, 2)
            max_m = group.loc[max_id, 'Month'] if (max_val > 0) else 'NA'
            min_m = group.loc[min_id, 'Month'] if (max_val > 0) else 'NA'
            seasonality = seasonality if max_val > 0 else 'NA'
            if region in regions:
                year_entries[region][year][tag] = [region, tag, year, 0, max_m, min_m, seasonality]
                year_highs[region][year].append((tag, avg_val))

    for region in year_highs:
        for year in year_highs[region]:
            for i, item in enumerate(Sort(year_highs[region][year], True)):
                if i < 5:
                    tag = item[0]
                    entry = year_entries[region][year][tag]
                    entry[3] = i + 1
                    out.append(entry)
    final_df = pd.DataFrame(out,
                            columns=["ticket_geo_region_name", "ticket_taxonomy_tag_name", "year", "Rank", "Max", "Min",
                                     'Demand_factor_max_to_min'])
    print(final_df)
    final_df = final_df.sort_values(by=['year', 'ticket_geo_region_name', 'Rank'], ascending=True).round(2).fillna('NA')
    final_df.to_csv("TOP5_TAGS_IT.csv", index=False)


def fixNas():
    df = pd.read_csv("FINAL_CHART_ITALY.csv")
    # prev = {
    #     'isNa': False,
    #     'vals': []
    #         }
    # for i, row in df.iterrows():
    #     if prev['isNa']:
    #         if row.iloc[0] == prev['vals'][0] and row.iloc[1] == prev['vals'][1]:
    #             df.loc[i, 'Index'] = "NA"
    #             prev['isNa'] = True
    #             prev['vals'] = row.tolist()
    #         else:
    #             if row['Index'] == 'NA' or pd.isna(row['Index']):
    #                 prev['isNa'] = True
    #                 prev['vals'] = row.tolist()
    #             else:
    #                 prev['isNa'] = False
    #                 prev['vals'] = row.tolist()
    #     else:
    #         if row['Index'] == 'NA' or pd.isna(row['Index']):
    #             prev['isNa'] = True
    #             prev['vals'] = row.tolist()
    grouped = df.groupby(['ticket_taxonomy_tag_name', 'ticket_geo_region_name'])
    for name, group in grouped:
        series = group.Index
        if series.hasnans:
            # group['Index'] = 'NA'
            tag = name[0]
            region = name[1]
            query = df.query('ticket_taxonomy_tag_name == @tag & ticket_geo_region_name == @region')
            query['Index'] = 'NA'
            # query_out = df.query('ticket_taxonomy_tag_name != @tag & ticket_geo_region_name != @region')
            # df = pd.concat([query, query_out])
            df.update(query)
    df = df.sort_values(by=['Country_chosen', 'ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month'])
    df['Index'] = df['Index'].fillna('NA')
    df.to_csv("FINAL_TEST.csv", index=False)


def convert_region_names_to_google(df: pd.DataFrame, is_geo=False):
    d_eng = {
        "Abruzzo": "IT-65",
        "Aosta": "IT-23",
        "Apulia": "IT-75",
        "Basilicata": "IT-77",
        "Calabria": "IT-78",
        "Campania": "IT-72",
        "Emilia-Romagna": "IT-45",
        "Friuli-Venezia Giulia": "IT-36",
        "Lazio": "IT-62",
        "Liguria": "IT-42",
        "Lombardy": "IT-25",
        "Marche": "IT-57",
        "Molise": "IT-67",
        "Piedmont": "IT-21",
        "Sardinia": "IT-88",
        "Sicily": "IT-82",
        "Trentino-Alto Adige/South Tyrol": "IT-32",
        "Tuscany": "IT-52",
        "Umbria": "IT-55",
        "Veneto": "IT-34",
        "Italy": "IT"
    }
    regions_remap = {
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
    region_it_id = {}
    for eng, id in d_eng.items():
        it = regions_remap.get(eng, eng)
        region_it_id[it] = id
    if not is_geo:
        df = df.replace(to_replace=region_it_id)
    else:
        def remap(row, regions_remap):
            row['ticket_geo_region_name'] = reverseDict(regions_remap).get(row['ticket_geo_region_name'],
                                                                           row['ticket_geo_region_name'])
            return row

        df = df.apply(remap, args=(regions_remap,), axis=1)
    return df


def make_uniform(df: pd.DataFrame, group_uniques: Dict[str, List[Any]], index_column):
    """
    Ensures that the dataframe has sufficient rows to be "square"
    Args:
        df:
        group_uniques:
        index_column:

    Returns:

    """
    l = list(group_uniques.keys())
    df = df.set_index(keys=l, drop=True)
    new_index = pd.MultiIndex.from_product(list(group_uniques.values()), names=l)
    df = df.reindex(new_index)
    df.reset_index(inplace=True)
    for x in [index_column, 'means']:
        df[x] = df[x].fillna(0)
    solved = l + [index_column, 'means']
    other_cols = [col for col in df.columns if col not in solved and col != 'date']
    for col in other_cols:
        val = df[col].unique().tolist()[0]
        if pd.isna(val):
            val = df[col].unique().tolist()[1]
        df[col] = df[col].fillna(val)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df = df.sort_values(by=l)
    mask = (df['date'] > '2017-12-31') & (df['date'] <= pd.Timestamp.today())
    df = df.loc[mask]
    return df


def treat_and_save(group, name, q, index_col, min_tickets, grouping_uniques, saving_folder, has_date):
    print(name, group.shape)
    # print(group)
    group_max = group[index_col].max()
    group_sum = group[index_col].sum()
    if group_sum < min_tickets:
        return
    group['means'] = group[index_col].apply(lambda x: (x / group_max) * 100)
    print("Adjusting number of rows based on grouping")
    group = make_uniform(group, grouping_uniques, index_col)
    group = convert_region_names_to_google(group)
    print(group.head())
    file_name, group, q = make_file_name(group, has_date, q)
    further_to_drop = ['No_of_tickets'] if not has_date else ['No_of_tickets', 'year', 'month', 'day']
    if 'Unnamed: 0' in group.columns:
        further_to_drop.append('Unnamed: 0')
    group = group.drop(columns=further_to_drop)
    save_csv(group, os.path.join(saving_folder, file_name), index=False)


def make_file_name(group, has_date, q, type_data='Time'):
    name_items = ['ticket_geo_region_name', 'tag_id', 'ticket_taxonomy_tag_name']
    file_name = ""
    for i in name_items:
        try:
            x = group[i].unique().tolist()[0]
            if isinstance(x, float):
                x = int(x)
            elif "/" in x:
                x = x.replace("/", " o ")
            file_name += f"{x}_"
            group = group.drop(columns=[i])
        except:
            file_name += f"-{q}_"
            q += 1
    file_name += type_data if type_data else input("What is the type of the data?") if not has_date else "Time"
    file_name += ".csv"
    return file_name, group, q


def renameColumns(df, file):
    renames = {}
    print(df.head())
    for col in df.columns:
        if binaryResponse(f"Do you want to rename the column '{col}'?"):
            renames[col] = input(f"What should the column '{col}' be called?\n").strip()
    if len(renames.keys()) > 0:
        df = df.rename(columns=renames)
        if binaryResponse("Do you  want to save the changes to the original file?"):
            df.to_csv(file, index=False)
            print(f"Saved")
    else:
        print("Didn't change anything")
    return df


def formatDate(df: pd.DataFrame):
    has_date = True
    if not 'date' in df.columns:
        options = {i: k for i, k in enumerate(df.columns)}
        options['None'] = 'None'
        components = ["year", "month", "day"]
        exchanges = []
        for item in ["year", "month", "day"]:
            if item in options.values():
                continue
            c = choose_from_dict(options, "Columns", request_description=f"What column represents the {item}?")
            if c == 'None':
                if not item == 'day':
                    raise ValueError(f"Cannot handle missing {item} column yet")  # fixme
                else:
                    df[item] = 1
            else:
                df = df.rename(columns={c: item})
                exchanges.append(c)
        df['date'] = pd.to_datetime(df[components])
        df = df.drop(columns=exchanges)
    return df, has_date


def group_split_col(split_file_col: list):
    """
    Allows the user to group columns that the files will be split up by together - e.g. to keep tag_ids and tag_names associated (e.g. for file naming)

    Args:
        split_file_col: list -- the list of the columns used to distinguish the files

    Returns: list of lists -- grouped by logical rules (same amount of groups as cols -> 1 per group) or manually

    """
    num_groups = len(split_file_col)
    if len(split_file_col) > 2:
        if binaryResponse(
                "Do you want to group the columns by which files will be aggregated?\nBy grouping you can allow two columns that co-occur, like a tag_name and a tag_id. Otherwise each selected item will be treated individually"):
            while True:
                try:
                    num_groups = int(input("How many groups do you want?"))
                    if num_groups > len(split_file_col) or num_groups <= 1:
                        print("This is invalid")
                    else:
                        break
                except Exception as e:
                    print(e)
    if num_groups == len(split_file_col):
        out = [[item] for item in split_file_col]
    else:
        out = []
        for i in range(num_groups):
            while True:
                choices = []
                if len(split_file_col) > 1:
                    if len(split_file_col) == num_groups - i - 1:
                        choices = split_file_col[0]
                    else:
                        choices = choose_multiple_from_dict({i: x for i, x in enumerate(split_file_col)}, "Columns",
                                                            request_description=f"Choose which of the columns you want to add to group {i + 1}?")
                if len(choices) + (num_groups - i - 1) < len(split_file_col):
                    print("The amount of chosen items is more than is possible. Please try again")
                else:
                    split_file_col = list(filter(lambda x: x not in choices, split_file_col))
                    break
            out.append(choices)
    return out


def form_pandas_query(column: str, options: List[str]) -> str:
    subqueries = []
    for option in options:
        subqueries.append(f"{column} == '{option}'")
    return " | ".join(subqueries)


def treatDBData():
    file = chooseFile(filetype="csv", testing=True,
                      test_return="/Users/chris/PycharmProjects/ProntoTrends/Input_Files/IT_Summer_Ticket_Counts.csv")
    df = pd.read_csv(file)
    print(f"Here are the columns in the selected file: {df.columns}")
    if binaryResponse("Do you want to rename them?", testing=True, test_return=False):
        df = renameColumns(df, file)
    print(f"{lcol.OKGREEN}Please select the folder where the outputs should be saved{lcol.ENDC}")
    saving_folder = chooseFolder(testing=True, test_return=os.path.join(FS.Aggregated, 'Italy'))
    pivot_col = choose_column(df,
                              instruction_str=f"{lcol.OKGREEN}Please choose the column to pivot file by from the following:{lcol.ENDC}",
                              testing=True, test_return='ticket_taxonomy_tag_name')
    grouping: list = choose_column(df, instruction_str='Please choose the columns used for grouping',
                                   testing=True, test_return=["year", 'month'], allow_multiple=True)
    if binaryResponse("Do you want to generate a pivot file (or individual files per Category)?"):
        # do pivot stuff

        categories = defineList(['Sum'])  # TODO (p1): Allow json input
        selections = {}
        tags = df[pivot_col].unique().tolist()
        for cat in categories:
            if len(categories) == 1:
                selections[cat] = tags
            else:
                selected_tags = choose_multiple_from_dict(tags, 'tags',
                                                          request_description=f'Please select the Tags you want to add to the category {cat}')
                selections[cat] = selected_tags
                tags = [tag for tag in tags if tag not in selected_tags]
        to_drop = []
        dimension = 'Time'
        if binaryResponse("Does the file include date data?", testing=TESTING, test_return=True):
            df, has_date = formatDate(df)
            index = ['date']
            index = index + choose_column(df,
                                          instruction_str=f"Do you want to use another column next to {index} as the index for the new pivot table?\nIf so, select it or otherwise select End",
                                          exclude=index, allow_multiple=True)
            to_drop = ['year', 'month', 'day']
        else:
            index = choose_column(df, instruction_str='Please choose the index column to use')
        to_drop = to_drop + choose_column(df,
                                          instruction_str="Which of the columns do you want to exclude from the pivot table?",
                                          allow_multiple=True, exclude=to_drop + index)
        df = df.drop(columns=to_drop)
        for category, items in selections.items():
            df_new = df.query(form_pandas_query(pivot_col, items))
            directory = getDirectory(['Output_Files', 'comparisons', category])
            pivot = df_new.pivot_table(index=index, columns=pivot_col, aggfunc=sum, fill_value=0)
            pivot.reset_index()
            grouped = pivot.groupby(index[-1])
            for name, group in grouped:
                print(group)
                # group = group.rename(columns=lambda x: x.split("/")[1] if isinstance(x, str) else x[1])
                group = rescale_comparison(group, scale=100)
                group = group.reset_index()
                group = convert_region_names_to_google(group)
                region_id = group[index[-1]].unique().tolist()[0]
                filename = f"{dimension}_Italy_{region_id}_{category}.csv"
                group = group.drop(columns=[index[-1]])
                cols = list(map(lambda col: col[0] if len(col[1]) == 0 else col[1], group.columns))
                group.columns = cols
                save_csv(group, os.path.join(directory, filename), index=False)
            # create Italy
            italy = pivot.groupby(index[0]).sum()
            italy = rescale_comparison(italy, scale=100)
            italy = italy.reset_index()
            cols = list(map(lambda col: col[0] if len(col[1]) == 0 else col[1], italy.columns))
            italy.columns = cols
            region_id = 'IT'
            filename = f"{dimension}_Italy_{region_id}_{category}.csv"
            save_csv(italy, os.path.join(directory, filename), index=False)
    else:
        # create individual files
        split_file_col = choose_column(df,
                                       instruction_str=f"{lcol.OKGREEN}Please choose the column to break up the file by from the following:{lcol.ENDC}",
                                       testing=TESTING, test_return=['ticket_geo_region_name'])
        if split_file_col in grouping:
            grouping.pop(grouping.index(split_file_col))
        splits = []
        for item in [split_file_col, pivot_col]:
            if isinstance(item, list):
                for i in item:
                    splits.append(i)
            else:
                splits.append(item)
        splits = list(set(splits))
        grouping_uniques = {}
        for col in grouping:
            grouping_uniques[col] = df[col].unique().tolist()
        has_date = False
        if binaryResponse("Does the file include date data?", testing=TESTING, test_return=True):
            df, has_date = formatDate(df)
        index_col = choose_column(df,
                                  instruction_str=f"{lcol.OKGREEN}Please choose the column that shows the distribution:{lcol.ENDC}",
                                  testing=TESTING, test_return='No_of_tickets', exclude=splits)
        grouped = df.groupby(by=splits, as_index=True)
        # splits = group_split_col(splits)
        min_tickets = 30  # TODO (P3): Add possibility to choose min_tickets
        q = 0
        for name, group in grouped:
            treat_and_save(group, name, q, index_col, min_tickets, grouping_uniques, saving_folder, has_date)

        handle_Italy_data(df, grouping, grouping_uniques, has_date, index_col, min_tickets, pivot_col, q, saving_folder,
                          split_file_col)

        # create geo data
        if "ticket_geo_region_name" in df.columns:
            has_tag_id = 'tag_id' in df.columns
            grouped = df[df.year == 2019].groupby(["ticket_taxonomy_tag_name"])
            for name, group in grouped:
                group = group.drop(columns=['year', 'month', 'day', 'date'])
                summed = group.groupby(['ticket_geo_region_name']).sum()
                summed['means'] = summed[index_col].apply(lambda x: x * 100 / summed[index_col].max())
                if has_tag_id:
                    summed['tag_id'] = group['tag_id'].unique().tolist()[0]
                summed = summed.drop(columns=['No_of_tickets'])
                summed = summed.reset_index()
                summed = convert_region_names_to_google(summed, is_geo=True)
                summed = summed.rename(columns={'ticket_geo_region_name': 'geoName'})
                tag_name = name if "/" not in name else name.replace("/", " o ")
                tag_id = summed['tag_id'].unique().tolist()[0] if has_tag_id else "-0"
                filename = f"IT_{tag_id}_{tag_name}_Geo.csv"
                summed = summed[["geoName", "means"]]
                summed.to_csv(os.path.join(saving_folder, filename))
                print(f"Saved file {filename}")


def handle_Italy_data(df, grouping, grouping_uniques, has_date, index_col, min_tickets, pivot_col, q, saving_folder,
                      split_file_col):
    grouped = df.groupby(pivot_col)
    for name, group in grouped:
        if not isinstance(split_file_col, list):
            split_file_col = [split_file_col]
        order = grouping + [col for col in df.columns if col not in grouping]
        group = group[order]
        keep_cols = group.copy()
        group = group.groupby(grouping, as_index=True).sum()
        group = group.reset_index()
        diff = [kc for kc in keep_cols.columns if kc not in group.columns]
        has_tag_id = False
        if 'tag_id' in df.columns:
            has_tag_id = True
            diff.append('tag_id')
        keep_cols = keep_cols[diff]
        # keep_cols = keep_cols.set_index(grouping)  # np.arange(0, group.shape[0]))
        # group = group.set_index(grouping)
        # group = pd.concat([keep_cols, group], axis=1)
        split_file_col = ["ticket_geo_region_name", "ticket_taxonomy_tag_name", 'tag_id'] if has_tag_id else ["ticket_geo_region_name", "ticket_taxonomy_tag_name"]
        for c in split_file_col:
            if c == 'ticket_geo_region_name':
                group[c] = "Italia"
            else:
                group[c] = keep_cols[c].unique().tolist()[0]
        group = group.dropna()
        group, _ = formatDate(group)
        treat_and_save(group, name, q, index_col, min_tickets, grouping_uniques, saving_folder, has_date)

        # TODO (P3): Add possibility to design File Names: set_up pattern (e.g. what comes at what point?)


def dialog():
    choice = choose_from_dict(
        {1: 'createBase', 2: "ensureEnoughRows", 3: 'Top5', 4: 'fixNas', 5: 'checkBase', 6: 'createFiles'},
        'Actions')
    # choice = 'checkBase'
    if choice == 'createBase':
        createBase()
    elif choice == 'ensureEnoughRows':
        ensureEnoughRows()
    elif choice == 'Top5':
        generateTop5()
    elif choice == 'fixNas':
        fixNas()
    elif choice == 'checkBase':
        checkDF()
    elif choice == 'createFiles':
        treatDBData()
    else:
        pass


if __name__ == '__main__':
    dialog()
