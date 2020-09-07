import sys
if __name__ == '__main__':
    sys.path.extend(['Users\simon\Desktop\ProntoTrends', "../", "./"])

import os
import pandas as pd
import phpserialize as ps  # pip install phpserialize
import json
from utils.sql_utils import selectTagsFromDB
from utils.user_interaction_utils import binaryResponse, choose_from_dict, choose_multiple_from_dict, \
    chooseFile, defineList
from utils.Filesys import generic_FileServer
from utils.misc_utils import save_csv

FS = generic_FileServer


def deserializeItem(x):
    if x != x or x is None:
        return None
    x = x.encode()
    x = ps.loads(x)
    x = [x[y].decode() for y in x]
    return x


def serializeItem(col_val):
    arr = col_val.split("|")
    return ps.dumps(arr)


def generateKeywordsFile(df, prefix_out='', cc='IT'):
    tag_kwd = []
    keywords = set()
    keyword_ids = []
    grouped = df.groupby(by=[df.columns[0], df.columns[1]])
    for name, group in grouped:
        kwd_ids = 1
        tag_id, tag_name = name
        for ind, row in group.iterrows():
            columns = group.columns[2:]
            for col in columns:
                val = row[col]
                if isinstance(val, str):
                    val = [val]
                elif isinstance(val, int) or isinstance(val, float) or val is None:
                    continue
                for x in val:
                    if x not in keywords and x.lower() not in keywords:
                        kwd_id = f"{tag_id}-{kwd_ids}"
                        kwd_ids += 1
                        keywords.add(x)
                        tag_kwd.append([tag_id, kwd_id, tag_name, x])
                        keyword_ids.append([kwd_id, x])
                    else:
                        continue
    keywords_df = pd.DataFrame(keyword_ids, columns=['kwd_id', 'Keyword'])
    tag_kwd_df = pd.DataFrame(tag_kwd, columns=["tag_id", "kwd_id", "tag", "keyword"])
    prefix_out = f"{prefix_out}_" if len(prefix_out) > 0 else ''
    saveLocation = os.path.join(FS.Inputs, f"{prefix_out}Keywords_{cc}.csv")
    save_csv(keywords_df, saveLocation, index=False)
    save_csv(tag_kwd_df, os.path.join(FS.Inputs, f"{prefix_out}Tag_Keyword_{cc}.csv"), index=False)


def read_php_csv(filename):
    df = pd.read_csv(filename)
    print(df.head())
    df = apply_php_deserialization(df)
    print(df.head())
    return df


def apply_php_deserialization(df):
    df['elite_keywords'] = df['elite_keywords'].apply(deserializeItem)
    df['top_keywords'] = df['top_keywords'].apply(deserializeItem)
    return df


def generateComparisonsFile(df, country):
    cats = defineList()
    selections = {}
    tags = deduplicatedTagsByName(df)
    for cat in cats:
        selections[cat] = choose_multiple_from_dict(tags, 'tags', request_description=f'Please select the Tags you want to add to the category {cat}')
    final = {}
    for cat, options in selections.items():
        cat_kwds = {}
        for option in options:
            option_kwds = []
            rows = df.query('tag_name == @option')
            dictionary = rows.to_dict(orient='list')
            for key, vals in dictionary.items():
                if key in ['tag_name', 'tag_id']:
                    continue
                else:
                    for val in vals:
                        if not isinstance(val, list):
                            if not val is None and not pd.isna(val) and val != 'None':
                                option_kwds.append(val)
                        else:
                            for item in val:
                                option_kwds.append(item)
            cat_kwds[option] = list(set(option_kwds))
        final[cat] = cat_kwds
    s = json.dumps(final)
    path = os.path.join(FS.Inputs, f'ProntoPro_Trends_Questions_{country.upper()}.json')
    with open(path, 'w+') as f:
        f.write(s)
        print(f"Saved file {path}")
    return final


def deduplicatedTagsByName(df):
    tags = {}
    for ind, row in df.iterrows():
        tags[ind] = row['tag_name']
    present = []
    to_pop = []
    for k, v in tags.items():
        if v in present:
            to_pop.append(k)
        else:
            present.append(v)
    for i in to_pop:
        tags.pop(i)
    return tags


if __name__ == '__main__':
    country = choose_from_dict({1: 'IT', 2: 'DE', 3: 'ES', 4: 'FR', 5: 'AT', 6: 'CH'}, 'Countries')
    if binaryResponse("Do you have a sourcefile to get Tag information from?"):
        filename = chooseFile(filetype=".csv")
        df = read_php_csv(filename)
    else:
        if binaryResponse("Do you want to search using Tag_Ids (y) or using Contains-Match in all text fields (n)?"):
            use_ids = True
        else:
            use_ids = False
        kwds = None
        tag_ids = None
        if use_ids:
            tag_ids = defineList(request_text="Please input the tag ids to use", wanted_type='int')
        else:
            kwds = defineList(wanted_type='str', request_text="Please input the Contains-Match keywords to use")
        df = selectTagsFromDB(country.lower(), kwds=kwds, tag_ids=tag_ids)
        df = apply_php_deserialization(df)
    c = choose_from_dict(['CSV to scrape Keywords individually', 'JSON file for Comparison', 'Both'], request_description='What type of files do you want to create?', label='actions')

    if c == 'CSV to scrape Keywords individually' or c == 'Both':
        prefix = ""
        if binaryResponse("Do you want to add a prefix to the filename?"):
            prefix = input("Please type the prefix:\n").strip()
        generateKeywordsFile(df, prefix_out=prefix, cc=country)
    if c == 'JSON file for Comparison' or c == 'Both':
        generateComparisonsFile(df, country)

