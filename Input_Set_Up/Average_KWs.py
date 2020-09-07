if __name__ == '__main__':
    import sys
    sys.path.extend(['Users\simon\Desktop\ProntoTrends', "../", "./"])

import json
import os

import pandas as pd
from Datapipeline.mainProxy import getChosenCountries
from utils.misc_utils import lcol
from utils.user_interaction_utils import binaryResponse, choose_from_dict
from utils.Filesys import generic_FileServer

FS = generic_FileServer

"""
Finds the dominant keyword for tags based on Tags in a Tag_Keyword file
"""


def getMean(keyword, country_name, short_code):
    filepath = findOutFile(keyword, country_name, short_code, "Geo")
    if filepath:
        df = pd.read_csv(filepath)
        present_data = df[keyword]
        if len(present_data) < 16:
            other_regions = []
            for i in range(len(present_data), 16):
                new = 0
                other_regions.append(new)
            present_data.append(other_regions)
        avg = present_data.mean()
        return avg
    else:
        return -1


def findOutFile(keyword, country, region_code, dimension):
    expected_file_location = os.path.join(FS.Kwd_Level_Outs, keyword)
    expected_file_pattern = f"{dimension}_{country}_{region_code}_{keyword}_" if dimension == 'Time' else f"{dimension}_{country}_{keyword}_"
    for filename in os.listdir(expected_file_location):
        if expected_file_pattern in filename:
            return os.path.join(expected_file_location, filename)
    return None


if __name__ == "__main__":
    ccs_todo = getChosenCountries()
    out = []
    prefix = input("What is the prefix?\n").strip() + "_" if binaryResponse("Do you have a prefix in your filename?") else ""
    for short, country in ccs_todo:
        filepath = os.path.join(FS.Inputs, f"{prefix}Tag_Keyword_{short.upper()}.csv")
        df = pd.read_csv(filepath)
        obj = {}
        for i, row in df.iterrows():
            tag_in_obj = obj.get(row['tag'], False)
            if not tag_in_obj:
                obj[row['tag']] = {
                    'tag_id': row['tag_id'],
                    'kwds': [
                        row['keyword']
                    ]
                }
            else:
                obj[row['tag']]['kwds'].append(row['keyword'])
        res = {"Tags": {}}
        chosenKwds = []
        for key in obj:
            kwds = obj[key]['kwds']
            max_kw = ""
            max_val = 0
            for kwd in kwds:
                mean = getMean(kwd, country, short)
                mean = mean / (1 / len(kwd))
                out.append([key, kwd, mean])
                if mean > max_val and kwd.lower() not in chosenKwds:
                    max_val = mean
                    max_kw = kwd
            if len(max_kw) > 0:
                res['Tags'][key] = [max_kw]
                chosenKwds.append(max_kw.lower())

        df = pd.DataFrame(out, columns=["TAG", "Keyword", "Mean"])
        df.to_csv(os.path.join(FS.Inputs, f"ProntoPro_Trends_TAGS_SUMMARIES_{short.upper()}.csv"))

        if binaryResponse("Do you want to select the keyword for each tag manually?"):
            res['Tags'] = {}
            grouped = df.groupby("TAG")
            for name, group in grouped:
                sorted = group.sort_values(by='Mean', ascending=False)
                num_shown = 5
                while True:
                    d = sorted.iloc[num_shown-5: num_shown].to_dict('index')
                    for ind in d:
                        d[ind].pop('TAG')
                    d['more'] = 'Display more...'
                    d['skip'] = 'SKIP THIS TAG'
                    choice = choose_from_dict(d, f'keywords for {lcol.OKBLUE}{name}{lcol.ENDC}')
                    if choice == 'Display more...':
                        num_shown += 5
                    elif choice == 'SKIP THIS TAG':
                        print(f"{lcol.WARNING}Will not add {name} to file{lcol.ENDC}")
                        break
                    else:
                        res['Tags'][name] = [choice['Keyword']]
                        break
            with open(os.path.join(FS.Inputs, f"ProntoPro_Trends_Questions_{short.upper()}.json"), "w+") as f:
                f.write(json.dumps(res))

        else:
            with open(os.path.join(FS.Inputs, f"ProntoPro_Trends_Questions_{short.upper()}.json"), "w+") as f:
                f.write(json.dumps(res))

    print("FINISHED")
