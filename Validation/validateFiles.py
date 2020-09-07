if __name__ == '__main__':
    import sys
    sys.path.append('../')

import os
import sys
import argparse

import pandas as pd

from utils.misc_utils import lcol
from utils.user_interaction_utils import getChosenCountry, binaryResponse, choose_from_dict, \
    choose_multiple_from_dict, chooseFolder
from Validation.validation_rules import rules
from utils.Filesys import generic_FileServer

FS = generic_FileServer

parser = argparse.ArgumentParser(description='Validate files for ProntoTrends')
parser.add_argument('-folder', help="The folder to validate files in", default=False, metavar='folder_path',
                    nargs='*')

file_name_remap = {
    "Ceremony": "ceremony-preferences.csv",
    "Map": "interest-map.csv",
    "Table": "requests-detail.csv",
    "Chart_Data": "requests-trend.csv",
    "Services_": "services.csv",
    "Spend": "spending.csv",
    "Top5": "top-5-services.csv",
    "Main": "top-choices.csv",
    "Reception": "top-locations.csv"
}


def validateColumns(df: pd.DataFrame, desired_columns):
    if len(desired_columns) != len(df.columns):
        print(
            f"{lcol.WARNING}Difference in number of columns ({len(df.columns)}) vs. expected ({len(desired_columns)}).{lcol.ENDC}")
        print(f"Current: {df.columns}\nDesired: {desired_columns}")
        return False, "Mismatched length of columns"
    for col_des, col_real in zip(desired_columns, df.columns):
        if col_des != col_real:
            print(
                f"{lcol.WARNING}Instead of the desired column '{col_des}' the column '{col_real}' appears in the file{lcol.ENDC}")
            print(f"{lcol.WARNING}Column '{col_des}' is not matched{lcol.ENDC}")
            return False, f"Column is not matched"
    return True, 0


def validateLabels(df: pd.DataFrame, col_label_dict: dict, return_column=False):
    for col in col_label_dict:
        if col not in df.columns:
            print(f"The column {col} does not appear in the columns: {df.columns}")
            continue
        values = df[col].unique().tolist()
        for val in values:
            val = 'NA' if pd.isna(val) else val
            if val not in col_label_dict[col]:
                for poss in col_label_dict[col]:
                    pass
                if return_column:
                    return False, col
                print(f"{lcol.WARNING}The column {col} contains the invalid label {val}{lcol.ENDC}")
                return False, "A column contains an invalid label"
    return True, 0


def validateLabelCounts(df: pd.DataFrame, col_label_counts: dict):
    for col, settings in col_label_counts.items():
        try:
            value_counts = df[col].value_counts()
        except KeyError:
            value_counts = df[col.lower()].value_counts()
        print(f"In the column {col} the labels appear with following frequency:")
        print(value_counts)
        cur_value = value_counts.iloc[0]
        cur_label = value_counts.index[0]
        counts_min = value_counts.min()
        for label, value in value_counts.iteritems():
            if settings["type"] == 'equal':
                if cur_value != value:
                    print(
                        f"{lcol.WARNING}In the column {col} the labels should be equally frequent but the label {cur_label} and {label} appear {cur_value} and {value} times respectively{lcol.ENDC}")
                    return False, "A label is more frequent than it should be"
            elif settings["type"] == 'unequal':
                try:
                    if value < (settings["options"][label] * counts_min * 0.95) or value > (
                            settings["options"][label] * counts_min * 1.05):
                        print(
                            f"{lcol.WARNING}In the column {col} the labels should be {settings['options'][label]} times more frequent than the minimum label but it actually is {value / counts_min}x the minimum (Check if either this label or the minimum are incorrect){lcol.ENDC}")
                        return False, "A label is more frequent than it should be"
                except Exception as e:
                    print(e)
    return True, 0


def validateSeparator(file: str, expected_separator: str, expected_col_count=2):
    with open(file, "r") as f:
        s = f.read()
    rows = s.split("\n")
    first_row = rows[0].split(expected_separator)
    if len(first_row) < expected_col_count:
        print(f"{lcol.WARNING}The expected separator {expected_separator} does not seem to be correct{lcol.ENDC}")
        return False, "Invalid separator"
    else:
        return True, 0


def validateVar_Types(df: pd.DataFrame, expected_types_dict):
    lu = {
        "dec": float,
        "str": str,
        "int": int
    }
    for col in expected_types_dict:
        split_instructions = expected_types_dict[col].split("|")
        typ = split_instructions[0]
        checkingType = lu.get(typ, False)
        for ind, item in df[col].iteritems():
            if typ == 'dec' and isinstance(item, str):
                if "%" in item:
                    df.iloc[ind].at[col] = float(item.strip('%')) / 100
            if item != 'NA':
                continue
            if checkingType:
                if not isinstance(item, checkingType):
                    if not (checkingType == float and isinstance(item, int)):
                        return False, 'Mismatched variable type'
                elif len(split_instructions) > 2:
                    bounds = [float(x.strip()) for x in split_instructions[1].split("-")]
                    if item > max(bounds) or item < min(bounds):
                        print(f"{lcol.WARNING}A value in col {col} at index {ind} is out of bounds: {item}{lcol.ENDC}")
                        return False, 'Value out of bounds'

    return True, 0


def saveFile(df: pd.DataFrame, filename):
    df.to_csv(filename, index=False, sep=",")


def dropColumns(df: pd.DataFrame, desired_columns):
    to_drop = [col for col in df.columns if col not in desired_columns]
    num_diff = len(to_drop)
    if not binaryResponse(f"Found the following columns mismatched: {to_drop}. Do you want to drop them (y) or do you want to manually choose which to drop (n)?"):
        to_drop = choose_multiple_from_dict({i: c for i, c in enumerate(df.columns)}, label='Columns', request_description="Which ones do you choose to drop?")
    df = df.drop(columns=to_drop)
    print(f"{lcol.OKGREEN}Fixed Columns by dropping '{to_drop}'{lcol.ENDC}")
    if num_diff > len(to_drop):
        if binaryResponse("There are still mismatched columns in the file, do you want to fix them still?"):
            return fixColumns(df, desired_columns)
        else:
            return df
    else:
        return df


def fixSeparator(df: pd.DataFrame, filepath: str):
    df = pd.read_csv(filepath, delimiter=";")
    saveFile(df, filepath)
    print(f"{lcol.OKGREEN}Resaved file with correct seperators{lcol.ENDC}")
    return df


def fixColumns(df: pd.DataFrame, desired_columns):
    order_as_is = {col: i for i, col in enumerate(df.columns)}
    order_as_should_be = {col: i for i, col in enumerate(desired_columns)}
    print(f"Right now we have the following columns: {order_as_is}")
    print(f"The rules expect the following columns: {order_as_should_be}")
    final_order = ["" for _ in range(len(desired_columns))]
    need_to_reorder = False
    for col, ind in order_as_should_be.items():
        final_order[ind] = col
        found_col = order_as_is.get(col, 'Not found')
        if found_col == 'Not found':
            print(
                f"The column '{col}' does not appear in the file. {lcol.UNDERLINE}Which of the columns is the translation of this?{lcol.ENDC}")
            options = {i: col for i, col in enumerate(df.columns)}
            options['None']: 'Drop column'
            chosen = choose_from_dict(options, 'Columns')
            if chosen == 'Drop column':
                df = df.drop(columns=[col])
            else:
                df = df.rename(columns={chosen: col})
                order_as_is[col] = order_as_is.get(chosen, ind)
        if ind != order_as_is[col]:
            need_to_reorder = True
    if need_to_reorder:
        df = df[final_order]
    return df


def fixLabels(df: pd.DataFrame, expected_labels_in: dict):
    cols_checked = []
    while True:
        to_check = expected_labels_in
        for i in cols_checked:
            try:
                to_check.pop(i)
            except Exception as e:
                print(e)
        valid, col = validateLabels(df, col_label_dict=to_check, return_column=True)
        if not valid:
            cols_checked.append(col)
            print(f"Invalid labels in Column: {lcol.UNDERLINE}'{col}'{lcol.ENDC}")
            if binaryResponse("Do you want to fix these?"):
                exp_col_labels = expected_labels_in[col].copy()
                curr_labels = df[col].unique().tolist()
                if len(exp_col_labels) < len(curr_labels):
                    print(f"Expected  labels: {exp_col_labels}")
                    print(f"Actual labels: {curr_labels}")
                    if binaryResponse(
                            "It seems like there is more labels than expected. Do you want to solve this by excluding some rows with labels?"):
                        df = dropLabels(df, curr_labels=curr_labels, exp_col_labels=exp_col_labels, col=col)
                    do_rename = binaryResponse("Do you want to rename any labels?")
                else:
                    do_rename = True
                if do_rename:
                    to_rename = {}
                    isMonthType = True if col == 'Month' else binaryResponse(
                        f'Is {lcol.UNDERLINE} "{col}" {lcol.ENDC} a {lcol.UNDERLINE}"month"-type{lcol.ENDC} column?')
                    if isMonthType:
                        x = 0 if len(exp_col_labels) == 1 else -2
                        if isinstance(exp_col_labels[x], int) or isinstance(exp_col_labels[x], float):
                            to_rename = {(i + 1): i for i in range(12)}
                    else:
                        for label in curr_labels:
                            ind = False
                            for i, item in enumerate(exp_col_labels):
                                if item == label:
                                    ind = i
                                    break
                                elif isinstance(label, int) and isinstance(item, float):
                                    if label == int(item):
                                        ind = i
                                        break
                                elif label == 'NA' and pd.isna(item):
                                    ind = i
                                    break
                            if ind is False:
                                to_rename[label] = ""
                            else:
                                exp_col_labels.pop(ind)
                    if len(to_rename) > 0:
                        if not isMonthType:
                            for label in to_rename:
                                print(f"Choose {lcol.UNDERLINE}which label to replace '{label}' with:{lcol.ENDC}")
                                options = {i: l for i, l in enumerate(exp_col_labels)}
                                options['self'] = label
                                to_rename[label] = choose_from_dict(options,
                                                                    label='Labels') if len(exp_col_labels) > 1 else \
                                exp_col_labels[0]
                        # split up file between cases -> nan and other
                        if binaryResponse(
                                f"It sometimes happens that two separate cases lead to inconsistencies. E.g. due to missing results in a different column. \n{lcol.UNDERLINE}Do you want to split the file by NaNs and other values and thus preserve a part of the file as is?{lcol.ENDC}"):
                            print(
                                f"{lcol.OKBLUE}Which column{lcol.ENDC} do you want to use for splitting based on NaNs? (e.g. Index, Distribution, Percentage)")
                            choice = choose_from_dict({i: col for i, col in enumerate(df.columns)}, 'Columns')

                            def fix_row(row, rename_dict, dep_col, to_adjust_col):
                                if pd.isna(row[dep_col]):
                                    row[to_adjust_col] = rename_dict.get(row[to_adjust_col], row[to_adjust_col])
                                return row

                            print(f"{lcol.WARNING}Applying fixes to col '{col}'{lcol.ENDC}")
                            df = df.apply(fix_row, args=(to_rename, choice, col), axis=1)
                            print(f"{lcol.OKGREEN}Fixed col {col} in conjunction with NaNs in col {choice}{lcol.ENDC}")
                        else:
                            df[col] = df[col].replace(to_rename)
                        print(f"Chosen rename rules: {to_rename}")
        else:
            break
    return df


def dropLabels(df, curr_labels="", exp_col_labels="Unknown", col=""):
    to_drop = []
    if len(curr_labels) == 0:
        curr_labels = df[col].unique().tolist()
    while True:
        print(f"Expected amount of labels {len(exp_col_labels)} - Current amount: {len(curr_labels)}")
        print(f"These are the expected labels: {exp_col_labels}")
        print(f"Choose based on which label(s) to drop rows")
        cs = {i: l for i, l in enumerate(filter(lambda x: x not in to_drop, curr_labels))}
        cs['End'] = "Finish - Selected All"
        c = choose_from_dict(cs)
        if c != 'Finish - Selected All':
            to_drop.append(c)
        else:
            break
    for l in to_drop:
        df = df.query(f"{col} != @l")
    return df


# TODO (P2): Need to find feasible logic to suggest fixes
def fixLabelCounts(df, label_counts_dict):
    # identify what groupings there are -> define order
    cols = {i: col for i, col in enumerate(df.columns)}
    grouping_order = []

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
                            "Country selected": country_selected,
                            "ticket_taxonomy_tag_name": tag,
                            "ticket_geo_region_name": region,
                            "Year": year,
                            "Month": month
                        })
    for col in ['ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month', 'Country selected']:
        counts = df[col].value_counts()
        for ind, item in counts.iteritems():
            expected = expected_counts.get(ind, False)
            if expected:
                if expected <= item:
                    for i, combination in enumerate(combinations):
                        if combination.get(col, "") == ind:
                            combinations.pop(i)

    additions = []
    for combination in combinations:
        tag = combination['ticket_taxonomy_tag_name']
        country_selected = combination['Country selected']
        region = combination['ticket_geo_region_name']
        year = combination['Year']
        month = combination['Month']
        query = df.query(
            "(ticket_taxonomy_tag_name==@tag) & (`Country selected`==@country_selected) & (ticket_geo_region_name==@region) & (Year==@year) & (Month==@month)")
        if query.shape[0] == 0:
            additions.append([tag, region, year, month, 0, 0, country_selected])
    addition = pd.DataFrame(additions, columns=['ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month',
                                                'No_of_tickets', 'Index', 'Country selected'])
    final_df = pd.concat([df, addition], ignore_index=True)

    final_df = final_df.sort_values(
        by=['Country selected', 'ticket_taxonomy_tag_name', 'ticket_geo_region_name', 'Year', 'Month'],
        ascending=True).fillna('NA')
    final_df = final_df.round(2)
    final_df.to_csv("FINAL_CHART_ITALY.csv", index=False)


def getFixer(fixing_instructions, locs):
    if fixing_instructions.get('any', False):
        fixer_fn = locs.get(fixing_instructions['any'], False)
        return fixer_fn
    else:
        for case, fix in fixing_instructions.items():
            if reason == case:
                fixer_fn = locs.get(fix, False)
                return fixer_fn
    return False


def dialog(lcls: dict = locals()):
    global args
    cc, country = getChosenCountry(action='validate')
    args = parser.parse_args()
    # args = vars(args)
    if args.folder:
        args.folder = " ".join(args.folder)
        print(f"Using {vars(args)}")
    if args.folder is False:
        print("What folder do you want to run validation for?")
        folder = chooseFolder(base_folder=FS.cwd)
    else:
        folder = args.folder
    files = os.listdir(folder)
    cc_rules = rules[country]
    # TODO (P2): Add label_counts fixing function (i.e. check what rows are missing and add)
    # TODO (P2): Add fixer for wrong values
    # TODO (P2): Add custom fixing functions -> find and replace
    availableRules = {
        "separators": {
            "validator": "validateSeparator",
            "fixer": {
                "any": "fixSeparator"
            }
        },
        "columns": {
            "validator": "validateColumns",
            "fixer": {
                "Mismatched length of columns": "dropColumns",
                "Column is not matched": "fixColumns"
            }
        },
        "labels": {
            "validator": "validateLabels",
            "fixer": {
                "any": "fixLabels"
            }
        },
        "label_counts": {
            "validator": 'validateLabelCounts',
        },
        "var_types": {
            'validator': 'validateVar_Types',
        }
    }
    rule_types = ['separators', 'columns', 'labels', 'label_counts', 'var_types']
    worked_on_files = []
    issues = 0
    fixes = 0
    for file_type, rule in cc_rules.items():
        alternate_name = file_name_remap.get(file_type, file_type)
        for file in files:
            if (file_type in file or alternate_name in file) and file not in worked_on_files:
                if binaryResponse(
                        f"{lcol.OKBLUE}Do you want to conduct the validation for files such as '{file_type}'? Current File: {file}{lcol.ENDC}"):
                    print(f"{lcol.OKGREEN}Working on file {file}{lcol.ENDC}")
                    worked_on_files.append(file)
                    wasChanged = False
                    hasAskedForManual = False
                    filepath = os.path.join(folder, file)
                    df = pd.read_csv(filepath)
                    for r in rule_types:
                        handling_instructions = availableRules[r]
                        validator = lcls.get(handling_instructions['validator'], False)
                        if validator is not False:
                            if r == 'separators':
                                valid, reason = validator(filepath, rule[r], len(rule.get('columns', [1, 2])))
                            else:
                                valid, reason = validator(df, rule[r])
                            if not valid:
                                issues += 1
                                print(Exception(reason))
                                fixing_instructions = handling_instructions.get('fixer', {})
                                fixer = getFixer(fixing_instructions, lcls)
                                if fixer is not False:
                                    if binaryResponse(
                                            f"{lcol.UNDERLINE}Do you want to attempt to fix this {r} issue?{lcol.ENDC}"):
                                        try:
                                            if r != 'separators':
                                                df = fixer(df, rule[r])
                                            else:
                                                df = fixer(df, filepath)
                                        except Exception as e:
                                            print(e)
                                        finally:
                                            wasChanged = True
                                            fixes += 1
                    if not hasAskedForManual:
                        if binaryResponse("Do you want to conduct any manual changes?"):
                            c = choose_from_dict({1: 'drop Labels for a column'}, 'Options')
                            if c == 'drop Labels for a column':
                                print(f"Which column do you want to drop Labels for?")
                                chosen_col = choose_from_dict({i: col for i, col in enumerate(df.columns)}, 'Columns')
                                df = dropLabels(df, curr_labels="", exp_col_labels="Unknown", col=chosen_col)
                                wasChanged = True
                                hasAskedForManual = True
                        else:
                            hasAskedForManual = True

                    if wasChanged:
                        if binaryResponse(f"{lcol.UNDERLINE}Do you want to save the conducted changes?{lcol.ENDC}"):
                            print(f"{lcol.OKGREEN}{lcol.BOLD}Saved file {filepath} with changes{lcol.ENDC}")
                            df = df.fillna("NA")
                            directory, filename = os.path.split(filepath)
                            df.to_csv(filepath, index=False)
                            alt_fp = os.path.join(directory, alternate_name)
                            df.to_csv(alt_fp, index=False)
                            print(f'Saved this:\n"file:/{filepath}"\n"file:/{alt_fp}"')
    print(f"Finished. Encountered {issues} issues and fixed {fixes} of them.")
    sys.exit()


if __name__ == '__main__':
    dialog(locals())
