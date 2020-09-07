if __name__ == '__main__':
    import sys

    sys.path.append('../')

import os
import sys
from pprint import pprint
from typing import Union, Dict, List

from utils.custom_types import *
from utils.misc_utils import lcol
import pandas as pd

CWD = os.getcwd()
if 'utils' in CWD:
    CWD = os.path.split(CWD)[0]


def user_input(prompt: str, blocked_contents: List[str] = None) -> str:
    while True:
        res = input(prompt)
        if res.lower() == 'stop' or res.lower() == 'break':
            print("Exiting")
            sys.exit()
        elif isinstance(blocked_contents, list) and any([x in res for x in blocked_contents]):
            print(f"The value {res} is invalid. Please ensure that it doesn't contain {blocked_contents}")
            continue
        else:
            return res


def getChosenCountry(action: str = 'scrape', testing: bool = False, test_return: Union[None, tuple] = None) -> (
Country_Shortcode, Country_Fullname):
    if testing:
        print(test_return)
        if not isinstance(test_return, tuple):
            raise ValueError(f"The passed in test_return value {test_return} is not a tuple")
        return test_return
    else:
        allowed_ccs = {
            "de": "Germany",
            "es": "Spain",
            "fr": "France",
            "it": "Italy",
            "at": "Austria",
            "ch": "Switzerland"
        }
        while True:
            pprint(allowed_ccs)
            chosen: str = user_input(
                f"What country do you want to {action} for? Please put in the shortcode:\n").strip().lower()
            res: Union[str, bool] = allowed_ccs.get(chosen, False)
            if res:
                return Country_Shortcode(chosen.upper()), Country_Fullname(res)


def binaryResponse(question_string: str, testing: bool = False, test_return: bool = ""):
    """
    Facilitates a binary input from the user. Includes loop until valid response
    Args:
        question_string: str -- the question to pose
        test_return: bool -- the return value in case of testing
        testing: bool -- whether to return a test_return rather than asking the user

    Returns: bool -- True if 'yes' False if 'no'

    """
    if testing:
        print(test_return)
        return test_return
    else:
        while True:
            response = user_input(f"{question_string} (y/n)\n")
            if 'y' in response.lower():
                return True
            elif 'n' in response.lower():
                return False
            else:
                continue


def choose_from_dict(dictionary: Union[Dict[Union[str, int], str], List[str]], label: str = 'items',
                     incl_end_option: bool = False,
                     end_description: str = "End Selection", allow_multiple_answers: bool = False,
                     testing: bool = False, test_return: str = "", request_description: str = "") -> Union[
    str, List[str]]:
    if testing:
        print(test_return)
        return test_return
    else:
        if isinstance(dictionary, list):
            dictionary = {i: k for i, k in enumerate(dictionary)}
        if len(request_description) > 0:
            print(f"{lcol.OKGREEN}{request_description}{lcol.ENDC}")
        if len(dictionary.keys()) == 1:
            single_choice = dictionary[list(dictionary.keys())[0]]
            print(f"Only a single choice can be made, thus it was chosen: {single_choice}")
            return single_choice
        if incl_end_option:
            dictionary['End'] = end_description
        choices = []
        while True:
            print(f"\nWhich of the {label} do you choose?") if not allow_multiple_answers else print(
                f"\n Which of the {label} do you choose? You can select multiple by seperating them by ','")
            pprint(dictionary)
            response = user_input("\n")
            invalids = []
            if "," in response and allow_multiple_answers:
                response = map(lambda x: x.strip(), response.split(","))
            else:
                response = [response]
            for r in response:
                if r.isdigit():
                    try:
                        r = int(r)
                    except Exception:
                        print('Cannot convert to num')
                elif r.lower() == 'end':
                    r = 'End'
                item_chosen = dictionary.get(r, False)
                if item_chosen:
                    choices.append(item_chosen)
                else:
                    invalids.append(r)
            if len(invalids) > 0:
                if allow_multiple_answers:
                    print(f"The following items could not be found: {invalids}")
                    print(f"Please try again. Current choices: {choices}")
                else:
                    print("Could not match this. Try again")
                    choices = []
            else:
                ret: Union[str, List[str]] = choices if len(choices) > 1 or allow_multiple_answers else choices[0]
                return ret


def choose_multiple_from_dict(dictionary: Union[Dict[Union[str, int], str], List[str]], label: str = 'items',
                              testing: bool = False, test_return: str = "", request_description: str = ""):
    if isinstance(dictionary, list):
        dictionary = {i: k for i, k in enumerate(dictionary)}
    if testing is True:
        return test_return
    else:
        selection = []
        while True:
            for y in selection:
                if dictionary.get(y, False):
                    dictionary.pop(y)
            s = choose_from_dict(dictionary, label=label, incl_end_option=True, request_description=request_description,
                                 allow_multiple_answers=True)
            if 'End Selection' in s:
                if len(s) == 1:
                    break
                else:
                    s = list(filter(lambda x: x != "End Selection", s))
                    selection += s
                    break
            else:
                selection += s
        return selection


def chooseFolder(request_str: str = None, base_folder=None, testing: bool = False, test_return: str = "") -> Filepath:
    if testing:
        print(test_return)
        return test_return
    else:
        if request_str:
            print(request_str)
        if base_folder:
            curr_path = base_folder
        else:
            curr_path = os.getcwd()
        while True:
            contents = os.listdir(curr_path)
            undesired = ['__pycache__', '.ipynb_checkpoints', 'venv', '.idea', '.DS-Store']
            folders = [item for item in contents if
                       os.path.isdir(os.path.join(curr_path, item)) and item not in undesired]
            choices = {i: item for i, item in enumerate(folders)}
            choices['End'] = 'Choose the current folder'
            if len(folders) == 0:
                return curr_path
            else:
                print(f"Current folder: {curr_path}")
                choice = choose_from_dict(choices, 'Folders')
                if choice != 'Choose the current folder':
                    curr_path = os.path.join(curr_path, choice)
                else:
                    return curr_path


def chooseFile(filetype: str = ".", other_only_if_contains_selections: list = None, testing: bool = False,
               test_return: str = "", request_prompt: str = None, base_path: Folderpath = None,
               give_filter_option: bool = True) -> Filepath:
    if testing:
        print(test_return)
        return test_return
    else:
        if request_prompt:
            print(request_prompt)
        curr_path = os.getcwd() if base_path is None else base_path
        do_selection = isinstance(other_only_if_contains_selections, list)
        if not do_selection and give_filter_option:
            print(f"To make file-selection easier, you can set up selection presets.\n")
            do_selection = binaryResponse("Do you want to set these?")
        if do_selection and give_filter_option:
            if isinstance(other_only_if_contains_selections, list):
                print(f"Currently the following are already chosen: {other_only_if_contains_selections}")
            other_only_if_contains_selections = defineList(initial_selection=other_only_if_contains_selections,
                                                           label="Items (applied as keep if contains item)")
        while True:
            contents = os.listdir(curr_path)
            undesired = ['__pycache__', '.ipynb_checkpoints', 'venv', '.idea']
            filtered = filter(lambda f: f not in undesired and (os.path.isdir(f) or filetype in f), contents)
            items = list(filtered)
            if other_only_if_contains_selections:
                items = list(filter(lambda f: any(map(lambda y: y in f, other_only_if_contains_selections)), items))
            choices = {i: item for i, item in enumerate(items)}
            if len(items) == 0:
                print(f"No files in curr directory: {curr_path}")
                intermediate = os.path.split(curr_path)
                curr_path = intermediate[0]
            else:
                print(f"Current path: {curr_path}")
                choice = choose_from_dict(choices, 'Items')
                poss_path = os.path.join(curr_path, choice)
                if os.path.isdir(poss_path):
                    curr_path = poss_path
                elif os.path.isfile(poss_path):
                    return poss_path


# TODO (p1): Add unit test -> single input, multi input
def defineList(initial_selection: list = (), label: str = "categories", wanted_type: str = None, request_text: str = None) -> list:
    chosen = initial_selection.copy() if initial_selection is not None and len(initial_selection) > 0 else []
    run = 0 if len(chosen) > 0 else 1
    while True:
        if run > 0:
            while True:
                question = f"Please declare the {label} you want to use:\n" if not request_text else f"{request_text}\n"
                cats = user_input(question)
                if "," in cats:
                    cats = cats.split(",")
                    cats = [x.strip() for x in cats]
                    if wanted_type is not None:
                        try:
                            if wanted_type == 'int':
                                cats = list(map(lambda x: int(x), cats))
                        except ValueError as e:
                            print(f"Invalid type of items: {type(cats[0])} should be {wanted_type}")
                    chosen += cats
                    break
                else:
                    if wanted_type is not None:
                        try:
                            if wanted_type == 'int':
                                cats = int(cats.strip())
                                chosen.append(cats)
                                break
                        except ValueError as e:
                            print(f"Invalid type of items: {type(cats[0])} should be {wanted_type}")
                    else:
                        chosen.append(cats.strip())
                        break
        run += 1
        print(f"You have selected the following {label}: {chosen}\nDo you want to end?")
        choice = choose_from_dict({1: 'Add more', 2: 'Finished', 3: 'Clear - Start over'})
        if choice == 'Finished':
            return chosen
        elif choice == 'Add more':
            print(f"Here's your current selection: {chosen}")
        elif choice == 'Clear - Start over':
            chosen = []


def defineFilename(target_ending: str = '.json', target_folder: str = None) -> Filepath:
    while True:
        filename = user_input(
            "What is the name of the file under which you want to save it? (it will automatically be saved in Input_Files)").strip()
        if "/" in filename:
            print("You have the invalid character '/' in your filename, please try again")
        else:
            break
    if target_ending not in filename:
        filename = filename + target_ending
    path: Filepath = os.path.join(target_folder, filename) if target_folder else os.path.join(CWD, filename)
    return path


def choose_column(df: pd.DataFrame, instruction_str: str = None, testing: bool = False,
                  test_return: Union[str, List[str]] = None, allow_multiple: bool = False, exclude: list = ()) -> Union[
    str, List[str]]:
    """
    Allows the user to select one or more columns
    Args:
        df:
        instruction_str:
        allow_multiple:
        exclude:
        testing:
        test_return:

    Returns:

    """
    if testing:
        return test_return
    if instruction_str:
        print(instruction_str)
    options = df.columns
    if len(exclude) > 0:
        options = list(filter(lambda x: x not in exclude, options))
    d = {i: k for i, k in enumerate(options)}
    if not allow_multiple:
        return choose_from_dict(d, label='column')
    else:
        return choose_multiple_from_dict(d, label='column')


def int_input(prompt: str) -> int:
    while True:
        try:
            inn = user_input(f"{prompt.strip()}\n")
            if inn.lower() == 'end' or inn.lower() == 'stop':
                sys.exit()
            integer = int(inn)
            return integer
        except ValueError:
            print("Could not parse input. Make sure you type a number")
