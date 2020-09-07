if __name__ == '__main__':
    import sys
    sys.path.extend(['../', './'])

import json
import os
import shutil
from typing import Dict, List, Union

from utils.Countries import Country, \
    getCountry  # Country is a class that represents a country with properties like a shortcode (e.g. "fr") and the english full_name (e.g. 'France')
from utils.Filesys import \
    generic_FileServer as FS  # FS is a class to save data about the different folders in the project
from utils.custom_types import *
from utils.user_interaction_utils import chooseFolder, choose_from_dict, \
    user_input  # chooseFolder -> utility to select a folder by traversing the file system, choose_from_dict -> handles user choice from a dict of choices, user_input -> convenience method for input() that stops execution on keywords


def explore_directory(directory_path: Folderpath) -> List[Dict[str, List[Union[dict, str]]]]:
    print(f"Exploring folder: {directory_path}")
    cont = []
    contents = os.listdir(directory_path)
    files = [item for item in contents if
             os.path.isfile(os.path.join(directory_path, item)) and not item.startswith(".")]
    folders = [item for item in contents if os.path.isdir(os.path.join(directory_path, item)) and item not in ['venv',
                                                                                                               "__pycache__"] and not item.startswith(
        ".")]
    cont.extend(files)
    folders = {folder: explore_directory(os.path.join(directory_path, folder)) for folder in folders}
    if len(list(folders.keys())) > 0:
        cont.append(folders)
    return cont


def gather_all_country_files(country: Country, dest_folder: Folderpath, base_folder: Folderpath,
                             curr_dir: Folderpath = FS.cwd):
    items = os.listdir(curr_dir)
    for item in items:
        path = os.path.join(curr_dir, item)
        if os.path.isdir(os.path.join(curr_dir, item)):
            # enter dir
            # if cc file in dir: create dirs and copy file to dest
            gather_all_country_files(country, dest_folder, base_folder, curr_dir=path)
        elif os.path.isfile(path) and (
                f"{country.Shortcode.upper()}" in item or country.Full_name.lower() in item.lower()):  # can edit the (conditions) for alternative filters
            # copy file using the parent folder copied
            relpath = os.path.relpath(curr_dir, start=base_folder)
            new_folder = os.path.join(dest_folder, relpath)
            if not os.path.exists(new_folder):
                os.makedirs(new_folder)
            new_file_path = os.path.join(new_folder, item)
            shutil.copy(path, new_file_path)
            print(f"Copied file: {item} to {new_file_path}")


if __name__ == '__main__':
    action = choose_from_dict(["Make list of files", "Create a country-specific copy of files"], "actions",
                              request_description="What do you want to do?")
    if action == 'Make list of files':
        start_folder = chooseFolder(base_folder=FS.cwd, request_str="Where do you want to start exploring the files?")
        result = explore_directory(start_folder)
        filepath = os.path.join(FS.cwd, "folder_profiler_result.json")
        with open(filepath, "w+") as f:
            f.write(json.dumps(result, indent=4))
            print(f"Saved output in file://{filepath}")
    elif action == "Create a country-specific copy of files":
        country = getCountry(prompt="What country do you want to do this for?")
        fname = user_input("What folder name do you want to use for the files?").strip()
        gather_all_country_files(country, dest_folder=os.path.join(FS.cwd, fname), base_folder=FS.cwd)
