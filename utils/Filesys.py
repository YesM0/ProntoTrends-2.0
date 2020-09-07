if __name__ == '__main__':
    import sys

    sys.path.append('../')

import os
from typing import Union, List, Dict
# import pandas as pd
from utils.custom_types import *


cwd = os.getcwd()
if os.path.split(cwd)[1] != 'ProntoTrends' or 'utils' in cwd:
    cwd = os.path.split(cwd)[0]


def makePath(*args) -> Filepath:
    return os.path.join(*args)


class Fileserver:
    """
    Represents the filesystem structure and implements utilities for opening and retrieving files
    typically settings should be found here: os.path.join(cwd, 'Input_Files', 'Static', '.settings.yaml')
    """

    def __init__(self, settings_file: str = None):
        """
        Sets up the main folders and file-locations
        Args:
            settings_file: str -- path to settings file
        """
        self.cwd: Folderpath = cwd
        self.Outfiles_general: Folderpath = makePath(self.cwd, 'Output_Files')
        self.Aggregated: Folderpath = makePath(self.cwd, 'Output_Files', 'Aggregated')
        self.Final: Folderpath = makePath(self.cwd, 'Output_Files', 'FINAL')
        self.Kwd_Level_Outs: Filepath = makePath(self.cwd, 'Output_Files', 'out')
        self.Comparisons: Folderpath = makePath(self.cwd, 'Output_Files', 'comparisons')
        self.Inputs: Folderpath = makePath(self.cwd, 'Input_Files')
        self.Statics: Folderpath = makePath(self.cwd, 'Input_Files', 'Static')
        self.Validation: Folderpath = makePath(self.cwd, 'Validation')
        if settings_file and ".yaml" in settings_file:
            import yaml
            try:
                with open(settings_file, "r") as f:
                    s = f.read()
                d: dict = yaml.safe_load(s)
                self.Outfiles_general: Folderpath = d.get("Out_files_path", self.Outfiles_general)
                self.Aggregated: Folderpath = d.get("Aggregated_path", self.Aggregated)
                self.Final: Folderpath = d.get("Final_path", self.Final)
                self.Kwd_Level_Outs: Folderpath = d.get("Tag_out_path", self.Kwd_Level_Outs)
                self.Comparisons: Folderpath = d.get("Comparisons_path", self.Comparisons)
                self.Inputs: Folderpath = d.get("Inputs_path", self.Inputs)
                self.Statics: Folderpath = d.get("Statics_path", self.Statics)
                self.Validation: Folderpath = d.get('Validation_path', self.Validation)
            except FileNotFoundError as e:
                print('No settings file could be found')

        self.Settings_File: Filepath = makePath(self.Statics, '.settings.yaml') if os.path.exists(
            makePath(self.Statics, '.settings.yaml')) else None
        self.Ordered_Regions: Filepath = makePath(self.Statics, "ordered_regions.json") if os.path.exists(
            makePath(self.Statics, "ordered_regions.json")) else FileNotFoundError(
            f"The file 'ordered_regions' does not exist. Ensure that it is located in: {self.Statics}")
        self.All_Google_Locales: Filepath = makePath(self.Statics, "All_Google_Locales.json") if os.path.exists(
            makePath(self.Statics, "All_Google_Locales.json")) else FileNotFoundError(
            f"The file 'All_Google_Locales.json' does not exist. Ensure that it is located in: {self.Statics}")
        self.Routines: Filepath = makePath(self.Inputs, 'Routines') if os.path.exists(makePath(self.Inputs, 'Routines')) else FileNotFoundError(
            f"The Folder '/Inputs/Routines' does not exist. Please set it up and use it for storing your routines"
        )

    def OutFilepath(self, keyword: str, dimension: str, country_fullname: Country_Fullname,
                    region_code: Region_Shortcode, keyword_id: int):
        path = makePath(self.Kwd_Level_Outs, keyword,
                        f"{dimension}_{country_fullname}_{region_code}_{keyword}_{keyword_id}.csv") if dimension.lower() == 'time' else makePath(
            self.Kwd_Level_Outs, keyword,
            f"{dimension}_{country_fullname}_{keyword}_{keyword_id}.csv")
        return path

    def AggregatedFilepath(self, country_fullname: Country_Fullname, region_id: Region_Shortcode, tag_id: int, tag_name: str, dimension: str, adjusted: Union[bool, str]):
        if adjusted:
            adjusted = "_Adjusted"  
        else:
            adjusted = ''
        fname = f"{region_id}_{tag_id}_{tag_name}_{dimension}{adjusted}.csv"
        return makePath(self.Aggregated, country_fullname, fname)

    def ComparisonFilepath(self, category_name: str, country_fullname: Country_Fullname, region_id: Region_Shortcode, dimension: str):
        dimension = 'time' if dimension.lower() == 'time' else 'Geo'
        fname = f"{category_name}/{dimension}_{country_fullname}_{region_id}_{category_name}.csv"
        return makePath(self.Comparisons, category_name, fname)

    def FinalCsvFilepath(self):
        # Output_Files/FINAL/Italy/Wed/Wed-Ceremony Type_Italy.csv
        # Output_Files/FINAL/Italy/Wed/Wed-Top5_Tags_Italy.csv
        # Wed_Main_Section_Italy.csv
        # Wed_Map_Data_Italy.csv
        # Wed_Table_Data_Italy.csv
        print("Not implemented yet")
        pass

    # TODO: replace normal filenaming with these Functions


generic_FileServer = Fileserver()

try:
    GDrive_FileServer = Fileserver(os.path.join(cwd, 'Input_Files', 'Static', '.settings.yaml'))
except Exception:
    print("No GDrive Saving Location could be found")
    GDrive_FileServer = generic_FileServer

# TODO (p1): Add functions to find files / get subfolders
# TODO (p2): consider smaller classes: e.g. Aggregated_Folder class with custom utilities
