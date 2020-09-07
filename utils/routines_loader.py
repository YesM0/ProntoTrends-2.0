import os
import sys

if __name__ == '__main__':
    sys.path.extend(['../', './'])

import yaml

from utils.Filesys import generic_FileServer as FS
from utils.user_interaction_utils import chooseFile


def get_routine_settings(filename: str = None) -> dict:
    file = None
    if not filename:
        file = chooseFile(filetype='.yaml', request_prompt='Choose the file to get the routine from',
                          base_path=FS.Routines, give_filter_option=False)
    else:
        for x in os.listdir(FS.Routines):
            if filename in x:
                file = os.path.join(FS.Routines, x)
                break
    if file:
        with open(file, "r") as f:
            s = f.read()
        d: dict = yaml.safe_load(s)
        return d
    else:
        return {}
