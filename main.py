import Datapipeline
import Data_Inspectors
import Input_Set_Up
import Validation

from utils.user_interaction_utils import binaryResponse, choose_from_dict, choose_multiple_from_dict, chooseFile, chooseFolder
from utils.misc_utils import lcol

class Activity:
    def __init__(self):
        pass

    def __repr__(self):
        pass



if __name__ == '__main__':
    print(f"{lcol.BOLD}Welcome to ProntoTrends. What do you want to do today?{lcol.ENDC}")
    choices = [
        "Work on generating Data",
        "Inspect Data",
        "Validate Data",
        "Set up Inputs for new Data generation"
    ]
    choice = choose_from_dict({i: x for i, x in enumerate(choices)})
    if choice == 'Work on generating Data':
        pass

