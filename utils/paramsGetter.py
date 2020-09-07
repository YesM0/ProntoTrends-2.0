from utils.Countries import get_region_id_to_name_dict

if __name__ == '__main__':
    import sys
    sys.path.append('../')

import json
import os

from utils.custom_types import Country_Fullname
from utils.misc_utils import deepSearch
from utils.Filesys import generic_FileServer

FS = generic_FileServer


def readInCategories():
    with open(os.path.join(FS.Statics, "All_Categories_Google.txt"), "r", encoding='utf-8') as f:
        allCats = f.read()
    allCats = json.loads(allCats)
    return allCats


def findCategory(name):
    result = deepSearch(name, readInCategories())
    return result


if __name__ == '__main__':
    # readInCategories()
    # readInLocales()
    # print(f"Found for Bavaria: {findLocale('Bavaria', isRegion=True)}")
    # print(f"Found for Netherlands: {findLocale('Netherlands', includeChildren=True)}")
    # print(f"Found id: {findCategory('Book Retailers')}")
    get_region_id_to_name_dict(Country_Fullname('Germany'))
