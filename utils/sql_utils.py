if __name__ == '__main__':
    import sys
    sys.path.append('../')

import getpass
import os
from typing import Dict, Tuple, List, Union

import pandas as pd
import pymysql.cursors
import yaml

from utils.Filesys import generic_FileServer as FS
from utils.custom_types import *
from utils.misc_utils import lcol
from utils.user_interaction_utils import binaryResponse, chooseFolder, user_input


def get_sql_login_data() -> dict:
    path: Filepath = FS.Settings_File
    create_file: bool = False
    if path is not None and os.path.exists(path):
        with open(path, 'r') as f:
            s = f.read()
        d: dict = yaml.safe_load(s)
        if isinstance(d, dict):
            if 'host' not in d or 'password' not in d or 'user' not in d:
                create_file = True
            else:
                return {key: val for key, val in d.items() if key in ['host', 'user', 'password']}
        else:
            create_file = True
    else:
        create_file = True
    if create_file:
        print(f"{lcol.OKGREEN}We don't seem to find your login details for the database. Let's set them up!{lcol.ENDC}")
        while True:
            host = input("What is the host (db link) you want to connect to?\n").strip()
            user = input("What is your user name? ").strip()
            password = getpass.getpass("Please pass in your password: ")
            try:
                connection = pymysql.connect(host=host,
                                             user=user,
                                             password=password,
                                             cursorclass=pymysql.cursors.DictCursor)
                connection.close()
                break
            except Exception as e:
                print(e)
                print(
                    "It seems like your login-data was not correct\nPlease also check that you're on company WIFI or connected via VPN")
        d = {'host': host, 'user': user, 'password': password}
        with open(path, "a+") as f:
            f.write(yaml.dump(d))
        return d


def establish_SQL_cursor(login_data: Dict[str, str] = None):
    login_data: dict = get_sql_login_data() if not login_data or not (
            'host' in login_data and 'user' in login_data and 'password' in login_data) else login_data
    connection = pymysql.connect(host=login_data['host'],
                                 user=login_data['user'],
                                 password=login_data['password'],
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection


def define_sql_date(request_str: str = None):
    expected_lengths = [4, 2, 2]
    if request_str:
        print(f"{lcol.OKGREEN}{request_str}{lcol.ENDC}")
    while True:
        result = user_input("What date do you choose?")
        try:
            split = result.split("-")
            if len(split) != 3:
                raise ValueError("Not enough elements in input")
            for item, ex in zip(split, expected_lengths):
                if len(item.strip("'\"")) != ex:
                    raise ValueError(f"The subpart {item} is not {ex} characters long")
            if not result.count('"') == 2 or result.count("'") == 2:
                result = f"'{result}'"
                return result
        except ValueError as e:
            print(e)


def selectTagsFromDB(cc_short: Country_Shortcode, kwds: list = None, tag_ids: list = None) -> pd.DataFrame:
    sql = construct_query_tags_keywords(cc_short, kwds, tag_ids)
    connection = establish_SQL_cursor()
    df = pd.read_sql(sql, connection)
    connection.close()
    print(df.head())
    if binaryResponse("Do you want to save the data to a file?"):
        folder = chooseFolder()
        filename = input("What filename do you want?\n").strip()
        if not '.csv' in filename:
            filename += '.csv'
        path = os.path.join(folder, filename)
        df.to_csv(path, index=False)
    return df


def construct_query_tags_keywords(cc_short: Country_Shortcode, kwds=None, tag_ids=None):
    sql = f"SELECT t.id as 'tag_id', t.name as 'tag_name', s.name as 'service_name', b.name as 'bc_name', b.elite_keywords as 'elite_keywords', b.top_keywords as 'top_keywords'" \
          f"FROM prontopro_{cc_short.lower()}.tag t" \
          f" RIGHT OUTER JOIN prontopro_{cc_short.lower()}.service s on s.id = t.service_id" \
          f" LEFT OUTER JOIN prontopro_{cc_short.lower()}.business_service bs on s.id = bs.service_id" \
          f" LEFT OUTER JOIN prontopro_{cc_short.lower()}.business b on bs.business_id = b.id"
    if kwds is not None or tag_ids is not None:
        sql += " WHERE "
        conditions = 0
    if kwds is not None:
        if isinstance(kwds, str):
            kwds = [kwds]
        for kwd in kwds:
            if not conditions == 0:
                sql += "or "
            sql += f"t.name like '%{kwd}%' or b.name like '%{kwd}%' or s.name like '%{kwd}%' or b.elite_keywords like '%{kwd}%' or b.top_keywords like '%{kwd}%'"
            conditions += 1
    if tag_ids is not None:
        if isinstance(tag_ids, str) or isinstance(tag_ids, int):
            tag_ids = [tag_ids]
        if conditions > 0:
            sql += "or "
        in_statement = "("
        for i, tid in enumerate(tag_ids):
            in_statement += f"{str(tid)}"
            if i != len(tag_ids) - 1:
                in_statement += ", "
        in_statement += ")"
        sql += f"t.id IN {in_statement}"
    print(sql)
    return sql


def format_like(item: str) -> str:
    return f"'%{item}%'"


def construct_in(items: List[Union[int, str]]) -> str:
    if not isinstance(items, list):
        items = [items]
    return f"({','.join(map(lambda x: str(x), items))})"


def construct_switch(
        switch_dict: Union[Dict[Union[str, int], List[Union[str, int]]], List[Union[str, int, float, Tuple[str, str]]]],
        column_name: str, comparison_operator: str = 'LIKE'):
    if switch_dict is not None:
        if isinstance(switch_dict, list):
            new = {}
            for combi in switch_dict:
                new[combi[0]] = combi[1]
        if comparison_operator == '==':
            raise ValueError(
                f"The comparison operator is invalid: '{comparison_operator}'. You probably want to use '='")
        s = 'CASE\n'
        for new_name, items in switch_dict.items():
            for item in items:
                s += f"WHEN {column_name} {comparison_operator} "
                if comparison_operator.upper() == 'LIKE':
                    s += format_like(item)
                else:
                    s += f'"{item}"'
                s += f" THEN '{new_name}'\n"
        s += f"ELSE {column_name}\nEND"
    else:
        s = column_name
    return s


def determine_field(conditionals: List[Union[int, str]]) -> str:
    if any(map(lambda x: isinstance(x, int), conditionals)):
        return "t2.id "
    else:
        return "t2.name "


def construct_query_tickets(cc_short: Country_Shortcode = 'IT',
                            tags_included: Dict[str, Union[List[Union[str, int]], str, int]] = None,
                            merge_tags: bool = False, incl_tag_id: bool = False,
                            tag_name_merges: Dict[str, List[str]] = None,
                            tag_id_merges: Dict[int, List[int]] = None,
                            ticket_field_likes: List[Tuple[str, str]] = None,
                            min_date_cutoff: str = '2018-01-01') -> str:
    if not cc_short or cc_short == 'IT' or cc_short == 'it':
        db = "prontopro"
    else:
        db = f'prontopro_{cc_short.lower()}'
    sql = [
        f"SELECT",
        "YEAR(t.status_new_at) as year,\n",
        "MONTH(t.status_new_at) as month,\n",
        "r.name as 'ticket_geo_region_name',\n"
    ]
    if incl_tag_id:
        if not tag_id_merges:
            sql.append("t2.id as 'tag_id',\n")
        else:
            sql.extend([construct_switch(tag_id_merges, 't2.id', comparison_operator='='), "as 'tag_id',\n"])
    if not merge_tags:
        sql.append("t2.name as 'ticket_taxonomy_tag_name',\n")
    else:
        sql.extend(
            [construct_switch(tag_name_merges, 't2.name', comparison_operator='='),
             " as 'ticket_taxonomy_tag_name',\n"])
    if ticket_field_likes:
        sql.extend([construct_switch(ticket_field_likes, 't.fields', comparison_operator='LIKE'), "as 'Options',\n"])
    sql.append("COUNT(t.fields) as 'No_of_tickets'\n")
    sql.extend([f"FROM {db}.ticket t\n",
                f"LEFT JOIN {db}.tag t2 on t.tag_id = t2.id\n",
                f"LEFT JOIN {db}.locality l on t.locality_id = l.id\n",
                f"LEFT JOIN {db}.province p on l.province_id = p.id\n",
                f"LEFT JOIN {db}.region r on p.region_id = r.id\n"])
    if tags_included or min_date_cutoff:
        sql.append("WHERE")
        conditions = 0
        if min_date_cutoff:
            if min_date_cutoff.count("'") < 2 and min_date_cutoff.count('"') < 2:
                min_date_cutoff = f"'{min_date_cutoff}'"
            sql.append(f"t.status_new_at >= {min_date_cutoff}")
            conditions += 1
        if tags_included:
            if conditions > 0:
                sql.append(" and (")
            items = []
            conditions = 0
            for cond, vals in tags_included.items():
                field = determine_field(vals)
                if conditions > 0:
                    sql.append("or")
                if cond.upper() == 'IN':
                    sql.append(f"{field} {cond} {construct_in(vals)}")
                    conditions += 1
                elif cond.upper() == 'LIKE':
                    if not isinstance(vals, list):
                        vals = [vals]
                    for val in vals:
                        if conditions > 0:
                            sql.append("or")
                        sql.append(f"{field} {cond} {format_like(val)}")
                        conditions += 1
                else:
                    sql.append(f"{field} {cond} {vals}")
            sql.append(")\n")
    sql.append("GROUP BY 1,2,3,4") if not ticket_field_likes else sql.append("GROUP BY 1,2,3,4,5")
    final_str = " ".join(sql)
    print(final_str)
    return final_str


def assemble_where(settings: Dict[str, Union[str, int, dict, List[Union[str, int]]]]) -> str:
    """
    Expected structure of the settings: {"or/and": {'operator': {'field': [items]}}}
    Args:
        settings:

    Returns:

    """
    l = []
    if len(settings) > 0:
        l.append("WHERE")
        l.extend(resolve_where_dict(settings))
        return " ".join(l) if isinstance(l, list) else l
    else:
        return ""


def resolve_where_dict(settings: Dict[str, Union[str, int, list, dict]], joiner: str = 'OR'):
    res = []
    for i, key in enumerate(settings):
        if key.lower() != 'and' and key.lower() != 'or':
            operator = key
            fields = settings[key]
            for ind, (field, conditions) in enumerate(fields.items()):
                if not isinstance(conditions, list):
                    conditions = [conditions]
                if 'LIKE' in operator.upper():
                    conditions = [format_like(condition) for condition in conditions]
                if 'IN' in operator.upper():
                    if any([isinstance(x, str) for x in conditions]):
                        conditions = [f"'{x}'" for x in conditions]
                    else:
                        conditions = [str(x) for x in conditions]
                    join = ','.join(conditions)
                    res.append(f"{field} {operator} ({join})")
                else:
                    alternatives = []
                    for condition in conditions:
                        alternatives.append(f"{field} {operator} {condition}")
                    or_join = " OR ".join(alternatives)
                    res.append(f"({or_join})")
                if ind < len(fields) - 1 and res[-1] != joiner:
                    res.append(joiner)
        else:
            res.append(f'({" ".join(resolve_where_dict(settings[key], joiner=key))})')
        if i < len(settings) - 1 and res[-1] != joiner:
            res.append(joiner)
    return res


if __name__ == '__main__':
    cc_short = 'IT'
    tags_included: dict = {'IN': [927, 792, 143, 122, 634, 132, 129, 891, 75, 646, 1, 8, 179, 309, 310, 451, 325, 299]}
    merge_tags = True
    incl_tag_id = False
    # tag_name_merges = {
    #     "Installazione barbecue": ["Installazione o sostituzione barbecue"],
    #     "Installazione pannelli solari": ["Installazione o sostituzione pannelli fotovoltaici",
    #                                       "Installazione o sostituzione pannelli solari"],
    #     'Installazione tende da sole': ['Installazione tende da sole', 'Manutenzione o riparazione tende da sole'],
    #     'Installazione piscina': ['Installazione piscine fuori terra', 'Costruzione piscina interrata'],
    #     'Riparazione console videogiochi': ['Riparazione console videogiochi'],
    #     'Installazione zanzariera': ['Installazione zanzariera'],
    #     'Sgombero cantine e soffitte': ['Sgombero cantine e soffitte'],
    #     'Aria Condizionata': ['Installazione aria condizionata', 'Manutenzione aria condizionata'],
    #     'Cura giardino': ['Cura giardino ed erba'],
    #     'Imbianchino': ['Imbiancatura o tinteggiatura pareti interne', 'Imbiancatura o tinteggiatura esterno'],
    #     'Psicologo': ['Psicologo'],
    #     'Lezioni di inglese': ['Lezioni di inglese'],
    #     'Personal trainer': ['Personal trainer']
    # }
    tag_name_merges = {
        "Outdoor": ["Installazione o sostituzione barbecue", "Installazione o sostituzione pannelli fotovoltaici",
                    "Installazione o sostituzione pannelli solari", 'Installazione tende da sole',
                    'Manutenzione o riparazione tende da sole', 'Installazione piscine fuori terra',
                    'Costruzione piscina interrata', 'Cura giardino ed erba'],
        'Wellness': ['Riparazione console videogiochi', 'Psicologo', 'Lezioni di inglese', 'Personal trainer'],
        'Homecare': ['Installazione zanzariera', 'Sgombero cantine e soffitte', 'Installazione aria condizionata',
                     'Manutenzione aria condizionata', 'Imbiancatura o tinteggiatura pareti interne',
                     'Imbiancatura o tinteggiatura esterno']
    }
    ticket_field_likes = None
    min_date_cutoff = '2018-01-01'

    query = construct_query_tickets(cc_short, tags_included=tags_included, merge_tags=merge_tags,
                                    incl_tag_id=incl_tag_id,
                                    tag_name_merges=tag_name_merges, min_date_cutoff=min_date_cutoff)
    connection = establish_SQL_cursor()
    try:
        df = pd.read_sql(query, connection)
        print(df)
        fname = os.path.join(FS.Inputs, 'IT_Summer_Ticket_Counts_Categories.csv')
        df.to_csv(fname, index=False)
        print(f"Saved file: file://{fname}")
    except Exception as e:
        print(e)
    finally:
        connection.close()
