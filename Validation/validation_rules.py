if __name__ == '__main__':
    import sys

    sys.path.append('../')
import os
import json

from utils.Filesys import generic_FileServer as FS
from utils.misc_utils import lcol

# TODO (p2): Add validation rules for all CCs for Table and Map
# TODO (p0): Add validation from json files


static = {
    'cc-specific': {
        'Germany': {
            'regions': ["Baden-Württemberg", "Bayern", "Berlin", "Brandenburg", "Bremen",
                        "Deutschland", "Hamburg", "Hessen", "Niedersachsen", "Mecklenburg-Vorpommern",
                        "Nordrhein-Westfalen", "Rheinland-Pfalz", "Saarland", "Sachsen",
                        "Sachsen-Anhalt", "Schleswig-Holstein", "Thüringen"],
            'label_counts': {
                'chart_like': {
                    'ticket_geo_region_name': {
                        "type": "unequal",
                        "options": {
                            "Baden-Württemberg": 2,
                            "Bayern": 2,
                            "Berlin": 2,
                            "Brandenburg": 2,
                            "Bremen": 2,
                            "Deutschland": 1,
                            "Hamburg": 2,
                            "Hessen": 2,
                            "Niedersachsen": 2,
                            "Mecklenburg-Vorpommern": 2,
                            "Nordrhein-Westfalen": 2,
                            "Rheinland-Pfalz": 2,
                            "Saarland": 2,
                            "Sachsen": 2,
                            "Sachsen-Anhalt": 2,
                            "Schleswig-Holstein": 2,
                            "Thüringen": 2
                        }
                    },
                    "Country_chosen": {
                        "type": "unequal",
                        "options": {
                            0: 16 / 16,
                            1: 17 / 16
                        }
                    }
                }
            }
        },
        'France': {
            'regions': ['Île-de-France', 'Grand-Est', 'Hauts-de-France', 'Centre-Val de Loire', 'Normandie',
                        'Bourgogne-Franche-Comté', 'Pays de la Loire', 'Bretagne', 'Nouvelle-Aquitaine',
                        'Occitanie', 'Auvergne-Rhône-Alpes', "Provence-Alpes-Côte d'Azur", 'Corse', 'France'],
            'label_counts': {
                'chart_like': {
                    'ticket_geo_region_name': {
                        "type": "unequal",
                        "options": {
                            'Île-de-France': 2,
                            'Grand-Est': 2,
                            'Hauts-de-France': 2,
                            'Centre-Val de Loire': 2,
                            'Normandie': 2,
                            'Bourgogne-Franche-Comté': 2,
                            'Pays de la Loire': 2,
                            'Bretagne': 2,
                            'Nouvelle-Aquitaine': 2,
                            'Occitanie': 2,
                            'Auvergne-Rhône-Alpes': 2,
                            "Provence-Alpes-Côte d'Azur": 2,
                            'Corse': 2,
                            'France': 1
                        }
                    },
                    "Country_chosen": {
                        "type": "unequal",
                        "options": {
                            0: 13 / 13,
                            1: 14 / 13
                        }
                    }
                }
            }
        },
        'Spain': {
            'regions': ['Madrid', 'Aragón', 'Murcia', 'Valencia', 'Castilla-La Mancha', 'Navarra',
                        'Galicia', 'Islas Canarias', 'Cataluña', 'Illes Balears', 'Castilla y León',
                        'Asturias', 'Andalucía', 'País Vasco', 'Extremadura', 'Melilla', 'La Rioja',
                        'Cantabria', 'Ceuta', 'España'],
            'label_counts': {
                'chart_like': {
                    'ticket_geo_region_name': {
                        "type": "unequal",
                        "options": {
                            'Madrid': 2,
                            'Aragón': 2,
                            'Murcia': 2,
                            'Valencia': 2,
                            'Castilla-La Mancha': 2,
                            'Navarra': 2,
                            'Galicia': 2,
                            'Islas Canarias': 2,
                            'Cataluña': 2,
                            'Illes Balears': 2,
                            'Castilla y León': 2,
                            'Asturias': 2,
                            'Andalucía': 2,
                            'País Vasco': 2,
                            'Extremadura': 2,
                            'Melilla': 2,
                            'La Rioja': 2,
                            'Cantabria': 2,
                            'Ceuta': 2,
                            'España': 1
                        }
                    },
                    "Country_chosen": {
                        "type": "unequal",
                        "options": {
                            0: 19 / 19,
                            1: 20 / 19
                        }
                    }
                }
            }
        },
        'Italy': {
            'regions': ["Valle d'Aosta", 'Basilicata', 'Calabria', 'Campania', 'Emilia-Romagna',
                        'Friuli Venezia Giulia', 'Italia', 'Lazio', 'Liguria', 'Lombardia', 'Marche',
                        'Molise', 'Piemonte', 'Puglia', 'Sardegna', 'Sicilia', 'Trentino-Alto Adige',
                        'Toscana', 'Umbria', 'Veneto', 'Abruzzo'],
            'label_counts': {
                'chart_like': {
                    'ticket_geo_region_name': {
                        "type": "unequal",
                        "options": {
                            'Abruzzo': 2,
                            "Valle d'Aosta": 2,
                            'Basilicata': 2,
                            'Calabria': 2,
                            'Campania': 2,
                            'Emilia-Romagna': 2,
                            'Friuli Venezia Giulia': 2,
                            'Italia': 1,
                            'Lazio': 2,
                            'Liguria': 2,
                            'Lombardia': 2,
                            'Marche': 2,
                            'Molise': 2,
                            'Piemonte': 2,
                            'Puglia': 2,
                            'Sardegna': 2,
                            'Sicilia': 2,
                            'Trentino-Alto Adige': 2,
                            'Toscana': 2,
                            'Umbria': 2,
                            'Veneto': 2
                        }
                    }
                }
            }
        },
    },
    "general": {
        'Years': [2018, 2019, 2020],
        'Month': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        'Country_chosen': [0, 1],
        "Rank": [1, 2, 3, 4, 5],
        "Max/Min": ["NA", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    },
    'File-specific': {
        'Chart_Data': {
            'columns': ["ticket_taxonomy_tag_name", "ticket_geo_region_name", "Year", "Month", "Index",
                        "Country_chosen"],
            'label_counts': {
                'Years': {
                    'type': "unequal",
                    "options": {
                        2018: 12 / 6,
                        2019: 12 / 6,
                        2020: 6 / 6
                    }
                },
                'Months': {
                    'type': "unequal",
                    "options": {
                        0: 3 / 2,
                        1: 3 / 2,
                        2: 3 / 2,
                        3: 3 / 2,
                        4: 3 / 2,
                        5: 3 / 2,
                        6: 2 / 2,
                        7: 2 / 2,
                        8: 2 / 2,
                        9: 2 / 2,
                        10: 2 / 2,
                        11: 2 / 2
                    }
                },
            },
            "var_types": {
                "Index": "dec|0-1"
            }
        },
        'Table': {
            "columns": ["ticket_taxonomy_tag_name", "ticket_geo_region_name", "Year", "Month",
                        "Distribution_of_tickets",
                        "Country_chosen"]
        },
        'Top5': {
            "columns": ["ticket_geo_region_name", "ticket_taxonomy_tag_name", "year", "Rank", "Max",
                        "Min", 'Demand_factor_max_to_min'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'year': {
                    'type': "equal"
                }
            },
            "var_types": {
                "Rank": "int"
            }
        },
        'Ceremony': {
            "columns": ["Year", "ticket_geo_region_name", 'Wed_type', 'Distribution'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'Year': {
                    'type': "equal"
                },
                'Wed_type': {
                    'type': 'equal'
                }
            },
        },
        'Reception': {
            "columns": ["Year", "ticket_geo_region_name", 'Loc_type', 'Distribution'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'Year': {
                    'type': "equal"
                },
                'Loc_type': {
                    'type': 'equal'
                }
            },
        },
        'Services': {
            "columns": ["Year", "ticket_geo_region_name", 'Prof_type', 'Distribution'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'Year': {
                    'type': "equal"
                },
                'Prof_type': {
                    'type': 'equal'
                }
            }
        },
        'Spend': {
            "columns": ["Year", "ticket_geo_region_name", 'Spend_type', 'Distribution'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'Year': {
                    'type': "equal"
                },
                'Spend_type': {
                    'type': 'equal'
                }
            }
        },
        'Wedding_style': {
            "columns": ["Year", "ticket_geo_region_name", 'Style_type', 'Distribution'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'Year': {
                    'type': "equal"
                },
                'Style_type': {
                    'type': 'equal'
                }
            },
        },
        'Main': {
            "columns": ["Year", "ticket_geo_region_name", 'Type', 'sub_type', 'Percentage'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'Year': {
                    'type': "equal"
                },
                "Type": {
                    'type': 'equal'
                }
            },
            "var_types": {
                "Percentage": "dec|0.0000001-1"
            }
        },
        'Map': {
            "columns": ["ticket_taxonomy_tag_name", "ticket_geo_region_name", "Score"],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'ticket_taxonomy_tag_name': {
                    "type": "equal"
                }
            },
            "var_types": {
                "Score": "dec|0-10"
            }
        },
        'Summer': {
            "columns": ["Year", "ticket_geo_region_name", 'Prof_type', 'Distribution'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'Year': {
                    'type': "equal"
                },
                'Prof_type': {
                    'type': 'equal'
                }
            },
        },
        'Budget': {
            "columns": ["Year", "ticket_geo_region_name", 'Category_type', 'Distribution'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'Year': {
                    'type': "equal"
                },
                'Category_type': {
                    'type': 'equal'
                }
            },
        },
    },
    'File-type-specific': {
        'comparison': {
            'var_types': {
                'Distribution': "dec|0-1"
            },
            "columns": ["Year", "ticket_geo_region_name", 'Type', 'Distribution'],
            "label_counts": {
                'ticket_geo_region_name': {
                    "type": "equal"
                },
                'Year': {
                    'type': "equal"
                },
                'Type': {
                    'type': 'equal'
                }
            }
        }
    },

}

rules = {
    'Germany': {
        'Chart_Data': {
            "columns": static['File-specific']['Chart_Data']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                "Month": static['general']['Month'],
                "Country_chosen": static['general']['Country_chosen']
            },
            "label_counts": {
                'ticket_geo_region_name': static['cc-specific']['Germany']['label_counts']['chart_like'][
                    'ticket_geo_region_name'],
                'ticket_taxonomy_tag_name': {
                    "type": "equal"
                },
                'Year': static['File-specific']['Chart_Data']['label_counts']['Years'],
                'Month': static['File-specific']['Chart_Data']['label_counts']['Months'],
                "Country_chosen": static['cc-specific']['Germany']['label_counts']['chart_like']["Country_chosen"]
            },
            "separators": ",",
            "var_types": static['File-specific']['Chart_Data']['var_types']
        },
        'Table': {
            "columns": static['File-specific']['Table']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                "Month": static['general']['Month'],
                "Country_chosen": static['general']['Country_chosen']
            },
            "label_counts": {
                'ticket_geo_region_name': static['cc-specific']['Germany']['label_counts']['chart_like'][
                    'ticket_geo_region_name'],
                'ticket_taxonomy_tag_name': {
                    "type": "equal"
                },
                'Year': static['File-specific']['Chart_Data']['label_counts']['Years'],
                'Month': static['File-specific']['Chart_Data']['label_counts']['Months'],
                "Country_chosen": static['cc-specific']['Germany']['label_counts']['chart_like']['Country_chosen']
            },
            "separators": ",",
            "var_types": {
                "Distribution_of_tickets": "dec|0-1"
            }
        },
        'Top5': {
            "columns": static['File-specific']['Top5']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "year": static['general']['Years'],
                "Rank": static['general']['Rank'],
                "Max": static['general']['Max/Min'],
                "Min": static['general']['Max/Min']
            },
            "label_counts": static['File-specific']['Top5']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Top5']['var_types']
        },
        'Ceremony': {
            "columns": static['File-specific']['Ceremony']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                "Wed_type": ['standesamtlich', 'kirchlich', 'freie Hochzeit']
            },
            "label_counts": static['File-specific']['Ceremony']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Food': {
            "columns": static['File-type-specific']['comparison']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                "Type": ['Buffet', 'Bar', 'Catering']
            },
            "label_counts": static['File-type-specific']['comparison']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Reception': {
            "columns": static['File-specific']['Reception']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                "Loc_type": ["Hotel/ Restaurant", "Villa", "Schloss", "Landhaus"]
            },
            "label_counts": static['File-specific']['Reception']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Services_': {
            "columns": static['File-specific']['Services']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                "Prof_type": ['Bilder', 'Brautkleid', 'Hochzeitstorte ', 'Blumen']
            },
            "label_counts": static['File-specific']['Services']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Spend': {
            "columns": static['File-specific']['Spend']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                # "Spend_type": ['Bilder', 'Brautkleid', 'Hochzeitstorte ', 'Blumen']
            },
            "label_counts": static['File-specific']['Spend']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Wedding Style': {
            "columns": static['File-specific']['Wedding_style']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                "Wed_type": ['standesamtlich', 'kirchlich', 'freie Hochzeit']
            },
            "label_counts": static['File-specific']['Wedding_style']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Main': {
            "columns": static['File-specific']['Main']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                "Type": ['Kosten', 'Service', 'Zeremonie', 'Location', 'Essen'],
                "sub_type": ['Standesamtlich', 'Kirchlich', 'freie Hochzeit',
                             'Bilder', 'Brautkleid', 'Hochzeitstorte ', 'Blumen', "Hotel/ Restaurant", "Villa",
                             "Schloss", "Landhaus", 'Buffet', 'Bar', 'Catering', 'Braut Make Up', 'Blumen Hochzeit',
                             'Hochzeitsringe', 'Hochzeitscatering', 'Hochzeitstorte', 'Dekorateur für Hochzeiten',
                             'Hochzeitsfotograf', 'Tanzkurs', 'Musiker für Hochzeit']
            },
            "label_counts": static['File-specific']['Main']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Main']['var_types']
        },
        'Tags': {
            "columns": static['File-specific']['Services']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions'],
                "Year": static['general']['Years'],
                "Prof_type": ['Braut Make Up', 'Blumen Hochzeit', 'Hochzeitsringe', 'Hochzeitscatering',
                              'Hochzeitstorte', 'Dekorateur für Hochzeiten', 'Hochzeitsfotograf', 'Tanzkurs',
                              'Musiker für Hochzeit']
            },
            "label_counts": static['File-specific']['Services']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Map': {
            "columns": static['File-specific']['Map']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Germany']['regions']
            },
            "label_counts": static['File-specific']['Map']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Map']['var_types']
        },
    },
    'France': {
        'Chart_Data': {
            "columns": static['File-specific']['Chart_Data']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Month": static['general']['Month'],
                "Country_chosen": static['general']['Country_chosen']
            },
            "label_counts": {
                'ticket_geo_region_name': static['cc-specific']['France']['label_counts']['chart_like'][
                    'ticket_geo_region_name'],
                'ticket_taxonomy_tag_name': {
                    "type": "equal"
                },
                'Year': static['File-specific']['Chart_Data']['label_counts']['Years'],
                'Month': static['File-specific']['Chart_Data']['label_counts']['Months'],
                "Country_chosen": static['cc-specific']['France']['label_counts']['chart_like']['Country_chosen']
            },
            "separators": ",",
            "var_types": static['File-specific']['Chart_Data']['var_types']
        },
        'Top5': {
            "columns": static['File-specific']['Top5']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "year": static['general']['Years'],
                "Rank": static['general']['Rank'],
                "Max": static['general']['Max/Min'],
                "Min": static['general']['Max/Min']
            },
            "label_counts": static['File-specific']['Top5']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Top5']['var_types']
        },
        'Ceremony': {
            "columns": static['File-specific']['Ceremony']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Wed_type": ['Civil', 'Laïque', 'Religieux']
            },
            "label_counts": static['File-specific']['Ceremony']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Food': {
            "columns": static['File-type-specific']['comparison']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Type": ['Viande']
            },
            "label_counts": static['File-type-specific']['comparison']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Reception': {
            "columns": static['File-specific']['Reception']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Loc_type": ['Plage', 'Jardin', 'Château', 'Restaurant', 'Salle des fêtes']
            },
            "label_counts": static['File-specific']['Reception']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Services_': {
            "columns": static['File-specific']['Services']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Prof_type": ['Fleuriste', 'Wedding Planner', 'Couturier', 'Maquilleur', 'Animation', 'Photographe']
            },
            "label_counts": static['File-specific']['Services']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Spend': {
            "columns": static['File-specific']['Spend']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Spend_type": ['Premium', 'Standard']
            },
            "label_counts": static['File-specific']['Spend']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Wedding Style': {
            "columns": static['File-specific']['Wedding_style']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Wed_type": ['Nature', 'Romantique', 'Original', 'Traditionnel', 'Chic', 'Elégant', 'Bohème',
                             'Champêtre', 'Glamour', 'Vintage', 'Plage']
            },
            "label_counts": static['File-specific']['Wedding_style']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Main': {
            "columns": static['File-specific']['Main']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Type": ["Dépenses", "Services_", "Type de cérémonie", "Lieu de réception", "Nourriture",
                         "Style de mariage"],
                "sub_type": ['Civil', 'Laïque', 'Religieux', 'Plage', 'Jardin', 'Château', 'Restaurant',
                             'Salle des fêtes', 'Premium', 'Standard', 'Fleuriste', 'Wedding Planner', 'Couturier',
                             'Maquilleur', 'Animation', 'Photographe', 'Maquilleur', 'Wedding Planner', 'Couturier',
                             'Fleuriste', 'Photographe', 'Nature', 'Romantique', 'Original', 'Traditionnel', 'Chic',
                             'Elégant', 'Bohème', 'Champêtre', 'Glamour', 'Vintage', 'Plage', 'Viande']
            },
            "label_counts": static['File-specific']['Main']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Main']['var_types']
        },
        'Tags': {
            "columns": static['File-specific']['Services']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Prof_type": ['Fleuriste', 'Wedding Planner', 'Couturier', 'Maquilleur', 'Animation', 'Photographe']
            },
            "label_counts": static['File-specific']['Services']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Table': {
            "columns": static['File-specific']['Table']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years'],
                "Month": static['general']['Month'],
                "Country_chosen": static['general']['Country_chosen']
            },
            "label_counts": {
                'ticket_geo_region_name': static['cc-specific']['France']['label_counts']['chart_like'][
                    'ticket_geo_region_name'],
                'ticket_taxonomy_tag_name': {
                    "type": "equal"
                },
                'Year': static['File-specific']['Chart_Data']['label_counts']['Years'],
                'Month': static['File-specific']['Chart_Data']['label_counts']['Months'],
                "Country_chosen": static['cc-specific']['France']['label_counts']['chart_like']['Country_chosen']
            },
            "separators": ",",
            "var_types": {
                "Distribution_of_tickets": "dec|0-1"
            }
        },
        'Map': {
            "columns": static['File-specific']['Map']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions']
            },
            "label_counts": static['File-specific']['Map']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Map']['var_types']
        },
        'Budget': {
            "columns": static['File-specific']['Budget']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years']
            },
            "label_counts": static['File-specific']['Budget']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Wellness': {
            "columns": static['File-specific']['Summer']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years']
            },
            "label_counts": static['File-specific']['Summer']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Outdoors': {
            "columns": static['File-specific']['Summer']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years']
            },
            "label_counts": static['File-specific']['Summer']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Homecare': {
            "columns": static['File-specific']['Summer']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['France']['regions'],
                "Year": static['general']['Years']
            },
            "label_counts": static['File-specific']['Summer']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
    },
    'Spain': {
        'Chart_Data': {
            "columns": static['File-specific']['Chart_Data']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "Year": static['general']['Years'],
                "Month": static['general']['Month'],
                "Country_chosen": static['general']['Country_chosen']
            },
            "label_counts": {
                'ticket_geo_region_name': static['cc-specific']['Spain']['label_counts']['chart_like'][
                    'ticket_geo_region_name'],
                'ticket_taxonomy_tag_name': {
                    "type": "equal"
                },
                'Year': static['File-specific']['Chart_Data']['label_counts']['Years'],
                'Month': static['File-specific']['Chart_Data']['label_counts']['Months'],
                "Country_chosen": static['cc-specific']['Spain']['label_counts']['chart_like']['Country_chosen']
            },
            "separators": ",",
            "var_types": static['File-specific']['Chart_Data']['var_types']
        },
        'Top5': {
            "columns": static['File-specific']['Top5']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "year": static['general']['Years'],
                "Rank": static['general']['Rank'],
                "Max": static['general']['Max/Min'],
                "Min": static['general']['Max/Min']
            },
            "label_counts": static['File-specific']['Top5']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Top5']['var_types']
        },
        'Ceremony': {
            "columns": static['File-specific']['Ceremony']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "Year": static['general']['Years'],
                "Wed_type": ['Civil', 'Religiosa', 'Simbólica']
            },
            "label_counts": static['File-specific']['Ceremony']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Food': {
            "columns": static['File-type-specific']['comparison']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "Year": static['general']['Years'],
                "Type": ['menú de carnes', 'menú vegetariano']
            },
            "label_counts": static['File-type-specific']['comparison']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Reception': {
            "columns": static['File-specific']['Reception']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "Year": static['general']['Years'],
                "Loc_type": ['aldea antigua', 'restaurante', 'finca/campo', 'palacio/castillo', 'villa']
            },
            "label_counts": static['File-specific']['Reception']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Services_': {
            "columns": static['File-specific']['Services']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "Year": static['general']['Years'],
                "Prof_type": ['wedding Planer', 'florista para bodas', 'modista para bodas', 'maquillaje para bodas',
                              'servicio de catering', 'dj', 'fotógrafo de bodas']
            },
            "label_counts": static['File-specific']['Services']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Spend': {
            "columns": static['File-specific']['Spend']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "Year": static['general']['Years'],
                "Spend_type": ['Premium', 'Estándar']
            },
            "label_counts": static['File-specific']['Spend']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Wedding Style': {
            "columns": static['File-specific']['Wedding_style']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "Year": static['general']['Years'],
                "Wed_type": ['Tradicional', 'En la playa', 'Espontáneo', 'Romántico', 'Fashion/Elegante',
                             'Country Chic', 'Original/Alternativo']
            },
            "label_counts": static['File-specific']['Wedding_style']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Main': {
            "columns": static['File-specific']['Main']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "Year": static['general']['Years'],
                "Type": ['Gasto', 'Profesional más solicitado', 'Tipo de ceremonia', 'Lugar de la recepción', 'Comida',
                         'Estilo'],
                "sub_type": ['Tradicional', 'En la playa', 'Espontáneo', 'Romántico', 'Fashion/Elegante',
                             'Country Chic', 'Original/Alternativo', 'Maquilladora a Domicilio', 'Peluquero',
                             'fotografía', 'coches deportivos', 'bodas', 'ramos de flores', 'modista', 'Pastelería',
                             'Registradores', 'Dj', 'peinados para boda', 'tarta boda', 'Floristería', 'limusina',
                             'organización de eventos', 'Mago', 'Maquillaje', 'Fotógrafo de bodas', 'Joyería',
                             'Eventos Musicales', 'publicidad', 'Maquillaje', 'catering', 'Premium', 'Estándar',
                             'wedding Planer', 'florista para bodas', 'modista para bodas', 'maquillaje para bodas',
                             'servicio de catering', 'dj', 'fotógrafo de bodas', 'aldea antigua', 'restaurante',
                             'finca/campo', 'palacio/castillo', 'villa', 'menú de carnes', 'menú vegetariano', 'Civil',
                             'Religiosa', 'Simbólica', 'Gasto', 'Profesional más solicitado', 'Tipo de ceremonia',
                             'Lugar de la recepción', 'Comida', 'Estilo']
            },
            "label_counts": static['File-specific']['Main']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Main']['var_types']
        },
        'Tags': {
            "columns": static['File-specific']['Services']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Spain']['regions'],
                "Year": static['general']['Years'],
                "Prof_type": ['Maquilladora a Domicilio', 'Peluquero', 'fotografía', 'coches deportivos', 'bodas',
                              'ramos de flores', 'modista', 'Pastelería', 'Registradores', 'Dj', 'peinados para boda',
                              'tarta boda', 'Floristería', 'limusina', 'organización de eventos', 'Mago', 'Maquillaje',
                              'Fotógrafo de bodas', 'Joyería', 'Eventos Musicales', 'publicidad', 'Maquillaje',
                              'catering']
            },
            "label_counts": static['File-specific']['Services']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Map': {
            "columns": static['File-specific']['Map']['columns'],
            "labels": {
                "ticket_geo_region_name": ['Madrid', 'Aragón', 'Murcia', 'Valencia', 'Castilla-La Mancha', 'Navarra',
                                           'Galicia', 'Islas Canarias', 'Cataluña', 'Illes Balears', 'Castilla y León',
                                           'Asturias', 'Andalucía', 'País Vasco', 'Extremadura', 'Melilla', 'La Rioja',
                                           'Cantabria', 'Ceuta']
            },
            "label_counts": static['File-specific']['Map']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Map']['var_types']
        },
    },
    'Italy': {
        'Chart_Data': {
            "columns": static['File-specific']['Chart_Data']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Italy']['regions'],
                "Year": static['general']['Years'],
                "Month": static['general']['Month'],
                "Country_chosen": static['general']['Country_chosen']
            },
            "label_counts": {
                'ticket_geo_region_name': static['cc-specific']['Italy']['label_counts']['chart_like'][
                    'ticket_geo_region_name'],
                'ticket_taxonomy_tag_name': {
                    "type": "equal"
                },
                'Year': static['File-specific']['Chart_Data']['label_counts']['Years'],
                'Month': static['File-specific']['Chart_Data']['label_counts']['Months'],
                "Country_chosen": static['cc-specific']['France']['label_counts']['chart_like']['Country_chosen']
            },
            "separators": ",",
            "var_types": static['File-specific']['Chart_Data']['var_types']
        },
        'Top5': {
            "columns": static['File-specific']['Top5']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Italy']['regions'],
                "year": static['general']['Years'],
                "Rank": static['general']['Rank'],
                "Max": static['general']['Max/Min'],
                "Min": static['general']['Max/Min']
            },
            "label_counts": static['File-specific']['Top5']['label_counts'],
            "separators": ",",
            "var_types": static['File-specific']['Top5']['var_types']
        },
        'Ceremony': {
            "columns": static['File-specific']['Ceremony']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Italy']['regions'],
                "Year": static['general']['Years'],
                "Wed_type": ['Religioso', 'Simbolico', 'Civile']
            },
            "label_counts": static['File-specific']['Ceremony']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Food': {
            "columns": static['File-type-specific']['comparison']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Italy']['regions'],
                "Year": static['general']['Years'],
                "Type": ["Catering d'asporto", 'Servizio al tavolo', 'Isole (più tavoli)', 'Buffet (unico tavolo)',
                         'Non lo so', 'Servizio al vassoio']
            },
            "label_counts": static['File-type-specific']['comparison']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Reception': {
            "columns": static['File-specific']['Reception']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Italy']['regions'],
                "Year": static['general']['Years'],
                "Loc_type": ['Ristorante', 'Cascina', 'Borgo antico', 'Villa', 'Castello']
            },
            "label_counts": static['File-specific']['Reception']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Spend': {
            "columns": static['File-specific']['Spend']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Italy']['regions'],
                "Year": static['general']['Years'],
                "Spend_type": ["premium", "standard"]
            },
            "label_counts": static['File-specific']['Spend']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Wedding Style': {
            "columns": static['File-specific']['Wedding_style']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Italy']['regions'],
                "Year": static['general']['Years'],
                "Wed_type": ['Tradizionale', 'Romantico/Fiabesco', 'Fashion/Elegante', 'Originale/Alternativo',
                             'Buocolico/Spontaneo', 'Country chic', 'In spiaggia']
            },
            "label_counts": static['File-specific']['Wedding_style']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
        'Main': {
            "columns": static['File-specific']['Main']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Italy']['regions'],
                "Year": static['general']['Years'],
                "Type": ['Costo', 'Professionista più richiesto', 'Tipo di cerimonia', 'Location', 'Cibo',
                         'Stile'],
                "sub_type": ['Tradizionale', 'Romantico/Fiabesco', 'Fashion/Elegante', 'Originale/Alternativo',
                             'Buocolico/Spontaneo', 'Country chic', 'In spiaggia', 'Fotografo di matrimonio',
                             'Dj per matrimoni', 'Affitto location matrimonio', 'Animazione per matrimonio',
                             'Fiorista matrimonio', 'Catering matrimoni', 'Trucco sposa', 'Wedding planner',
                             'Video matrimonio', 'Allestimenti e decorazioni per matrimonio',
                             'Abito da sposa', 'Torta nuziale', 'Parrucchiere matrimonio', "premium", "standard",
                             'Ristorante', 'Cascina', 'Borgo antico', 'Villa', 'Castello', "Catering d'asporto",
                             'Servizio al tavolo', 'Isole (più tavoli)', 'Buffet (unico tavolo)', 'Non lo so',
                             'Servizio al vassoio', 'Religioso', 'Simbolico', 'Civile']
            },
            "label_counts": static['File-specific']['Main']['label_counts'],
            "separators": ",",
            "var_types": {
                "Percentage": "dec|0-1"
            }
        },
        'Tags': {
            "columns": static['File-specific']['Services']['columns'],
            "labels": {
                "ticket_geo_region_name": static['cc-specific']['Italy']['regions'],
                "Year": static['general']['Years'],
                "Prof_type": ['Affitto location matrimonio', 'Catering matrimoni', 'Dj per matrimoni',
                              'Fiorista matrimonio', 'Fotografo di matrimonio', 'Trucco sposa', 'Wedding planner']
            },
            "label_counts": static['File-specific']['Services']['label_counts'],
            "separators": ",",
            "var_types": static['File-type-specific']['comparison']['var_types']
        },
    }
}

for cc in rules:
    folder = os.path.join(FS.Validation, cc)
    if os.path.exists(folder):
        for file in os.listdir(folder):
            if ".json" in file:
                type_name = file.split(".jso")[0]
                with open(os.path.join(folder, file), "r") as f:
                    j = json.loads(f.read())
                rules[cc][type_name] = j


ccs = []
cc_file_type_counts = []
for cc, cc_items in rules.items():
    cc_file_type_counts.append(len(cc_items))
    ccs.append(cc)

if max(cc_file_type_counts) > min(cc_file_type_counts):
    print(
        f"{lcol.WARNING}FROM RULES SOURCE: Please be aware, there are more file types set-up for a country than another")
    print(ccs)
    print(cc_file_type_counts)
    print(lcol.ENDC)

if __name__ == '__main__':
    print(rules)
