import os
import json
from typing import List, Dict
from utils.Filesys import generic_FileServer as FS

template_constants = {
    'REGIONS_GERMANY': ["Baden-Württemberg", "Bayern", "Berlin", "Brandenburg", "Bremen",
                        "Deutschland", "Hamburg", "Hessen", "Niedersachsen", "Mecklenburg-Vorpommern",
                        "Nordrhein-Westfalen", "Rheinland-Pfalz", "Saarland", "Sachsen",
                        "Sachsen-Anhalt", "Schleswig-Holstein", "Thüringen"],
    'REGIONS_FRANCE': ['Île-de-France', 'Grand-Est', 'Hauts-de-France', 'Centre-Val de Loire', 'Normandie',
                       'Bourgogne-Franche-Comté', 'Pays de la Loire', 'Bretagne', 'Nouvelle-Aquitaine',
                       'Occitanie', 'Auvergne-Rhône-Alpes', "Provence-Alpes-Côte d'Azur", 'Corse', 'France'],
    'REGIONS_SPAIN': ['Madrid', 'Aragón', 'Murcia', 'Valencia', 'Castilla-La Mancha', 'Navarra',
                      'Galicia', 'Islas Canarias', 'Cataluña', 'Illes Balears', 'Castilla y León',
                      'Asturias', 'Andalucía', 'País Vasco', 'Extremadura', 'Melilla', 'La Rioja',
                      'Cantabria', 'Ceuta', 'España'],
    'REGIONS_ITALY': ["Valle d'Aosta", 'Basilicata', 'Calabria', 'Campania', 'Emilia-Romagna',
                      'Friuli Venezia Giulia', 'Italia', 'Lazio', 'Liguria', 'Lombardia', 'Marche',
                      'Molise', 'Piemonte', 'Puglia', 'Sardegna', 'Sicilia', 'Trentino-Alto Adige',
                      'Toscana', 'Umbria', 'Veneto', 'Abruzzo']
}


def handleGUIData(data, logging_func):
    print(f"Data passed In handle GUI {data}")
    country: str = data.get('country', False)
    fileName: str = data.get('title', False)
    columns: List[str] = data.get('colNames', False)
    labels: Dict[str, List[str]] = data.get('labels', False)
    label_counts = data.get('labelCounts', False)
    separators = data.get('separators', False)
    var_types = data.get('variableTypes', False)
    if not country or not fileName:
        return 'Error'
    else:
        dictionary = {'separators': separators}
        if isinstance(columns, list):
            dictionary['columns'] = columns
        if isinstance(labels, dict):
            for col, label_list in labels.items():
                if isinstance(label_list, str) or (isinstance(label_list, list) and len(label_list) == 1):
                    token = label_list if isinstance(label_list, str) else label_list[0]
                    labels[col] = template_constants.get(token, label_list)  # tries to match a label to a constant for faster addition of repetitive labels
            dictionary['labels'] = labels
        if isinstance(label_counts, dict):
            dictionary['label_counts'] = [{'type': val} for val in label_counts]
        if isinstance(var_types, dict):
            dictionary['var_types'] = var_types
        fname = fileName if fileName.endswith('.json') else f"{fileName}.json"
        folderpath = os.path.join(FS.Validation, country)
        if not os.path.exists(folderpath):
            os.makedirs(folderpath)
        with open(os.path.join(folderpath, fname), "w+") as f:
            f.write(json.dumps(dictionary))
        logging_func(f"Saved file: {os.path.join(folderpath, fname)}")
        return f"Saved file: {os.path.join(folderpath, fname)}"
