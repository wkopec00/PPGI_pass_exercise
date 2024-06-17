import pandas as pd
import os
import re
import time
import zipfile
import requests
from bs4 import BeautifulSoup
from pyproj import Transformer


def main():
    path_data = 'data/'

    url = 'https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/miesieczne/klimat/'
    save_path = 'download/'

    correct_csv(path_data, 'Stacje_klimat_utf-8.csv')       # correct Stacje-klimat_utf-8.csv

    links = get_directory_structure(url)

    for year in links:
        download_zip(url+year+year[:-1]+'_m_k.zip', save_path, year)
        extract_zip(save_path+year+'data.zip', path_data+'all_years_csv')
        time.sleep(3)       # delay for safety

    df_stations_coord = import_stations_coord(path_data+'correct-Stacje_klimat_utf-8.csv')
    deg_min_sec2deg(df_stations_coord, 'lat')
    deg_min_sec2deg(df_stations_coord, 'lon')

    transform_coordinates(df_stations_coord)

    df_stations_connector = import_stations_connector(path_data+'wykaz_stacji.csv')

    merged_df = pd.merge(df_stations_coord, df_stations_connector, on='short number')

    files = os.listdir(path_data+'all_years_csv')
    all_data = []
    for file in files:
        df = import_stations_measurement(path_data+'all_years_csv/'+file)
        all_data.append(df)

    df_all_data = pd.concat(all_data, ignore_index=True)

    merged_df = pd.merge(merged_df, df_all_data, on='long number')

    new_order = ['short number', 'long number', 'x_1992', 'y_1992', 'station', 'location', 'year', 'month',
                 'sr_miesiac_temp', 'stat_temp', 'sr_miesiac_wilg', 'stat_wilg',
                 'sr_miesiac_wind', 'stat_wind', 'sr_miesiac_cloud', 'stat_cloud']
    merged_df = merged_df.reindex(columns=new_order)
    merged_df.to_excel('result/all_data_merged.xlsx', index=False)

    create_every_year_xlsx('sr_miesiac_wilg', merged_df, df_stations_coord)


def create_every_year_xlsx(value: str, merged_df, df_stations_coord):
    df_dict_years = {rok: merged_df[merged_df['year'] == rok].pivot(
        index='short number', columns='month', values=value) for rok in merged_df['year'].unique()}

    for rok, df_roku in df_dict_years.items():
        m_df = pd.merge(df_roku, df_stations_coord, on='short number')

        folder_path = f'result/{value}/'
        os.makedirs(folder_path, exist_ok=True)

        m_df.to_excel(f'{folder_path + str(rok)}.xlsx', index=False)


def get_directory_structure(url_to_download):
    r = requests.get(url_to_download)

    soup = BeautifulSoup(r.text, 'html.parser')

    links = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and not href.startswith('?') and not href.startswith('/') and not href.endswith('.txt'):
            links.append(href)

    return links


def transform_coordinates(df: pd.DataFrame):
    transformer = Transformer.from_crs('EPSG:4326', 'EPSG:2180')
    df['x_1992'], df['y_1992'] = transformer.transform(df['lon'], df['lat'])
    del df['lon'], df['lat']


def deg_min_sec2deg(df: pd.DataFrame, begin: str):
    df[begin] = df[begin + ' deg'] + df[begin + ' min']/60 + df[begin + ' sec']/3600
    del df[begin + ' deg']
    del df[begin + ' min']
    del df[begin + ' sec']


def import_stations_connector(csv):
    return pd.read_csv(csv, quotechar='"', sep=',', encoding='ISO-8859-1',
                       names=['long number', 'location', 'short number'])


def import_stations_measurement(csv):
    df = pd.read_csv(csv, quotechar='"', sep=',', encoding='ISO-8859-1',
                     names=['long number', 'location', 'year', 'month',
                     'sr_miesiac_temp', 'stat_temp', 'sr_miesiac_wilg', 'stat_wilg',
                     'sr_miesiac_wind', 'stat_wind', 'sr_miesiac_cloud', 'stat_cloud'])
    df.loc[df['stat_temp'] == 8, 'sr_miesiac_temp'] = pd.NA
    df.loc[df['stat_wilg'] == 8, 'sr_miesiac_wilg'] = pd.NA
    df.loc[df['stat_wind'] == 8, 'sr_miesiac_wind'] = pd.NA
    df.loc[df['stat_cloud'] == 8, 'sr_miesiac_cloud'] = pd.NA
    return df


def import_stations_coord(csv):
    return pd.read_csv(csv, sep=' ', names=['short number', 'location',
                                            'lat deg', 'lat min', 'lat sec',
                                            'lon deg', 'lon min', 'lon sec',
                                            'station', 'y1', 'y2', 'y3', 'y4', 'y5', 'y6', 'y7', 'y8'])


def download_zip(link: str, path_to_save: str, catalog_to_save: str):
    """
    Download zip file from url to save_path/catalog
    :param link: url to zip file
    :param path_to_save:
    :param catalog_to_save:
    :return: None
    """
    r = requests.get(link)
    os.makedirs(path_to_save + catalog_to_save, exist_ok=True)
    with open(path_to_save + catalog_to_save + 'data.zip', 'wb') as f:
        f.write(r.content)

    return None


def extract_zip(path_to_zip: str, path_to_save):
    """
    Extract zip file from path_to_zip and saves in path_to_save directory
    :param path_to_zip:
    :param path_to_save
    :return: None
    """
    with zipfile.ZipFile(path_to_zip, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if 'k_m_t' in file_info.filename:
                zip_ref.extract(file_info, path_to_save)
                print(f'Extracted {file_info.filename} to {path_to_save}')

    return None


def correct_csv(path: str, file_name: str) -> None:
    """
    this function search lines where is no seconds and fill them with '0', then save it
    to csv file named 'correct-{name}'
    :param path: path to csv file
    :param file_name: name of the csv file
    :return: None
    """
    new_lines = []
    with open(path+file_name, 'r') as f:
        f.readline()  # skip first line

        lines = f.readlines()
        for i, line in enumerate(lines):
            new_line = fix_split_names(line)    # check if names doesnt split by ' ', then fix it by '_'
            new_line = fix_split_names(new_line)    # double for 3-items names
            if len(new_line.split(' ')) < 16:
                new_line = correct_line(new_line)

            if index_of_start_data(new_line.split(' ')) != 9:
                print(f'Bład w danych, pomijam tą stacje (Nr stacji: {line.split(" ")[0]})')

                continue
            new_lines.append(new_line)

    save_csv(path + 'correct-' + file_name, new_lines)


def correct_line(line: str) -> str:
    """
    function fills lines where seconds in lon/lat are missing
    :param line: string with data without seconds
    :return: line with seconds filled
    """
    splited_line = line.split(' ')

    # create new line filling 0 where seconds should be
    new_line = splited_line[0:4]
    new_line.append('0')
    new_line.extend(splited_line[4:6])
    new_line.append('0')
    new_line.extend(splited_line[6:])

    # conversion elements to str to make join possible and join
    new_line = [str(item) for item in new_line]
    joined_string = ' '.join(new_line)

    return joined_string


def fix_split_names(line: str) -> str:
    row = line.split(' ')
    fixed_row = []

    index = index_of_start_data(row)
    rest = row[index:]     # save rest of line for later
    remove_elements_from_index(row, index)     # delete rest from main row

    skip = False
    for i in range(len(row)):
        if i == len(row):
            break
        if skip:
            skip = False
            continue
        if re.match(r'^\d+$', row[i]):  # Jeśli element jest liczbą (np. kod stacji)
            fixed_row.append(row[i])
        elif i < len(row) - 1 and not re.match(r'^\d+$', row[i + 1]):    # kolejny nie jest liczba
            fixed_row.append(row[i] + '-' + row[i + 1])
            skip = True  # Pomijamy następny element, ponieważ już go połączyliśmy
        else:
            fixed_row.append(row[i])

    joined_string = ' '.join(fixed_row+rest)

    return joined_string


def index_of_start_data(list_line: list) -> int:
    """
    return the index of item -------- or KKKKKKKK etc.
    :param list_line:
    :return: index in list, -1 if not found
    """
    for item in list_line:
        if not re.match(r'^[KN\-]+$', item):
            continue
        else:
            return list_line.index(item)

    return -1


def save_csv(name: str, lines: list) -> None:
    """
    Save a csv file
    :param name: name of the file
    :param lines: list of lines
    :return: None
    """
    with open(name, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line)

    return None


def remove_elements_from_index(ls: list, index: int) -> None:
    # remove from index to the end
    del ls[index:]


if __name__ == '__main__':
    main()
