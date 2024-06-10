import pandas as pd
import requests
import zipfile
import os
import re

path = 'data/'
file_name = 'Stacje_klimat_utf-8.csv'

url = 'https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/miesieczne/klimat/'
zip_name = '2001_m_k.zip'
catalog = '2001/'
save_path = 'download/'


def main():
    # correct_csv(path+file_name)

    # download_zip(url+catalog+zip_name, save_path, catalog)
    # extract_zip(save_path+catalog+'1951_1955_m_k.zip')

    df_stations_coord = import_stations_coord(path+'correct-'+file_name)
    deg_min_sec2deg(df_stations_coord, 'lat')
    deg_min_sec2deg(df_stations_coord, 'lon')

    df_stations_measurement = import_stations_measurement(save_path+catalog+'k_m_t_2001.csv')
    df_stations_connector = import_stations_connector(path+'wykaz_stacji.csv')

    merged_df = pd.merge(df_stations_coord, df_stations_connector, on='short number')
    merged_df = pd.merge(merged_df, df_stations_measurement, on='long number')

    print(merged_df)
    new_order = ['short number', 'long number', 'lat', 'lon', 'station', 'location', 'year', 'month',
                 'sr_miesiac_temp', 'stat_temp', 'sr_miesiac_wilg', 'stat_wilg',
                 'sr_miesiac_wind', 'stat_wind', 'sr_miesiac_cloud', 'stat_cloud']
    merged_df = merged_df.reindex(columns=new_order)
    merged_df.to_excel('test.xlsx', index=False)
    print(merged_df)


def deg_min_sec2deg(df: pd.DataFrame, begin: str):
    df[begin] = df[begin + ' deg'] + df[begin + ' min']/60 + df[begin + ' sec']/3600
    del df[begin + ' deg']
    del df[begin + ' min']
    del df[begin + ' sec']


def import_stations_connector(csv):
    return pd.read_csv(csv, quotechar='"', sep=',', encoding='ISO-8859-1',
                       names=['long number', 'location', 'short number'])


def import_stations_measurement(csv):
    return pd.read_csv(csv, quotechar='"', sep=',', encoding='ISO-8859-1',
                       names=['long number', 'location', 'year', 'month',
                              'sr_miesiac_temp', 'stat_temp', 'sr_miesiac_wilg', 'stat_wilg',
                              'sr_miesiac_wind', 'stat_wind', 'sr_miesiac_cloud', 'stat_cloud'])


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
    with open(path_to_save + catalog_to_save + '1951_1955_m_k.zip', 'wb') as f:
        f.write(r.content)

    return None


def extract_zip(path_to_zip: str):
    """
    Extract zip file from path_to_zip and saves in same path
    :param path_to_zip:
    :return: None
    """
    with zipfile.ZipFile(path_to_zip, 'r') as zip_ref:
        zip_ref.extractall(save_path+catalog)

    return None


def correct_csv(name: str) -> None:
    """
    this function search lines where is no seconds and fill them with '0', then save it
    to csv file named 'correct-{name}'
    :param name: name of the csv file
    :return: None
    """
    new_lines = []
    with open(name, 'r') as f:
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
    :return: index in list
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
