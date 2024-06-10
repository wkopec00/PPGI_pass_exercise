import pandas as pd
import requests
import zipfile
import os

path = 'data/'
file_name = 'Stacje_klimat_utf-8.csv'

url = 'https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/miesieczne/klimat/'
zip_name = '2001_m_k.zip'
catalog = '2001/'
save_path = 'download/'

def main():
    correct_csv(path+file_name)
    download_zip(url+catalog+zip_name, save_path, catalog)
    extract_zip(save_path+catalog+'1951_1955_m_k.zip')


def download_zip(url: str, save_path: str, catalog: str):
    """
    Download zip file from url to save_path/catalog
    :param url: url to zip file
    :param save_path:
    :param catalog:
    :return: None
    """
    r = requests.get(url)
    os.makedirs(save_path+catalog, exist_ok=True)
    with open(save_path+catalog+'1951_1955_m_k.zip', 'wb') as f:
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
    with open(name, 'r') as f:
        f.readline()  # skip first line

        lines = f.readlines()
        for i, line in enumerate(lines):
            if len(line.split(' ')) < 17:
                lines[i] = correct_line(line)

    save_csv(path + 'correct-' + file_name, lines)

    return None


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


if __name__ == '__main__':
    main()
