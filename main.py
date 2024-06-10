import pandas as pd

data_path = 'data\Stacje_klimat_utf-8.csv'

def main():
    correct_csv(data_path)


def correct_csv(name: str) -> None:
    """
    this function search lines where is no seconds and fill them with '0', then save it
    to csv file named 'correct-{name}'
    :param name: name of the csv file
    :return: None
    """
    with open('Stacje_klimat_utf-8.csv', 'r') as f:
        f.readline()     # pominiecie pierwszej linii

        lines = f.readlines()
        for i, line in enumerate(lines):
            if len(line.split(' ')) < 17:
                lines[i] = correct_line(line)

    save_csv('correct-'+name, lines)

    return None


def save_csv(name: str, lines: list) -> None:
    """
    Save a csv file
    :param name: name of the file
    :param lines: list of lines
    :return: None
    """
    with open(name, 'w') as f:
        print('linie:')
        for line in lines:
            f.write(line)

    return None


def correct_line(line: str) -> str:
    """
    function fills lines where seconds are missing
    :param line: string with data without seconds
    :return: line with seconds filled
    """
    splited_line = line.split(' ')

    # create new line filling 0 where seconds should be
    new_line = splited_line[0:4]
    new_line.append(0)
    new_line.extend(splited_line[4:6])
    new_line.append(0)
    new_line.extend(splited_line[6:])

    # conversion elements to str to make join possible and join
    new_line = [str(item) for item in new_line]
    joined_string = ' '.join(new_line)

    return joined_string


if __name__ == '__main__':
    main()
