import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from django.conf import settings
from os import makedirs
from pathlib import Path
from x_finder.utils.fixtures.args import item_category_arguments as ica
import pandas as pd
from bs4 import BeautifulSoup

item_category_arguments = ica
BASE_DIR = Path(__file__).resolve().parent.parent


def run_sql(sql):
    c = psycopg2.connect(database=settings.DATABASES['default']['USER'],
                         user=settings.DATABASES['default']['USER'],
                         password=settings.DATABASES['default']['PASSWORD'])
    c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = c.cursor()
    cur.execute(sql)
    c.close()


class U:
    @staticmethod
    def get_me_out(argument, category="default"):
        if not argument and category in item_category_arguments.keys():
            return category
        if category in item_category_arguments.keys() and argument in item_category_arguments[category].keys():
            return item_category_arguments[category][argument]
        if argument in item_category_arguments["default"].keys():
            return item_category_arguments["default"][argument]
        return None

    @staticmethod
    def save(df, name, directory=None, app="utils"):
        path = f"{BASE_DIR}\\{app}\\fixtures\\csv\\"
        if directory:
            path += f"{directory}\\"
        makedirs(path, exist_ok=True)
        df.to_csv(f"{path}\\{name}.csv", sep='|', index=False)
        print(f"{name} successfully saved at {path}.")

    @staticmethod
    def load(name, app="utils", directory=None, suffix="raw"):
        if directory:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{directory}\\{name}_{suffix}.csv"
        else:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{name}{suffix}.csv"
        df = pd.read_csv(pathfile, delimiter="|")
        return df

    @staticmethod
    def snake_to_under(snake):
        if snake is None or not isinstance(snake, str) or len(snake) == 0:
            print("No string to convert")
            return snake
        under = snake[0].lower()
        if len(snake) == 1:
            return under
        for char in snake[1:]:
            if char.isupper() or char == " ":
                under += "_"
            under += char.lower()
        return under

    @staticmethod
    def load_nested_table(child):
        table = []
        row_len = 0
        for tr in child.find_all('tr'):
            row = [td.get_text() for td in tr.find_all('td')]
            if len(row) < row_len:
                row = [table[-1][0]] + row
            row_len = max(row_len, len(row))
            table.append(row)
        return table

    @staticmethod
    def clean_text(raw_string):
        if raw_string is None:
            return ''
        text = raw_string.strip(' ,;\n\r—').replace('\xa0', ' ')
        if text.startswith(']'):
            if len(text) < 2:
                return ''
            text = text[1:]
        if text.endswith('['):
            text = text[:-1]
        return text

    @staticmethod
    def format_key(nav_string=None, text=""):
        if nav_string is not None:
            key = nav_string.get_text().strip(':')
        else:
            key = text
        separator = '('
        if ':' in key:
            separator = ':'
        key_parts = key.split(separator)
        key = key_parts[0].strip(' ,;').lower().replace(' ', '_').replace('-', '_').replace('–', '_')
        value = ""
        if len(key_parts) > 1:
            value = separator.join(key_parts[1:]).strip('( ,;)')
        return key, value

    @staticmethod
    def get_href(child):
        if child is None:
            return ""
        if child.name == 'a':
            return child["href"]
        try:
            link = child.find('a')["href"]
            return link
        except AttributeError:
            return ""
        except TypeError:
            return ""

    @staticmethod
    def clean(value):
        clean_value = BeautifulSoup(value, 'html.parser')
        if clean_value:
            text = clean_value.get_text()
            return text.strip()

    @staticmethod
    def format_column_name(name, url=False):
        header = name.replace(" ", "_").replace("-", "_")
        if url and "url" not in header:
            header += "_url"
        return header.lower()

    @staticmethod
    def load_fixture(app, name, raw=True):
        suffix = ""
        if raw:
            suffix = "_raw"
        pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{name}{suffix}.csv"
        df = pd.read_csv(pathfile, delimiter="|")
        return df

    @staticmethod
    def translate_date(mm_dd_yyyy):
        if '/' in mm_dd_yyyy:
            parts = mm_dd_yyyy.split('/')
        elif '-' in mm_dd_yyyy:
            parts = mm_dd_yyyy.split('-')
        else:
            return None
        return f"{parts[-1]}-{parts[0]}-{parts[1]}"

    @staticmethod
    def find_table(soup):
        table = soup.find(id="main").find('table')
        table_rows = table.find_all('tr')
        if table_rows:
            try:
                headers = [U.format_column_name(th.get_text()) for th in table_rows[0].find_all('th')]
            except TypeError:
                headers = []
                print("headers not found")
            if len(table_rows) > 0:
                return table_rows[1:], headers
        print("Table not found")
        return [], []

    @staticmethod
    def find_traits(data, separator="!", singles=("traituncommon", "traitrare", "traitunique", ), multiple="trait"):
        """Traits have a many-to-many relationship with most of the items we scrap. We need to extract their name.
        Arg: BS4 String
        Return : String """
        traits = []
        for single in singles:
            traits += [data.find(class_=single)]
        traits += data.find_all(class_=multiple)
        return separator.join([trait.a.get_text() for trait in traits if trait is not None])

    @staticmethod
    def find_level(data):
        """We target 1 text in a broken title supposed to hold the 'Level' of the target item as subtitle.
        Arg: data: String like object
        Return : String or None if str(data) is None
        """
        try:
            sub_title = str(data).split('<span style="margin-left:auto; margin-right:0">')[-1].split('</span>')[0]
            return sub_title
        except TypeError:
            return None

    @staticmethod
    def get_ability_modifier(boosts, flaw, ability):
        print(flaw, boosts)
        if isinstance(flaw, list) and ability in "".join(flaw).lower():
            print(flaw)
            return -2
        if isinstance(boosts, list):
            for boost in boosts:
                print(boosts)
                if boost and ability in boost.lower():
                    return 2
        return 0

    @staticmethod
    def numerize_level(level):
        if level is None:
            return 0
        if isinstance(level, list):
            level = level[0]
        if isinstance(level, int):
            return level
        level = str(level)
        if level:
            level = str(level).split(' ')[-1].strip()
            if level.isnumeric():
                return int(level)
            if level.endswith('+'):
                return -1
        return 0

    @staticmethod
    def get_index(list_of_strings, string):
        try:
            index = list_of_strings.index(string)
            return index
        except ValueError:
            return -1

    @staticmethod
    def norm_row(row, length):
        if length > 0:
            diff = len(row) - length
            if diff < 0:
                row += [None] * abs(diff)
                print(f"Warning: A row was shorter than expected: {row}.")
            if diff > 0:
                excess = row[length - 1:]
                row = row[:length-1]
                row = row[:-1] + "!".join([row[-1]] + excess)
                print(f"Warning: A row was longer than expected. Last table column hold concatenated data: {row}.")
        return row

    @staticmethod
    def build_rows(rows, index, length=0):
        parsed_rows = []
        counter = 0
        for row in rows:
            raw_cells = row.find_all('td')
            parsed_row = [cell.get_text() for cell in raw_cells]
            if index < 0:
                U.norm_row(parsed_row, length)
                parsed_rows.append(parsed_row)
                continue
            try:
                item_url = raw_cells[index].a['href']
            except TypeError:
                item_url = ""
                print(f"Warning: item url link not found for row {row}.")
                counter += 1
            except IndexError:
                item_url = ""
                counter += 1
                print(f"Warning: row {row}is too short for url index: {raw_cells}")
            U.norm_row(parsed_row, length)
            parsed_row.append(item_url)
            parsed_rows.append(parsed_row)
        return parsed_rows, counter
