from bs4 import BeautifulSoup
import requests
import pandas as pd
from pathlib import Path
from x_finder.utils.fixtures.args import item_category_arguments as ica


item_category_arguments = ica


BASE_URL = "https://2e.aonprd.com/"
BASE_DIR = Path(__file__).resolve().parent.parent


class SoupKitchen:
    base_url = BASE_URL
    nav_links = None
    df = None
    list_df = None
    parsed_rows = None
    dfs = []

    def __init__(self,
                 url):
        self.content = requests.get(self.base_url + url).content
        self.raw_soup = BeautifulSoup(self.content, 'html.parser')
        self.raw_soup.prettify()
        self.name = url.split('.')[0]

    def __str__(self):
        return self.raw_soup

    @staticmethod
    def get(argument, category="default"):
        if category in item_category_arguments.keys() and argument in item_category_arguments[category].keys():
            return item_category_arguments[category][argument]
        if argument in item_category_arguments["default"].keys():
            return item_category_arguments["default"][argument]
        return None

    def save(self, prefix="", suffix="", app="utils"):
        if prefix:
            prefix += "_"
        if suffix:
            suffix = "_" + suffix
        self.df_to_csv(f"{BASE_DIR}\\{app}\\fixtures\\csv\\{prefix}{self.name}{suffix}_raw.csv", self.df)

    def extract_nav_links(self):
        """ If our table is split among sub-tables, we fetch their urls.
        We store their names / urls as key / value pairs in a dict """
        main = self.raw_soup.find(id="main").span
        nav_links = main.find_all('a')
        self.nav_links = {link.get_text(): link['href'] for link in nav_links}
        self.clean_nav_links()

    def clean_nav_links(self):
        """Overload in child if needed"""
        print(self.nav_links)

    def load_table(self, category="default"):
        table_rows, headers = self.find_table(self.raw_soup)
        if not table_rows or not headers:
            return

        item_url_col = self.get("item_url_column", category)
        text_cols = self.get("text_columns", category)
        url_cols = self.get("url_columns", category)
        url_index = self.get_index(headers, item_url_col.lower())
        table_rows, counter = self.build_rows(table_rows, url_index)
        if url_index < 0:
            self.df = pd.DataFrame(data=table_rows, columns=headers)
            print("Table successfully extracted")
            print(url_index, category, item_url_col, headers)
            return

        if "nethys_url" not in headers:
            headers += ["nethys_url"]
        if not text_cols and not url_cols:
            self.df = pd.DataFrame(data=table_rows, columns=headers)
            print(f"Table extracted with {counter} missing item url{'s' if counter >1 else ''}.")
            return

        if text_cols:
            headers += [self.format_column_name(n) for n in text_cols]
        if url_cols:
            headers += [self.format_column_name(n, url=True) for n in url_cols]

        for row in table_rows:
            item_url = row[-1]
            if item_url:
                new_bowl = SoupKitchen(item_url)
                new_bowl.parse_item_data(category=category)
                for column in text_cols:
                    row.append(new_bowl.get_item_data(column))
                for column in url_cols:
                    row.append(new_bowl.get_item_data(column, url=True))
            else:
                row += [None] * len(text_cols + url_cols)
        self.df = pd.DataFrame(data=table_rows, columns=headers)
        print(f"Table extracted with {counter} missing item url{'s' if counter > 1 else ''}.")

    def find_table(self, soup):
        table = soup.find(id="main").find('table')
        table_rows = table.find_all('tr')
        if table_rows:
            try:
                headers = [self.format_column_name(th.get_text()) for th in table_rows[0].find_all('th')]
            except TypeError:
                headers = []
                print("headers not found")
            if len(table_rows) > 0:
                return table_rows[1:], headers
        print("Table not found")
        return [], []

    def build_rows(self, rows, index, length=0):
        parsed_rows = []
        counter = 0
        for row in rows:
            raw_cells = row.find_all('td')
            parsed_row = [cell.get_text() for cell in raw_cells]
            if index < 0:
                self.norm_row(parsed_row, length)
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
            self.norm_row(parsed_row, length)
            parsed_row.append(item_url)
            parsed_rows.append(parsed_row)
        return parsed_rows, counter

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

    def load_sub_tables(self, category="sources"):
        """After nav link extraction we load each table using load_table
        then contact all in one df"""
        for key, url in self.nav_links.items():
            sub_bowl = SoupKitchen(url)
            sub_bowl.load_table(category=category)
            sub_bowl.df["category"] = [key] * len(sub_bowl.df)
            self.dfs.append(sub_bowl.df)
        self.df = pd.concat(self.dfs)

    def load_source_items(self, update=False, offset=3):
        raw_items = self.raw_soup.find(id="main").find_all('u')
        item_links = self.extract_source_links(offset)
        base_columns = ["name", "nethys_url"]
        name = self.get_source_name(update)
        category_data, no_category_data = self.parse_source_items(raw_items, offset)

        new_category_dfs = {}
        for key, value in no_category_data.items():
            df = pd.DataFrame(data=value, columns=base_columns)
            new_category_dfs[key] = df
            df.to_csv(f"{BASE_DIR}\\utils\\fixtures\\csv\\{name}\\{key}_items_raw.csv",
                      sep='|',
                      index=False)

        completed_category = self.complete_all_category_items(category_data, base_columns)
        completed_category_dfs = {}
        for key in completed_category.keys():
            text_cols = [self.format_column_name(n) for n in self.get("text_columns", key)]
            url_cols = [self.format_column_name(n, url=True) for n in self.get("url_columns", key)]
            columns = base_columns + text_cols + url_cols
            if len(columns) < len(completed_category[key][0]):
                columns += ["pickup", "heightened"]
            df = pd.DataFrame(data=completed_category[key], columns=columns)
            try:
                df = self.norm_df(df, key)
            except Exception:
                print("An error occurred while norming dfs")
                pass
            app = self.get("app", key)
            completed_category_dfs[key] = df
            df.to_csv(f"{BASE_DIR}\\{app}\\fixtures\\csv\\{name}\\{key}_items_raw.csv",
                      sep='|',
                      index=False)
        self.list_df = completed_category_dfs

    def extract_source_links(self, offset):
        item_list = self.raw_soup.find(id="main").find_all('u')
        link_list = [item.a for item in item_list[offset:] if item.a is not None]
        return link_list

    def get_source_name(self, update=False):
        try:
            name = self.raw_soup.find(id="main").find(class_="title").a.get_text()
        except TypeError:
            name = "Unknown"

        if update:
            name += "_update"
        return name

    def complete_all_category_items(self, category_dict, base_columns):
        result_dict = {}
        for key, value in category_dict.items():
            data = self.complete_category_items(key, value, base_columns)
            result_dict[key] = data
        return result_dict

    def complete_category_items(self, category, data, base_columns):
        """"""
        result_data = []
        for row in data:
            try:
                item_bowl = SoupKitchen(row[1])
            except requests.exceptions.ConnectTimeout:
                print(f"Connection Timeout for {row}")
                continue
            texts = self.get("text_columns", category)
            urls = self.get("url_columns", category)
            item_bowl.parse_item_data(category=category, show=True)
            for header in texts:
                row.append(item_bowl.get_item_data(header))
            for header in urls:
                row.append(item_bowl.get_item_data(header, url=True))
            pickup = []
            heighten = []
            for key, value in item_bowl.parsed_rows.items():
                if key not in base_columns + texts + urls:
                    part = f"{key}: {self.clean(value)}"
                    if "Heightened" in key:
                        heighten.append(part)
                    else:
                        pickup.append(part)
            row.append("; ".join(pickup))
            row.append("; ".join(heighten))
            result_data.append(row)
        return result_data

    @staticmethod
    def parse_source_items(item_list, offset):
        category_data = {}
        no_category_data = {}
        for raw_item in item_list[offset:]:
            item = raw_item.a
            if item:
                item_data = [item.get_text(), item['href']]
                item_category = item_data[-1].split('.')[0].lower()
                if not item_category:
                    item_category = "unknown"
                    print(item)
                if item_category not in item_category_arguments.keys():
                    if item_category not in no_category_data.keys():
                        no_category_data[item_category] = []
                    no_category_data[item_category].append(item_data)
                else:
                    if item_category not in category_data.keys():
                        category_data[item_category] = []
                    category_data[item_category].append(item_data)
            else:
                print(f"{item} contains no link")
        return category_data, no_category_data

    @staticmethod
    def clean(value):
        clean_value = BeautifulSoup(value, 'html.parser')
        if clean_value:
            text = clean_value.get_text()
            return text.strip()

    def norm_df(self, df, key):
        if self.get("subtype", key) and "name" in df.columns:
            df["subtype"] = df.apply(
                lambda r: r["name"].split('(')[-1].split(')')[0].strip() if '(' in r["name"] else '',
                axis=1)
            df["name"] = df.apply(lambda r: r["name"].split('(')[0].strip(), axis=1)
        if "spell_level" in df.columns:
            df["spell_type"] = df.apply(
                lambda r: r["spell_level"].split(' ')[0].strip(),
                axis=1)
            df["spell_level"] = df.apply(
                lambda r: int(r["spell_level"].split(' ')[-1].strip()),
                axis=1)
        if "description" not in df.columns and "other" in df.columns:
            df.rename(columns={"other": "description"}, inplace=True)
        if "source" in df.columns and "source_page" not in df.columns:
            df["source_page"] = df.apply(
                lambda r: int(r["source"].split('pg. ')[-1].split(' ')[0].strip()) if 'pg. ' in r["source"] else 0,
                axis=1)
            df["source"] = df.apply(lambda r: r["source"].split('pg. ')[0].strip(), axis=1)
        return df

    def norm_dfs(self):
        for key in self.list_df.keys():
            df = self.list_df[key]
            self.norm_df(df, key)

    def parse_item_data(self, show=False, category="default"):
        """Once our table or list of items is loaded, we often need to
        fetch additional data on the item's page. Here we parse that page
        thanks to the markup provided to the constructor."""
        args = ["start", "end", "row_separator", "cell_separator", "tail_start", "row_sep_bis", "traits"]
        start, end, row, cell, tail, row2, traits = (self.get(arg, category) for arg in args)
        raw_data = self.raw_soup.find(id="main")
        # for child in raw_data.children:
        try:
            data = str(raw_data).split(start)[1].split(end)[0]
        except IndexError:
            print(f"Start not found : {raw_data}, {str(raw_data)}, {category}")
            raise Exception
        if start == "<b>Source":
            data = "Source" + data
        if row2:
            data = data.replace(row, row2)
            row = row2
        rows = data.split(row)
        if show:
            print(rows)
        other = ""
        if end == "<h1984":
            new_rows = []
            for row in rows:
                new_row = row.replace("<hr>", "<hr/>")
                if "<hr/>" not in new_row or other:
                    new_rows.append(row)
                else:
                    sliced_row = new_row.split("<hr/>")
                    new_rows.append(sliced_row[0])
                    other += " ".join(sliced_row[1:])
            rows = new_rows
        elif tail and tail in rows[-1]:
            last = rows[-1].split(tail)
            rows = rows[:-1] + last[:1]
            other = " ".join(last[1:])
        parsed_rows = {row.split(cell)[0]: row.split(cell)[1] for row in rows if cell in row}
        parsed_rows["Other"] = other
        if traits:
            parsed_rows["Traits"] = self.find_traits(raw_data)
        if category in ["spells", "feats", "equipment", "weapons", "armor", "shield"]:
            parsed_rows["Spell Level"] = self.find_level(raw_data)
        self.parsed_rows = parsed_rows
        if show:
            print(self.parsed_rows)

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

    def get_item_data(self, header, url=False):
        """Here we use the missing columns' headers given to the constructor to fetch
        the missing item data in the row we just parsed.
        Args:
            header  : String
            url     : Bool
        Return String
        """
        if self.parsed_rows and header and header in self.parsed_rows.keys():
            value = BeautifulSoup(self.parsed_rows[header], 'html.parser')
            if value:
                if url:
                    try:
                        url = value.find('a')['href']
                        return url
                    except TypeError:
                        return ""
                text = value.get_text()
                return text.strip()
        return ""

    @staticmethod
    def format_column_name(name, url=False):
        header = name.replace(" ", "_")
        if url and "url" not in header:
            header += "_url"
        return header.lower()

    @staticmethod
    def df_to_csv(pathfile, df):
        df.to_csv(pathfile, sep='|', index=False)

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


if __name__ == "__main__":
    bowl = SoupKitchen("Sources.aspx?ID=1")
    bowl.load_source_items()
