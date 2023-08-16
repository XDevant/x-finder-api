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
        """
        if self.save:
            self.df_to_csv(f"{BASE_DIR}\\{self.app}\\fixtures\\{self.name}_raw.csv", self.df)
        """

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
        table = self.raw_soup.find(id="main").find('table')
        table_rows = table.find_all('tr')
        if len(table_rows) <= 1:
            print("table not found")
            return 0
        headers = [self.format_column_name(th.get_text()) for th in table_rows[0].find_all('th')]
        if len(headers) == 0:
            print("headers not found")
            return 0

        item_url_col = self.get("item_url_column", category)
        text_cols = self.get("text_columns", category)
        url_cols = self.get("url_columns", category)
        url_index = -1
        if item_url_col:
            headers += ["nethys_url"]
            try:
                url_index = headers.index(item_url_col)
            except ValueError:
                pass
            if text_cols:
                headers += [self.format_column_name(n) for n in text_cols]
            if url_cols:
                headers += [self.format_column_name(n, url=True) for n in url_cols]

        df = pd.DataFrame(columns=headers)
        for i in range(1, len(table_rows)):
            raw_row = table_rows[i].find_all('td')
            row = [td.get_text() for td in raw_row]
            if url_index >= 0:
                item_url = raw_row[url_index].a['href']
                row.append(item_url)
                if text_cols or url_cols:
                    new_bowl = SoupKitchen(item_url)
                    new_bowl.parse_item_data(category=category)
                    for column in text_cols:
                        row.append(new_bowl.get_item_data(column))
                    for column in url_cols:
                        row.append(new_bowl.get_item_data(column, url=True))

            df.loc[i] = row
        self.df = df

    def load_sub_tables(self, category="default"):
        """After nav link extraction we load each table using load_table
        then contact all in one df"""
        for key, url in self.nav_links.items():
            sub_bowl = SoupKitchen(url)
            sub_bowl.load_table(category=category)
            sub_bowl.df["Category"] = [key] * len(sub_bowl.df)
            self.dfs.append(sub_bowl.df)
        self.df = pd.concat(self.dfs)

    def load_source_items(self, update=False, offset=3):
        items = self.raw_soup.find(id="main").find_all('u')
        base_columns = ["name", "nethys_url"]
        category_data = {}
        name = self.raw_soup.find(id="main").find(class_="title").a.get_text()
        for item in items[offset:2280]:
            item = item.a
            if item:
                item_data = [item.get_text(), item['href']]
                item_category = item_data[-1].split('.')[0].lower()
                if item_category not in category_data.keys():
                    category_data[item_category] = []
                    print(f"Adding {item_category} to data dict.")
                if item_category in item_category_arguments.keys():
                    try:
                        item_bowl = SoupKitchen(item_data[1])
                    except requests.exceptions.ConnectTimeout:
                        print(f"Connection Timeout for {item_data}")
                        continue
                    texts = self.get("text_columns", item_category)
                    urls = self.get("url_columns", item_category)
                    item_bowl.parse_item_data(category=item_category, show=True)
                    for header in texts:
                        item_data.append(item_bowl.get_item_data(header))
                    for header in urls:
                        item_data.append(item_bowl.get_item_data(header, url=True))
                    pickup = []
                    heighten = []
                    for key, value in item_bowl.parsed_rows.items():
                        if key not in base_columns + texts + urls:
                            part = f"{key}: {self.clean(value)}"
                            if "Heightened" in key:
                                heighten.append(part)
                            else:
                                pickup.append(part)
                    item_data.append("; ".join(pickup))
                    item_data.append("; ".join(heighten))
                category_data[item_category].append(item_data)
            else:
                print(f"{item} contains no link")
        if update:
            name += "_update"
        dfs = {}
        for key in category_data.keys():
            text_cols = [self.format_column_name(n) for n in self.get("text_columns", key)]
            url_cols = [self.format_column_name(n, url=True) for n in self.get("url_columns", key)]
            columns = base_columns + text_cols + url_cols
            if len(columns) < len(category_data[key][0]):
                columns += ["pickup", "heightened"]
            dfs[key] = pd.DataFrame(data=category_data[key], columns=columns)
            self.list_df = dfs
            try:
                self.norm_dfs()
            except Exception:
                print("An error occurred while norming dfs")
                pass
            app = self.get("app", key)
            dfs[key].to_csv(f"{BASE_DIR}\\{app}\\fixtures\\csv\\{name}\\{key}_items_raw.csv",
                            sep='|',
                            index=False)

    @staticmethod
    def clean(value):
        clean_value = BeautifulSoup(value, 'html.parser')
        if clean_value:
            text = clean_value.get_text()
            return text.strip()

    def norm_dfs(self):
        for key in self.list_df.keys():
            df = self.list_df[key]
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

    def parse_item_data(self, show=False, category="default"):
        """Once our table or list of items is loaded, we often need to
        fetch additional data on the item's page. Here we parse that page
        thanks to the markup provided to the constructor."""
        args = ["start", "end", "row_separator", "cell_separator", "tail_start", "row_sep_bis", "traits"]
        start, end, row, cell, tail, row2, traits = (self.get(arg, category) for arg in args)
        raw_data = self.raw_soup.find(id="main")
        data = str(raw_data).split(start)[1].split(end)[0]
        if start == "<b>Source":
            data = "Source" + data
        if row2:
            data = data.replace(row, row2)
            row = row2
        rows = data.split(row)

        other = ""
        if end == "<h1984":
            new_rows = []
            for row in rows:
                if ("<hr/>" not in row and "<hr>" not in row) or other:
                    new_rows.append(row)
                else:
                    if "<hr/>" in row:
                        sliced_row = row.split("<hr/>")
                    else:
                        sliced_row = row.split("<hr>")
                        new_rows.append(sliced_row[0])
                    other += sliced_row[1]
            rows = new_rows
        elif tail and tail in rows[-1]:
            last = rows[-1].split(tail)
            rows = rows[:-1] + last[:1]
            other = " ".join(last[1:])
        parsed_rows = {row.split(cell)[0]: row.split(cell)[1] for row in rows if cell in row}
        parsed_rows["Other"] = other
        if traits:
            parsed_rows["Traits"] = self.find_traits(raw_data)
        if category in ["spells"]:
            parsed_rows["Spell Level"] = self.find_level(raw_data)
        self.parsed_rows = parsed_rows
        if show:
            print(self.parsed_rows)

    @staticmethod
    def find_traits(data):
        datas = [data.find(class_="traituncommon")] + [data.find(class_="traitrare")]
        datas += [data.find(class_="traitunique")] + data.find_all(class_="trait")
        return "!".join([data.a.get_text() for data in datas if data is not None])

    @staticmethod
    def find_level(data):
        title = str(data).split('<span style="margin-left:auto; margin-right:0">')[-1].split('</span>')[0]
        return title

    def get_item_data(self, header, url=False):
        """Here we use the missing columns' headers given to the constructor to fetch
        the item data missing."""
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
