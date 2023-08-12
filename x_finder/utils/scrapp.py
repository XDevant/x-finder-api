from django.conf import settings
from bs4 import BeautifulSoup
import requests
import pandas as pd

BASE_DIR = settings.BASE_DIR
BASE_URL = "https://2e.aonprd.com/"


class SoupKitchen:
    base_url = BASE_URL
    nav_links = None
    df = None
    parsed_rows = None
    text_complement_columns = []
    url_complement_columns = []
    dfs = []

    def __init__(self,
                 url,
                 app="soup",
                 table=False,
                 sub_tables=True,
                 save=True,
                 item_url_column='Name',
                 text_complement_columns=None,
                 url_complement_columns=None,
                 separator='<br/><b>',
                 start='</h1><b>',
                 end='<h2',
                 delimiter='</b>',
                 tail_start=None):
        self.item_url_column = item_url_column
        if text_complement_columns:
            self.text_complement_columns = text_complement_columns
        if url_complement_columns:
            self.url_complement_columns = url_complement_columns
        self.separator = separator
        self.start = start
        self.end = end
        self.delimiter = delimiter
        self.tail_start = tail_start
        self.content = requests.get(self.base_url + url).content
        self.raw_soup = BeautifulSoup(self.content, 'html.parser')
        self.soup = self.raw_soup.prettify()
        self.app = app
        self.table = table
        self.sub_tables = sub_tables
        self.save = save
        self.name = url.split('.')[0]
        """
        if self.sub_tables:
            self.extract_nav_links()
        if self.table:
            self.load_table()
        if not self.sub_tables and not self.table:
            self.load_source_items()
        if self.save:
            self.df_to_csv(f"{BASE_DIR}\\{self.app}\\fixtures\\{self.name}_raw.csv", self.df)
        """

    def __str__(self):
        return self.soup

    def save(self):
        self.df_to_csv(f"{BASE_DIR}\\{self.app}\\fixtures\\{self.name}_raw.csv", self.df)

    def extract_nav_links(self):
        main = self.raw_soup.find(id="main").span
        nav_links = main.find_all('a')
        self.nav_links = {link.get_text(): link['href'] for link in nav_links}
        self.clean_nav_links()

    def clean_nav_links(self):
        """Overload in child if needed"""
        print(self.nav_links)

    def load_table(self):
        table = self.raw_soup.find(id="main").find('table')
        table_rows = table.find_all('tr')
        if len(table_rows) <= 1:
            return 0
        headers = [self.format_column_name(th.get_text()) for th in table_rows[0].find_all('th')]
        if len(headers) == 0:
            return 0

        url_index = -1
        if self.item_url_column:
            headers += ["nethys_url"]
            try:
                url_index = headers.index(self.item_url_column)
            except ValueError:
                pass

            if self.text_complement_columns:
                headers += [self.format_column_name(n) for n in self.text_complement_columns]
            if self.url_complement_columns:
                headers += [self.format_column_name(n, url=True) for n in self.url_complement_columns]

        df = pd.DataFrame(columns=headers)
        for i in range(1, len(table_rows)):
            raw_row = table_rows[i].find_all('td')
            row = [td.get_text() for td in raw_row]
            if url_index >= 0:
                item_url = raw_row[url_index].a['href']
                row.append(item_url)
                if self.text_complement_columns or self.url_complement_columns:
                    new_bowl = SoupKitchen(item_url,
                                           table=False,
                                           sub_tables=False,
                                           save=False,
                                           item_url_column=self.item_url_column,
                                           text_complement_columns=self.text_complement_columns,
                                           url_complement_columns=self.url_complement_columns,
                                           separator=self.separator,
                                           start=self.start,
                                           end=self.end,
                                           delimiter=self.delimiter,
                                           tail_start=self.tail_start)
                    new_bowl.parse_item_data()
                    for column in self.text_complement_columns:
                        row.append(new_bowl.get_item_data(column))
                    for column in self.url_complement_columns:
                        row.append(new_bowl.get_item_data(column, url=True))
            df.loc[i] = row
        self.df = df

    def load_sub_tables(self):
        for key, url in self.nav_links.items():
            sub_bowl = SoupKitchen(url,
                                   table=True,
                                   sub_tables=False,
                                   save=False,
                                   item_url_column=self.item_url_column,
                                   text_complement_columns=self.text_complement_columns,
                                   url_complement_columns=self.url_complement_columns,
                                   separator=self.separator,
                                   start=self.start,
                                   end=self.end,
                                   delimiter=self.delimiter,
                                   tail_start=self.tail_start)
            sub_bowl.load_table()
            sub_bowl.df["Category"] = [key] * len(sub_bowl.df)
            sub_bowl.norm_df()
            self.dfs.append(sub_bowl.df)
            print(key)
        self.df = pd.concat(self.dfs)

    def load_source_items(self):
        items = self.raw_soup.find(id="main").find_all('u')
        columns = ["name", "category", "url"]
        df = pd.DataFrame(columns=columns)
        for item in items:
            item = item.a
            item_url = item['href']
            item_category = item_url.split('.')[0].lower()
            df += [item.get_text(), item_category, item_url]

    def norm_df(self):
        return self.df

    def parse_item_data(self):
        data = self.raw_soup.find(id="main")
        data = str(data).split(self.start)[-1].split(self.end)[0]
        rows = data.split(self.separator)
        other = ""
        if self.tail_start and self.tail_start in rows[-1]:
            last = rows[-1].split(self.tail_start)
            rows = rows[:-1] + last[:1]
            other = " ".join(self.end[1:])
        parsed_rows = {r.split(self.delimiter)[0]: r.split(self.delimiter)[1] for r in rows}
        parsed_rows["other"] = other
        self.parsed_rows = parsed_rows

    def get_item_data(self, header, url=False):
        if self.parsed_rows and header in self.parsed_rows.keys():
            value = BeautifulSoup(self.parsed_rows[header], 'html.parser')
            if value:
                if url:
                    try:
                        url = value.find('a')['href']
                        return url
                    except TypeError:
                        return ""
                return value.get_text()
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
        pathfile = f"{BASE_DIR}\\{app}\\fixtures\\{name}{suffix}.csv"
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
    pass
