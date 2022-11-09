from bs4 import BeautifulSoup
import requests
import pandas as pd


class SoupKitchen:
    base_url = "https://2e.aonprd.com/"
    nav_links = None
    df = None
    parsed_rows = None

    def __init__(self, url):
        print(self.base_url + url)
        self.content = requests.get(self.base_url + url).content
        self.raw_soup = BeautifulSoup(self.content, 'html.parser')
        self.soup = self.raw_soup.prettify()

    def __str__(self):
        return self.soup

    def extract_nav_links(self):
        main = self.raw_soup.find(id="main").span
        nav_links = main.find_all('a')
        self.nav_links = {link.get_text(): link['href'] for link in nav_links}
        print(self.nav_links)

    def load_table(self,
                   item_url_column='Name',
                   text_complement_columns=None,
                   url_complement_columns=None,
                   tail_start=None):
        table = self.raw_soup.find(id="main").find('table')
        table_rows = table.find_all('tr')
        if len(table_rows) <= 1:
            return 0
        headers = [th.get_text() for th in table_rows[0].find_all('th')]
        if len(headers) == 0:
            return 0

        url_index = -1
        if item_url_column:
            headers += ["item_url"]
            try:
                url_index = headers.index(item_url_column)
            except ValueError:
                pass

            if text_complement_columns:
                headers += [self.format_column_name(n) for n in text_complement_columns]
            else:
                text_complement_columns = []
            if url_complement_columns:
                headers += [self.format_column_name(n, url=True) for n in url_complement_columns]
            else:
                url_complement_columns = []

        df = pd.DataFrame(columns=headers)
        for i in range(1, len(table_rows)):
            raw_row = table_rows[i].find_all('td')
            row = [td.get_text() for td in raw_row]
            if url_index >= 0:
                item_url = raw_row[url_index].a['href']
                row.append(item_url)
                if text_complement_columns or url_complement_columns:
                    new_bowl = SoupKitchen(item_url)
                    new_bowl.parse_item_data(tail_start)
                    for column in text_complement_columns:
                        row.append(new_bowl.get_item_data(column))
                    for column in url_complement_columns:
                        row.append(new_bowl.get_item_data(column, url=True))
            df.loc[i] = row
        self.df = df
        print(df.head())

    def df_to_csv(self):
        pass

    def complete_df(self):
        pass

    def norm_df(self):
        pass

    def parse_item_data(self,
                        separator='<br/><b>',
                        start='</h1><b>', end='<h2',
                        delimiter='</b>',
                        tail=None):
        data = self.raw_soup.find(id="main")
        data = str(data).split(start)[-1].split(end)[0]
        rows = data.split(separator)
        other = ""
        if tail and tail in rows[-1]:
            last = rows[-1].split(tail)
            rows = rows[:-1] + last[:1]
            other = " ".join(end[1:])
        parsed_rows = {r.split(delimiter)[0]: r.split(delimiter)[1] for r in rows}
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
        return header
