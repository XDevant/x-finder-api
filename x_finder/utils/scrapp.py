import requests
import pandas as pd
from bs4 import BeautifulSoup
from os import makedirs
from time import time, sleep
from multiprocessing import Pool
from pathlib import Path
from x_finder.utils.fixtures.args import item_category_arguments as ica


item_category_arguments = ica


BASE_URL = "https://2e.aonprd.com/"
BASE_DIR = Path(__file__).resolve().parent.parent


def chrono(func):
    """Wrapper to print the program runtime."""
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        print(f"Time: {round(end - start, 2)}")
        return result
    return wrapper


class SoupKitchen:
    base_url = BASE_URL
    nav_links = None
    parsed_rows = None
    df = None
    item_links = None
    dfs = []
    completed_dfs = {}
    normed_dfs = {}

    def __init__(self, url=None, name=None, content=None, parser='html.parser'):
        if url and isinstance(url, str):
            self.url = url
        if name:
            self.name = name
        elif self.url:
            self.name = url.split('.')[0]
        else:
            self.name = "unknown"
        if content:
            self.content = content
        else:
            self.content = self.request_content(url)
        self.parser = parser
        if self.content:
            self.soup = BeautifulSoup(self.content, parser)
        else:
            self.soup = None

    def __str__(self):
        return str(self.soup)

    def request_content(self, url):
        if url:
            response = requests.get(self.base_url + url)
            if response.status_code == 200:
                return response.content
            print(f"Received status code {response.status_code} for url {url}")
        print("No url provided.")
        return None

    def cook(self, url=None, name=None, content=None, parser='html.parser'):
        self.parser = parser
        if content:
            self.content = content
            raw_soup = BeautifulSoup(self.content, parser)
            self.soup = raw_soup
            if url:
                self.url = url
            if name:
                self.name = name
            elif url:
                self.name = url.split('.')[0]
            else:
                self.name = "unknown"
            return
        if url:
            content = self.request_content(url)
            self.content = content
            if name:
                self.name = name
            else:
                self.name = url.split('.')[0]
            raw_soup = BeautifulSoup(content, parser)
            self.soup = raw_soup
            return
        if name:
            self.name = name
            return

    @staticmethod
    def get(argument, category="default"):
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

    def load(self, name, app="utils", directory=None, suffix="raw"):
        if directory:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{directory}\\{name}_{suffix}.csv"
        else:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{name}{suffix}.csv"
        df = pd.read_csv(pathfile, delimiter="|")
        return df

    def extract_nav_links(self):
        """ If our table is split among sub-tables, we fetch their urls.
        We store their names / urls as key / value pairs in a dict """
        if self.soup:
            main = self.soup.find(id="main").span
            nav_links = main.find_all('a')
            self.nav_links = {link.get_text(): link['href'] for link in nav_links}
            self.clean_nav_links()
            print(f"{len(self.nav_links)} navigation links extracted.")
        else:
            print("No soup found, did you cook it?")

    def clean_nav_links(self):
        """Overload in child if needed"""
        print(self.nav_links)

    def load_table(self, category="default", save=True):
        if not self.soup:
            print("No soup found, did you cook it?")
            return
        table_rows, headers = self.find_table(self.soup)
        if not table_rows or not headers:
            print("No table found.")
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
        print(f"Table {category} extracted with {counter} missing item url{'s' if counter > 1 else ''}.")
        if save:
            self.save(self.df, f"{category}_sources_completed", app="core")

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
            sub_bowl.load_table(category=category, save=False)
            sub_bowl.df["category"] = [key] * len(sub_bowl.df)
            self.dfs.append(sub_bowl.df)
        self.df = pd.concat(self.dfs)
        self.save(self.df, "sources_completed", app="core")

    def load_source_items(self, update=False, offset=3, from_df=None, source_name="unknown", category="default"):
        """ The base use is to extract a list of links and then extract data from those links.
        Links encountered will be sorted into different category according to the url found and stored into a dict
        :param update: Bool used as a suffix for filename of csv, used by the commands when loading into db
        :param offset: Int used to filter the first source_links only in extract_source_links
        :param from_df: Panda df used to complete a single item category previously saved, bypass extract_source_links
        :param source_name: String used with from_df, name of the subdirectory where completed df will be saved
        :param category: String base name of the file if from_df is not none
        :return: nothing

        """
        if not from_df:
            item_links = self.extract_source_links(offset)
            if source_name == "unknown":
                source_name = self.get_source_name(update)
            category_data, no_category_data = self.parse_source_links(item_links)

            new_category_dfs = {}
            for key, value in no_category_data.items():
                df = pd.DataFrame.from_records(data=value)
                new_category_dfs[key] = df
                self.save(df, f"{key}_raw", directory=source_name)
        else:
            category_data = {category: from_df.to_dict('records')}
        completed_category = self.complete_all_category_items(category_data)
        completed_category_dfs = {}
        normed_category_dfs = {}
        for key in completed_category.keys():
            app = self.get("app", key)
            df = pd.DataFrame(data=completed_category[key])
            try:
                df = self.norm_df(df, key)
                normed_category_dfs[key] = df
                suffix = "normed"
            except Exception:
                print(f"An error occurred while norming {key} df")
                completed_category_dfs[key] = df
                suffix = "completed"
            self.save(df, f"{key}_{suffix}", directory=source_name, app=app)
        self.completed_dfs = completed_category_dfs
        self.normed_dfs = completed_category_dfs

    def extract_source_links(self, offset):
        if self.soup:
            item_list = self.soup.find(id="main").find_all('u')
            link_list = [item.a for item in item_list[offset:] if item.a is not None]
            return link_list
        print("No soup found, did you cook it?")
        return []

    def get_source_name(self, update=False):
        if self.soup:
            try:
                name = self.soup.find(id="main").find(class_="title").a.get_text()
            except TypeError:
                name = "Unknown"
            if update:
                name += "_update"
            return name
        print("No soup found, did you cook it?")
        return ""

    def complete_all_category_items(self, category_dict):
        result_dict = {}
        for key, value in category_dict.items():
            result_dict[key] = self.complete_category_items(key, value)
        return result_dict

    @chrono
    def complete_category_items(self, category, data):
        """"""
        result_data = []
        for row in data:
            try:
                item_bowl = SoupKitchen(row["url"])
            except requests.exceptions.ConnectTimeout:
                print(f"Connection Timeout for {row}")
                continue
            texts = self.get("text_columns", category)
            urls = self.get("url_columns", category)
            item_bowl.parse_item_data(category=category, show=True)
            for header in texts:
                row[self.format_column_name(header)] = item_bowl.get_item_data(header).strip(";")
            for header in urls:
                row[self.format_column_name(header, url=True)] = item_bowl.get_item_data(header, url=True)
            pickup = []
            heightened = []
            for key, value in item_bowl.parsed_rows.items():
                if key in urls:
                    formatted_key = self.format_column_name(key)
                else:
                    formatted_key = self.format_column_name(key)
                if formatted_key not in row.keys():
                    part = f"{formatted_key}: {self.clean(value)}"
                    if "heightened" in formatted_key:
                        heightened.append(part)
                    else:
                        pickup.append(part)
            row["pickup"] = "; ".join(pickup)
            row["heightened"] = "; ".join(heightened)
            result_data.append(row)
        return result_data

    @staticmethod
    def parse_source_links(item_list):
        category_data = {}
        no_category_data = {}
        for item in item_list:
            name = item.get_text()
            url = item['href']

            if name:
                item_dict = {"name": name, "url": url}
            else:
                continue
            if url:
                item_category = url.split('.')[0].lower()
            else:
                item_category = "unknown"

            if item_category not in item_category_arguments.keys():
                if item_category not in no_category_data.keys():
                    no_category_data[item_category] = []
                no_category_data[item_category].append(item_dict)
            else:
                if item_category not in category_data.keys():
                    category_data[item_category] = []
                category_data[item_category].append(item_dict)
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

    def parse_item(self, debug=False, category="default"):
        main = self.soup.find(id="main")
        started = False
        ended = 0
        last_key = ""
        loaded_values = []
        parsed = []
        titles = [{}]
        links = []
        tails = []
        title = main.find_all('span')[-1].h1
        if title is not None and title.a is not None and title.a['href'] is not None:
            main = main.find_all('span')[-1]

        for child in main:
            if not child.name and child.get_text() in [' ', '\n', '']:
                continue
            if not started:
                if child.name == 'h1':
                    child = child.a
                if child.name == "a":
                    if child['href'] and child.get_text():
                        titles[0]["name"] = child.get_text()
                        titles[0]["url"] = child['href']
                        title_model = child['href'].split('.')[0].strip().lower().replace(' ', '_')
                        if title_model == category:
                            titles[0]['x_finder_model'] = title_model
                            started = True
                            if debug:
                                print(f"Parsing started on {child}")
                elif category == "spells" and child.name is None and len(child.get_text()) > 1:
                    titles[0]["name"] = child.get_text()
                    titles[0]["url"] = self.url
                    titles[0]['x_finder_model'] = "spells"
                    started = True
                    if debug:
                        print(f"Parsing started on {child}")
                continue

            if loaded_values:
                if child.name in ['b', 'br', 'hr', 'h2', 'h3']:
                    self.store(last_key, loaded_values, ended, titles, parsed)
                    last_key = ""
                    loaded_values = []
                else:
                    value = child.get_text().strip(',; ')
                    if value:
                        loaded_values.append(value)
                    continue

            if child.name in ["h2", "h3"]:
                ended += 1
                titles.append({})
                name = child.get_text().strip(',; ')
                titles[ended]["name"] = name
                name_model = self.get("", name.lower().replace(' ', '_'))
                link = child.find('a')
                if link:
                    titles[ended]["url"] = link['href']
                    if link['href']:
                        url_model = link['href'].split('.')[0].strip().lower().replace(' ', '_')
                        if url_model:
                            titles[ended]["x_finder_model"] = self.get("", url_model)
                        if (not url_model or url_model == "rules") and name_model != "default":
                            titles[ended]["x_finder_model"] = name_model
                continue
            if child.name == 'b':
                value = child.get_text().strip(' ,;')
                value = "_".join(value.split(" "))
                if value:
                    last_key = value.lower()
                continue

            if last_key:
                value = child.get_text().strip(';, ')
                if value:
                    if value == '(disease)':
                        ended += 1
                        titles.append({})
                        titles[ended]["name"] = last_key
                        titles[ended]["x_finder_model"] = 'diseases'
                        last_key = ""
                        continue
                    loaded_values.append(value)
                    continue

            if child.name == 'a':
                links.append(child['href'])
            if child.name == 'span':
                if titles[0]["x_finder_model"] == 'spells' and 'spell_level' not in titles[0].keys():

                    titles[0]['spell_level'] = child.get_text().split(' ')[-1]
                    titles[0]['spell_type'] = child.get_text().split(' ')[0]
                    continue
                link = child.find('a')
                if link and link['href'] and link.get_text():
                    if "Traits" in link['href']:
                        if "traits" not in titles[0].keys():
                            titles[ended]['traits'] = []
                        titles[ended]['traits'].append(link.get_text())
                    links.append({link.get_text(): link['href']})
                    continue

            if child.name is None or child.name in ['a', 'u']:
                value = child.get_text().strip(' ,;')
                if value:
                    if "description" not in titles[ended].keys():
                        titles[ended]["description"] = []
                    titles[ended]["description"].append(value)
                continue
            if child.name in ['br', 'hr']:
                continue
            value = child.get_text().strip()
            if value:
                tails.append(value)
        for title in titles:
            print(title)
        print(parsed, '\n', tails)

    def store(self, key, value, index, found, not_found):
        if 'x_finder_model' in found[0].keys() and key not in found[0].keys():
            category = found[0]['x_finder_model']
            text_cols = self.get("text_columns", category)
            if key in text_cols:
                found[0][key] = value
                return
        if key not in found[index].keys() and 'x_finder_model' in found[index].keys():
            if found[index]['x_finder_model']:
                category = found[index]['x_finder_model']
                text_cols = self.get("text_columns", category)
                if key in text_cols:
                    found[index][key] = value
                    return
        not_found.append({key: value})

    def parse_item_data(self, show=False, category="default"):
        """Once our table or list of items is loaded, we often need to
        fetch additional data on the item's page. Here we parse that page
        thanks to the markup provided to the constructor."""
        args = ["start", "end", "row_separator", "cell_separator", "tail_start", "row_sep_bis", "traits"]
        start, end, row, cell, tail, row2, traits = (self.get(arg, category) for arg in args)
        raw_data = self.soup.find(id="main")
        for child in raw_data:
            print(child.name, child)
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
        if end == "<h1984>":
            new_rows = []
            for row in rows:
                new_row = row.replace("<hr>", "<hr/>")
                new_row = new_row.replace("<br>", "<br/>")
                if tail not in new_row or other:
                    new_rows.append(row)
                else:
                    sliced_row = new_row.split(tail)
                    new_rows.append(sliced_row[0])
                    other += " ".join(sliced_row[1:])
            rows = new_rows
        elif tail and tail in rows[-1]:
            last = rows[-1].split(tail)
            rows = rows[:-1] + last[:1]
            other = " ".join(last[1:])
        parsed_rows = {self.extract_value(row.split(cell)[0]): row.split(cell)[1] for row in rows if cell in row}
        parsed_rows["Other"] = other
        if traits:
            parsed_rows["Traits"] = self.find_traits(raw_data)
        if category in ["spells", "feats", "equipment", "weapons", "armor", "shield"]:
            parsed_rows["Spell Level"] = self.find_level(raw_data)
        if category == "deities":
            if "Pantheons" in parsed_rows.keys() and parsed_rows["Pantheons"] is not None:
                parsed_rows["Pantheons"] = parsed_rows["Pantheons"].split('<h2')[0]
            if "Cleric Spells" in parsed_rows.keys() and parsed_rows["Cleric Spells"] is not None:
                split_row = parsed_rows["Cleric Spells"].split('<h2')
                parsed_rows["Cleric Spells"] = split_row[0]
                parsed_rows["Divine Intercession"] = split_row[-1].split('</h2>')[-1]
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
            value = BeautifulSoup(self.parsed_rows[header], self.parser)
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

    def extract_value(self, value, url=False):
        if value:
            value = BeautifulSoup(value, self.parser)
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
