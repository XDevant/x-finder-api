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
    parsed_row = None
    parsed_rows = None
    nested_rows = None
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

    def load_source_items(self,
                          update=False,
                          offset=3,
                          from_df=None,
                          source_name="unknown",
                          category="default",
                          debug=False):
        """ The base use is to extract a list of links and then extract data from those links.
        Links encountered will be sorted into different category according to the url found and stored into a dict
        :param update: Bool used as a suffix for filename of csv, used by the commands when loading into db
        :param offset: Int used to filter the first source_links only in extract_source_links
        :param from_df: Panda df used to complete a single item category previously saved, bypass extract_source_links
        :param source_name: String used with from_df, name of the subdirectory where completed df will be saved
        :param category: String base name of the file if from_df is not none
        :param debug: Bool
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
        completed_categories, nested_categories = self.complete_all_category_items(category_data, debug=debug)
        completed_category_dfs = {}
        normed_category_dfs = {}
        for key in completed_categories.keys():
            app = self.get("app", key)
            df = pd.DataFrame.from_records(data=completed_categories[key])
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

    def complete_all_category_items(self, category_dict, debug=False):
        result_dict = {}
        nested_dict = {}
        for key, value in category_dict.items():
            if key in ["spells", "equipment"]:
                result_dict[key], nested_dict[key] = self.complete_category_items(key, value, debug=debug)
        return result_dict, nested_dict

    @chrono
    def complete_category_items(self, category, data, limit=30, debug=False):
        """"""
        results = []
        nesteds = []
        count = 0
        for row in data:
            try:
                item_bowl = SoupKitchen(row["url"])
            except requests.exceptions.ConnectTimeout:
                print(f"Connection Timeout for {row}")
                continue
            item_bowl.parse_item(category=category, debug=True)

            result = item_bowl.parsed_rows
            nested = item_bowl.nested_rows
            results += result
            nesteds += nested
            count += 1
            if debug and count == limit:
                break
        print(len(results))
        return results, nesteds

    @staticmethod
    def parse_source_links(item_list):
        category_data = {}
        no_category_data = {}
        flags = []
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
                if item_category == "equipment" and item_dict["url"] in flags:
                    continue
                flags.append(item_dict["url"])
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
        if "level" in df.columns:
            if "spell_type" in self.get("text_columns", key):
                df["spell_type"] = df.apply(
                    lambda r: r["level"].split(' ')[0].strip(),
                    axis=1)
            df["level"] = df.apply(
                lambda r: int(r["level"].split(' ')[-1].strip()) if r["level"].split(' ')[-1].strip().isnumeric() else -1,
                axis=1)
        if "description" not in df.columns and "other" in df.columns:
            df.rename(columns={"other": "description"}, inplace=True)
        if "source" in df.columns and "source_page" not in df.columns:
            df.rename(columns={"source": "sources"}, inplace=True)
            df["source_page"] = df.apply(
                lambda r: int(r["sources"][-2].split('pg. ')[-1]) if 'pg. ' in r["sources"][-2] else 0,
                axis=1)
            df["source"] = df.apply(lambda r: r["sources"][-2].split('pg. ')[0].strip(), axis=1)
            df["source_update"] = df.apply(lambda r: r["sources"][-1].strip(), axis=1)
        return df

    def find_start_ok(self, content, status, result, category, debug=False, verbose=False):
        if content:
            title = content.find('h1')
            if title is not None and title.get_text():
                title_content = title.get_text(separator=',').split(',')
                result.titles.append({"name": title_content[0].strip(' ,;'),
                                      "x_finder_model": category})
                if title.a is not None and title.a['href'] is not None:
                    result.titles[0]["url"] = title.a['href']
                    status.type = "ok"
                else:
                    result.titles[0]["url"] = self.url
                    status.type = "ok_broken"
                if "level" in self.get("text_columns", category):
                    level = title_content[-1].strip(' ,;')
                    if level:
                        result.titles[0]["level"] = level
                    if level.endswith('+'):
                        status.family = True
                if verbose:
                    print(f"Start found for {'family' if status.family else 'item'}: {result.titles[0]['name']}")
                    print(f"on h1: {title}")
                return title.next_sibling
        if debug or verbose:
            print(f"Start not found for h1: {self.name}")
        status.type = "broken"
        return None

    def find_start_broken(self, content, status, result, category, debug=False, verbose=False):
        """
        if verbose:
            print(f"Secondary title: {result.titles[status.ended]} found for family {result.titles[0]}")
            return broken_title.find_next_sibling()
        if debug or verbose:
            print(f"unable to find members of family {result.titles[0]}")
            return None
        """
        broken_title = content
        if broken_title is not None and broken_title.name in [None, 'a']:
            url = self.url
            if broken_title.name == 'a':
                status.type = "broken_title"
                name = broken_title.get_text().strip(' ,;')
                url = broken_title["href"]
            elif broken_title.name is None and len(broken_title) > 2:
                status.type = "broken_link"
                name = str(broken_title).strip(' ,;')
            else:
                return self.find_start_broken(broken_title.next_sibling,
                                              status,
                                              result,
                                              category,
                                              debug=debug,
                                              verbose=verbose)
            if name:
                result.titles.append({"name": name,
                                      "url": url,
                                      "x_finder_model": category})
                status.ended = len(result.titles) - 1
                print(f"found name {name} for {broken_title}")
                title_end = broken_title.next_sibling
                if title_end and title_end.name == "span":
                    text = title_end.get_text().strip(' ,;')
                    if "action" in text:
                        result.titles[status.ended]['action'] = text
                        title_end = title_end.next_sibling
                    if title_end and title_end.name == "span" and 'level' in self.get("text_columns", category):
                        level = title_end.get_text().strip(' ,;')
                        result.titles[status.ended]['level'] = level
                        if level.endswith('+'):
                            status.family = True
                            if verbose:
                                print(f"Secondary title: {result.titles[status.ended]} found for {result.titles[0]}")
                        return title_end.next_sibling
                return title_end
        else:
            if broken_title is None:
                if debug or verbose:
                    print(f"unable to find members of family {result.titles[0]}")
                return None
            return self.find_start_broken(broken_title.next_sibling,
                                          status,
                                          result,
                                          category,
                                          debug=debug,
                                          verbose=verbose)

    def add_new_title(self, child, result, status, category=None, name=None):
        title = child.get_text(separator=',')
        if title:
            status.ended += 1
            result.titles.append({})

            if child.name == 'a' and child['href'] and child['href'] not in [None, "PFS.aspx"]:
                url = child['href']
            elif child.name and child.find('a') and child.find('a')['href'] not in [None, "PFS.aspx"]:
                url = child.find('a')['href']
            elif status.family:
                url = result.titles[0]["url"]
            else:
                url = ""
            title_parts = title.strip(',; ').split(',')
            if name is None:
                name = title_parts[0]
            result.titles[status.ended]["name"] = name
            result.titles[status.ended]["url"] = url
            if category is None:
                category = self.find_nested_item_category(name, url)
            if category:
                result.titles[status.ended]['x_finder_model'] = category
                if result.titles[status.ended]["url"] == "":
                    result.titles[status.ended]["url"] = self.url
            if 'level' not in result.titles[status.ended].keys():
                if 'level' in self.get("text_columns", category):
                    result.titles[status.ended]['level'] = title_parts[-1]
            if 'action' in self.get("text_columns", category):
                try:
                    result.titles[status.ended]['action'] = child.find('span').get_text(' ,;')
                except AttributeError:
                    pass

    def find_nested_item_category(self, name, url, next_child=None):
        next_model = "default"
        name = name.lower().strip('()[]').replace(' ', '_').replace('-', '_')
        name_model = self.get("", name)
        url_base = url.split('.')[0].strip().lower().replace(' ', '_').replace('-', '_')
        url_model = self.get("", url_base)
        if next_child is not None and next_child.get_text():
            next_text = next_child.get_text()
            if next_text:
                next_text = next_text.lower().strip(' (),;').replace(' ', '_')
                if not next_text.endswith('s'):
                    next_text += 's'
                next_model = self.get("", next_text.lower().strip(' (),;'))
                if next_model not in ["rules", "default"]:
                    return next_model
        if url_model is not None and url_model not in ["rules", "default"]:
            return url_model
        if name_model != "default":
            return name_model
        return "default"

    def parse_item(self, category="default", debug=False, verbose=False):
        if self.soup:
            main = self.soup.find(id="ctl00_RadDrawer1_Content_MainContent_DetailedOutput")
        else:
            print(f"No soup provided for {self.name}: {category}, {self.url}.")
            return

        class Status:
            type = "broken_title"
            family = False
            ended = 0
            last_key = ""
            loaded_values = []

        class Result:
            parsed = []
            titles = []
            links = []
            tails = []

        status = Status()
        result = Result()
        ok_start = self.find_start_ok(main, status, result, category, debug=debug, verbose=verbose)
        if ok_start is not None:
            self.read_soup(ok_start, status, result, category, debug=debug, verbose=verbose)
        if ok_start is None or status.type == "ok_broken":
            main = self.soup.find(id="ctl00_RadDrawer1_Content_MainContent_DetailedOutput")
            start_broken = self.find_start_broken(main, status, result, category, debug=debug, verbose=verbose)
            if start_broken is not None:
                self.read_soup(start_broken, status, result, category, debug=debug, verbose=verbose)

        self.parsed_rows = []
        self.nested_rows = []
        for title in result.titles:
            if title and len(title) > 2 and 'x_finder_model' in title.keys():
                if title['x_finder_model'] == category:
                    if 'level' in result.titles[0].keys() and 'level' not in title.keys():
                        print(title)
                        continue
                    if result.titles[0]['name'] == 'Scroll':
                        print(result.titles)
                    self.parsed_rows.append(title)
                else:
                    self.nested_rows.append(title)
        if verbose:
            for title in result.titles:
                if title and len(title) > 2 and 'x_finder_model' in title.keys():
                    print(title)
        if debug:
            if result.parsed:
                print(result.titles[0]["name"], result.parsed)
            if result.tails:
                print(result.titles[0]["name"], result.tails)

    def read_soup(self, child, status, result, category, debug=False, verbose=False):
        if status.last_key:
            if child.name and child.name in self.get("cell_ends", category):
                self.store(status, result)
                status.last_key = ""
                status.loaded_values = []
        if status.last_key:
            value = child.get_text().strip(',; \r\n')
            if value and value != '\n':
                status.loaded_values.append(value)
        elif child is not None and child.name and child.name in self.get("next_titles", category):
            if status.family:
                self.add_new_title(child, result, status, category)
            else:
                self.add_new_title(child, result, status)

        elif child is not None and child.name and child.name in self.get("cell_starts", category):
            key = child.get_text().strip(' ,;')
            key = key.replace('-', '_')
            key = key.replace(' ', '_')
            if key:
                status.last_key = key.lower()
                status.loaded_values = []
                if key not in self.get("text_columns", category):
                    title = result.titles[status.ended]
                    if 'x_finder_model' in title.keys() and key not in self.get(
                            "text_columns",
                            title['x_finder_model']):
                        test_category = self.find_nested_item_category(key,
                                                                       title['url'],
                                                                       child.next_sibling)
                        if test_category is not None and test_category not in [category,
                                                                               title['x_finder_model'],
                                                                               ]:
                            self.add_new_title(child, result, status, category=test_category)

        elif child is not None and child.name == 'span':
            link = child.find('a')
            if link and link['href'] and link.get_text():
                if "Traits" in link['href']:
                    if "traits" not in result.titles[status.ended].keys():
                        result.titles[status.ended]['traits'] = []
                    result.titles[status.ended]['traits'].append(link.get_text())
                else:
                    result.links.append({"name": link.get_text(), "url": link['href']})
                link = link.next_sibling
                if link is not None:
                    self.read_soup(link, status, result, category, debug=debug, verbose=verbose)

        elif child is not None and (child.name is None or child.name in ['a', 'u', 'i', 'ol', 'ul']):
            values = []
            if child.name in ['ol', 'ul']:
                values = child.find_all('li')
                values = [value.get_text().strip(' ,;') for value in values if value.get_text() is not None]
            else:
                value = child.get_text().strip(' ,;\n\r')
                if value:
                    values.append(value)
                    if child.name == 'a' or child.name is not None and child.find('a') is not None:
                        if child.name == 'a':
                            url = child['href']
                        else:
                            url = child.find('a')['href']
                        if "description_links" not in result.titles[status.ended].keys():
                            result.titles[status.ended]["description_links"] = []
                        result.titles[status.ended]["description_links"].append({"name": value, "url": url})
            if values:
                if "description" not in result.titles[status.ended].keys():
                    result.titles[status.ended]["description"] = []
                result.titles[status.ended]["description"] += values

        elif child is not None and child.name == "table":
            table = []
            row_len = 0
            for tr in child.find_all('tr'):
                row = [td.get_text() for td in tr.find_all('td')]
                if len(row) < row_len:
                    row = [table[-1][0]] + row
                row_len = max(row_len, len(row))
                table.append(row)
            result.titles[status.ended]["table"] = table

        elif child is not None and child.get_text().strip(' ,;'):
            if "description" not in result.titles[status.ended].keys():
                result.titles[status.ended]["description"] = []
            result.titles[status.ended]["description"].append(child.get_text().strip(' ,;\n\r'))
        if child:
            next_child = child.next_sibling
            if next_child:
                return self.read_soup(next_child, status, result, category, debug=debug, verbose=verbose)
        if status.last_key and status.loaded_values:
            self.store(status, result)
        return

    def store(self, status, result):
        match = False
        if 'x_finder_model' in result.titles[0].keys():
            category = result.titles[0]['x_finder_model']
            text_cols = self.get("text_columns", category)
            if status.last_key in text_cols or status.last_key.split('_')[0] in text_cols:
                match = True
                if status.last_key not in result.titles[0].keys() and not status.family:
                    result.titles[0][status.last_key] = status.loaded_values
                    return
        if 'x_finder_model' in result.titles[status.ended].keys():
            if result.titles[status.ended]['x_finder_model']:
                category = result.titles[status.ended]['x_finder_model']
                text_cols = self.get("text_columns", category)
                if status.last_key in text_cols:
                    if status.last_key not in result.titles[status.ended].keys():
                        result.titles[status.ended][status.last_key] = status.loaded_values
                        return
        if match and status.last_key == 'advanced_domain_spell':
            if 'advanced_apocryphal_domain_spell' not in result.titles[0].keys():
                result.titles[0]['advanced_apocryphal_domain_spell'] = status.loaded_values
                return
        if match and isinstance(result.titles[0][status.last_key], list):
            result.titles[0][status.last_key] += status.loaded_values
            return
        if 'x_finder_model' in result.titles[status.ended].keys():
            name = result.titles[status.ended]["name"].lower().replace(' ', '_')
            if name == result.titles[status.ended]['x_finder_model']:
                result.titles[status.ended]["name"] = status.last_key
            category = result.titles[status.ended]['x_finder_model']
            nested_cols = self.get("nested_pairs", category)
            if nested_cols:
                nested_number = len(nested_cols) // 2
                for i in range(nested_number):
                    if nested_cols[2 * i] not in result.titles[status.ended].keys() and status.loaded_values:
                        result.titles[status.ended][nested_cols[2 * i]] = status.last_key
                        result.titles[status.ended][nested_cols[2 * i + 1]] = status.loaded_values
                        return
        result.parsed.append({status.last_key: status.loaded_values})

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
        parsed_row = {self.extract_value(row.split(cell)[0]): row.split(cell)[1] for row in rows if cell in row}
        parsed_row["Other"] = other
        if traits:
            parsed_row["Traits"] = self.find_traits(raw_data)
        if category in ["spells", "feats", "equipment", "weapons", "armor", "shield"]:
            parsed_row["Spell Level"] = self.find_level(raw_data)
        if category == "deities":
            if "Pantheons" in parsed_row.keys() and parsed_row["Pantheons"] is not None:
                parsed_row["Pantheons"] = parsed_row["Pantheons"].split('<h2')[0]
            if "Cleric Spells" in parsed_row.keys() and parsed_row["Cleric Spells"] is not None:
                split_row = parsed_row["Cleric Spells"].split('<h2')
                parsed_row["Cleric Spells"] = split_row[0]
                parsed_row["Divine Intercession"] = split_row[-1].split('</h2>')[-1]
        self.parsed_row = parsed_row
        if show:
            print(self.parsed_row)

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
        if self.parsed_row and header and header in self.parsed_row.keys():
            value = BeautifulSoup(self.parsed_row[header], self.parser)
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
