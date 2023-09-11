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

    @staticmethod
    def load(name, app="utils", directory=None, suffix="raw"):
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
                          category_filter=None,
                          source_name="unknown",
                          category="default",
                          debug=False,
                          verbose=False):
        """ The base use is to extract a list of links and then extract data from those links.
        Links encountered will be sorted into different category according to the url found and stored into a dict
        :param update: Bool used as a suffix for filename of csv, used by the commands when loading into db
        :param offset: Int used to filter the first source_links only in extract_source_links
        :param from_df: Panda df used to complete a single item category previously saved, bypass extract_source_links
        :param category_filter: List of categories you want to complete. If None, all categories will be completed
        :param source_name: String used with from_df, name of the subdirectory where completed df will be saved
        :param category: String base name of the file if from_df is not none
        :param debug: Bool
        :param verbose: Bool
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
        if category_filter is not None and isinstance(category_filter, list):
            category_data = {key: value for key, value in category_data.items() if key in category_filter}
        completed_categories, nested_categories = self.complete_all_category_items(category_data,
                                                                                   debug=debug,
                                                                                   verbose=verbose)
        normed_dfs, completed_dfs, finalized_dfs = self.build_dfs(completed_categories, source_name=source_name)
        normed_nested_dfs, completed_nested_dfs, finalized_nested_dfs = self.build_dfs(nested_categories,
                                                                                       source_name=source_name,
                                                                                       suff="nested")
        for key in normed_nested_dfs.keys():
            if key not in normed_dfs.keys():
                normed_dfs[key] = normed_nested_dfs[key]
        self.completed_dfs = completed_dfs
        self.normed_dfs = normed_dfs

    def extract_model_dfs(self, df, key):
        columns = self.get("model_columns", key)
        if columns:
            columns = [column for column in columns if column in df.columns]
        else:
            excluded = self.get("model_excluded_columns", key)
            columns = [column for column in df.columns if column not in excluded]

        for column in columns:
            join = " "
            if column.endswith("s"):
                join = "; "
            df[column] = df.apply(
                lambda r: join.join(r[column]) if isinstance(r[column], list) else r[column],
                axis=1)
        if "description_links" in df.columns and "description" in columns:
            columns.append("description_links")
        model_df = df[columns]
        return model_df

    def build_dfs(self, dict_of_dicts, source_name="Unknown", suff=""):
        completed_category_dfs = {}
        normed_category_dfs = {}
        finalized_category_dfs = {}
        for key in dict_of_dicts.keys():
            app = self.get("app", key)
            df = pd.DataFrame.from_records(data=dict_of_dicts[key])
            try:
                df = self.norm_df(df, key, source_name)
                normed_category_dfs[key] = df
                suffix = "normed"
            except Exception:
                print(f"An error occurred while norming {key} df")
                completed_category_dfs[key] = df
                suffix = "completed"
            if suffix == "normed":
                try:
                    model_df = self.extract_model_dfs(df, key)
                    finalized_category_dfs[key] = model_df
                    self.save(model_df, f"{key}_finalized", directory=source_name, app=app)
                except Exception:
                    print(f"An error occurred while finalizing {key} df")
            if suff:
                suffix += "_" + suff
            self.save(df, f"{key}_{suffix}", directory=source_name, app=app)
        return normed_category_dfs, completed_category_dfs, finalized_category_dfs

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

    def complete_all_category_items(self, category_dict, debug=False, verbose=False):
        result_dict = {}
        nested_dict = {}
        for key, value in category_dict.items():
            result_dict[key], partial_nested = self.complete_category_items(key,
                                                                            value,
                                                                            debug=debug,
                                                                            verbose=verbose)
            for nested_category in partial_nested.keys():
                if nested_category not in nested_dict.keys():
                    nested_dict[nested_category] = []
                nested_dict[nested_category] += partial_nested[nested_category]
        return result_dict, nested_dict

    @chrono
    def complete_category_items(self, category, data, limit=20, debug=False, verbose=False):
        """"""
        results = []
        nesteds = {}
        count = 0
        for row in data:
            try:
                item_bowl = SoupKitchen(row["url"])
            except requests.exceptions.ConnectTimeout:
                print(f"Connection Timeout for {row}")
                continue
            item_bowl.parse_item(category=category, debug=debug, verbose=verbose)

            result = item_bowl.parsed_rows
            nested = item_bowl.nested_rows
            if result[0]["name"] in row["name"] and self.get("subtype", category) and "subtype" not in result[0].keys():
                result[0]["name"] = row["name"]
            results += result
            for key in nested.keys():
                if key not in nesteds.keys():
                    nesteds[key] = []
                nesteds[key] += nested[key]
            count += 1
            if debug and count == limit:
                break
        print(f"Extracted {len(results)}/{count} items")
        return results, nesteds

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

    def parse_source_links(self, item_list):
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
                snake_item_category = url.split('.')[0]
                item_category = self.snake_to_under(snake_item_category)
                if "General=true" in url:
                    item_category += "_general"
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

    def norm_df(self, df, key, source_name):
        subtype = self.get("subtype", key)
        name_nest = self.get("name_nest", key)
        self.split_text_column(df, "name", name_nest, separator='[', strip=' ]')
        self.split_text_column(df, "name", subtype, separator='(', strip=' )')
        if "level" in df.columns:
            if "spell_type" in self.get("text_columns", key):
                df["spell_type"] = df.apply(
                    lambda r: r["level"].split(' ')[0].strip(),
                    axis=1)
            df["level"] = df.apply(
                lambda r: self.numerize_level(r["level"]),
                axis=1)
        if key == "deities" and "cleric_spells" in df.columns:
            new_columns = ["first_cleric_spell", "second_cleric_spell_level", "second_cleric_spell",
                           "third_cleric_spell_level", "third_cleric_spell"]
            self.split_column(df, "cleric_spells", new_columns, strip=' ')
            df.rename(columns={"cleric_spells": "first_cleric_spell_level"}, inplace=True)

        if key == "bloodlines" and "granted_spells" in df.columns:
            new_columns = ["granted_cantrip"] + [f"granted_{i}" for i in range(1, 10)]
            self.split_column(df, "granted_spells", new_columns, strip=' ', step=2)

        if key == "bloodlines" and "bloodline_spells" in df.columns:
            new_columns = ["initial_bloodline_spell", "advanced_bloodline_spell", "greater_bloodline_spell"]
            self.split_column(df, "bloodline_spells", new_columns, strip=' ', step=2)

        if "description" not in df.columns and "other" in df.columns:
            df.rename(columns={"other": "description"}, inplace=True)
        if "source" in df.columns and "source_page" not in df.columns:
            df.rename(columns={"source": "sources"}, inplace=True)
            df["source"] = df.apply(lambda r: [src for src in r["sources"] if source_name in src], axis=1)
            df["source_page"] = df.apply(
                lambda r: int(r["source"][0].split('pg. ')[-1]) if r['source'] and 'pg. ' in r["source"][0] else 0,
                axis=1)
            df["source"] = df.apply(lambda r: r["source"][0].split('pg. ')[0].strip() if r['source'] else "unknown",
                                    axis=1)
        columns = {'url': 'nethys_url'}
        if "x_finder_related_model" in df.columns and "subtype" not in df.columns:
            columns['x_finder_related_model'] = "subtype"
        df.rename(columns=columns, inplace=True)
        return df

    @staticmethod
    def split_column(df, name, new_names, strip=' ', step=1):
        if name in df.columns and new_names:
            for i in range(len(new_names)):
                new_name = new_names[i]
                df[new_name] = df.apply(
                    lambda r: r[name][i * step + 1].strip(strip) if r[name] and isinstance(r[name], list) else '',
                    axis=1)
            if step == 1:
                df[name] = df.apply(
                    lambda r: r[name][0].strip(strip) if r[name] and isinstance(r[name], list) else '',
                    axis=1)

    @staticmethod
    def split_text_column(df, name, new_name, separator=' ', strip=' '):
        if name in df.columns and new_name:
            df[new_name] = df.apply(
                    lambda r: r[name].split(separator)[1].strip(strip) if r[name] and separator in r[name] else '',
                    axis=1)
            df[name] = df.apply(
                    lambda r: r[name].split(separator)[0].strip(strip) if r[name] and separator in r[name] else '',
                    axis=1)

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
                if debug:
                    print(f"found name {name} for {broken_title}")
                title_end = broken_title.next_sibling
                if not title_end or title_end.name is None and not title_end.get_text().strip(' '):
                    title_end = title_end.next_sibling
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
                if 'level' in self.get("text_columns", category) and len(title_parts) > 1:
                    result.titles[status.ended]['level'] = title_parts[-1]
            if 'action' in self.get("text_columns", category):
                try:
                    result.titles[status.ended]['action'] = child.find('span').get_text(' ,;')
                except AttributeError:
                    pass

    def find_nested_item_category(self, name, url, next_child=None):
        name = name.lower().strip('()[]').replace(' ', '_').replace('-', '_')
        if name.endswith("_tasks") and name.startswith("sample_"):
            return "sample_tasks"
        name_model = self.get("", name)
        url_base = url.split('.')[0].strip().lower().replace(' ', '_').replace('-', '_')
        url_model = self.get("", url_base)
        if next_child is not None and next_child.get_text():
            next_text = next_child.get_text()
            if next_text:
                next_text = next_text.lower().strip(' (),;').replace(' ', '_')
                if not next_text.endswith('s'):
                    next_text += 's'
                next_model = self.get("", next_text)
                if next_model not in ["rules", "default"]:
                    return next_model
        if url_model is not None and url_model not in ["rules", "default"]:
            return url_model
        if name_model != "default":
            return name_model
        return ""

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
        self.nested_rows = {}
        trained = False
        if category == "skills_general" and "(Trained)" in result.titles[0]["name"]:
            trained = True
        for title in result.titles:
            if " Trained Actions" in title["name"]:
                trained = True
            if title and len(title) > 3 and 'x_finder_model' in title.keys():
                current_category = title['x_finder_model']
                if current_category == category:
                    if 'level' in result.titles[0].keys() and 'level' not in title.keys():
                        print(title)
                        continue
                    self.parsed_rows.append(title)
                else:
                    related_item = result.titles[0]["name"].split('(')[0].strip()
                    title["x_finder_related_item"] = related_item
                    title["x_finder_related_model"] = category
                    check_1 = current_category == "actions" and trained
                    check_2 = category == "skills" or category == "skills_general"
                    if check_1 and check_2:
                        if "prerequisite" not in title.keys():
                            title["prerequisite"] = []
                        if category == "skills":
                            title["prerequisite"].append(f"Trained in {related_item}")
                        else:
                            title["prerequisite"].append("Trained in related skill")

                    if self.get("nested", current_category):
                        if current_category in "actions":
                            if "action" not in title.keys() and "traits" not in title.keys():
                                continue
                        if current_category not in self.nested_rows.keys():
                            self.nested_rows[current_category] = []
                        self.nested_rows[current_category].append(title)

        if verbose:
            for title in result.titles:
                if (title and len(title) > 3 and 'x_finder_model' in title.keys()) or debug:
                    print(title)
            if result.parsed:
                print(result.titles[0]["name"], result.parsed)
            if result.tails:
                print(result.titles[0]["name"], result.tails)
        if debug or verbose:
            for title in self.parsed_rows:
                print(title)
            for key in self.nested_rows.keys():
                print(self.nested_rows[key])

    def read_soup(self, child, status, result, category, debug=False, verbose=False):
        if child is None:
            return
        current_category = "default"
        if 'x_finder_model' in result.titles[status.ended].keys():
            current_category = result.titles[status.ended]['x_finder_model']

        if status.last_key:
            if child.name and child.name in self.get("cell_ends", current_category):
                self.store(status, result, category, current_category)
                status.last_key = ""
                status.loaded_values = []
        if status.last_key:
            value = self.clean_text(child.get_text())
            if value:
                if status.last_key == "level" and len(value) > 3:
                    values = value.split('. ')
                    value = values[0]
                    if len(values) > 1:
                        description = ". ".join(values[1:])
                        if "description" not in result.titles[status.ended].keys():
                            result.titles[status.ended]["description"] = []
                        result.titles[status.ended]["description"].append(description)
                status.loaded_values.append(value)

        elif child.name and child.name in self.get("next_titles", current_category):
            key, value = self.format_key(child)
            href = self.get_href(child)
            if key:
                test_category = self.find_nested_item_category(key, href)
                if key in self.get("text_columns", category) + self.get("text_columns", current_category) and not test_category:
                    status.last_key = key
                    status.loaded_values = []
                    if value:
                        status.loaded_values.append(value)
                elif status.family:
                    self.add_new_title(child, result, status, category)
                elif test_category is not None and test_category not in ["default", "rules", category]:
                    self.add_new_title(child, result, status, test_category)
                elif category == "classes":
                    if child.name != "h3":
                        self.add_new_title(child, result, status, "class_features")
                    else:
                        status.last_key = key
                else:
                    self.add_new_title(child, result, status)

        elif child.name and child.name in self.get("cell_starts", current_category):
            key, value = self.format_key(child)
            if key:
                if key in self.get("check_columns", category) and key not in result.titles[0].keys():
                    result.titles[0][key] = True
                    if "description" not in result.titles[0].keys():
                        result.titles[0]["description"] = []
                    result.titles[0]["description"].append(key)
                elif key in self.get("check_columns", current_category) and key not in result.titles[status.ended].keys():
                    result.titles[status.ended][key] = True
                    if "description" not in result.titles[status.ended].keys():
                        result.titles[status.ended]["description"] = []
                    result.titles[status.ended]["description"].append(key)
                elif key not in self.get("text_columns", category) + self.get("text_columns", current_category):
                    test_category = self.find_nested_item_category(key, self.get_href(child), child.next_sibling)
                    if test_category is not None and test_category not in [category, current_category]:
                        self.add_new_title(child, result, status, category=test_category)
                else:
                    status.last_key = key.lower()
                    status.loaded_values = []
                    if value:
                        status.loaded_values.append(value)

        elif child.name == 'span':
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

        elif child.name is None or child.name in self.get("desc_tags", current_category):
            values = []
            if child.name in ['ol', 'ul']:
                values = child.find_all('li')
                values = [self.clean_text(value.get_text()) for value in values if value.get_text() is not None]
            else:
                value = self.clean_text(child.get_text())
                if value:
                    url = self.get_href(child)
                    if url:
                        related_model = self.snake_to_under(url.split('.')[0])
                        if "description_links" not in result.titles[status.ended].keys():
                            result.titles[status.ended]["description_links"] = []
                        result.titles[status.ended]["description_links"].append(f"{value}: {related_model}")
                        value = "<i>" + value + "</i>"
                    values.append(value)
            if values:
                if "description" not in result.titles[status.ended].keys():
                    result.titles[status.ended]["description"] = []
                result.titles[status.ended]["description"] += values

        elif child.name == "table" or child.name == "details" and 'table' in [c.name for c in child.children]:
            key, name, description = [""] * 3
            if result.titles[status.ended]["name"].startswith('Table '):
                key, name = self.format_key(text=result.titles[status.ended]["name"])
                description = result.titles[status.ended].pop("description", "")
                result.titles = result.titles[:status.ended]
                status.ended -= 1
            elif child.find('summary') is not None:
                key, name = self.format_key(child.find('summary'))
            else:
                key, name = self.format_key(text=result.titles[status.ended]["name"])
                key = "table_" + key

            if child.name == "table":
                table = self.load_nested_table(child)
            else:
                table = self.load_nested_table(child.find('table'))

            if key and key not in result.titles[0].keys():
                result.titles[0][key] = {"name": name, "description": description, "table": table}
            else:
                if key is None:
                    key = f"table_{result.titles[0]['name']}"
                if category == "classes" and "spells" in result.titles[0].keys():
                    key = f"table_spells_per_day_{result.titles[0]['name']}"
                    result.titles[0][key] = {"name": name, "description": description, "table": table}
                else:
                    for i in range(1, 50):
                        test = key + "_" + str(i)
                        if test not in result.titles[0].keys():
                            result.titles[0][test] = {"name": name, "description": description, "table": table}
                            break

        elif child.name in ["div", "details"] or "rules%" in child.name:
            children = child.children
            next_child = next(children)
            if next:
                self.read_soup(next_child, status, result, category, debug=debug, verbose=verbose)

        elif child.get_text().strip(' ,;'):
            if child.name in ["h1", "h2"] and child.get_text().startswith("Table "):
                child_name = child.get_text().replace('–', '-').replace(' ', '_').lower().strip()
                title_name = result.titles[status.ended]["name"].replace('–', '-').replace(' ', '_').lower().strip()
                if child_name != title_name:
                    self.add_new_title(child, result, status, current_category)
            else:
                text = self.clean_text(child.get_text())
                if child.name in ["h1", "h2", "h3", "h4"]:
                    text = "<b>" + text + "</b>"
                if "description" not in result.titles[status.ended].keys():
                    result.titles[status.ended]["description"] = []
                result.titles[status.ended]["description"].append(text)
        if child:
            next_child = child.next_sibling
            if next_child:
                try:
                    return self.read_soup(next_child, status, result, category, debug=debug, verbose=verbose)
                except RecursionError:
                    pass
        if status.last_key and status.loaded_values:
            self.store(status, result, category, current_category)
        return None

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
        text = raw_string.strip(' ,;\n\r').replace('\xa0', ' ')
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
            key = nav_string.get_text()
        else:
            key = text
        separator = '('
        if ':' in key:
            separator = ':'
        key_parts = key.split(separator)
        key = key_parts[0].strip(' ,;').lower().replace(' ', '_').replace('-', '_').replace('–', '_')
        value = ""
        if len(key_parts) > 1:
            value = separator.join(key_parts[1:]).strip(' ,;)')
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

    def store(self, status, result, category, current_category):
        match = False
        text_cols = self.get("text_columns", category)
        if status.last_key in text_cols or status.last_key.split('_')[0] in text_cols:
            match = True
            if status.last_key not in result.titles[0].keys() and not status.family:
                result.titles[0][status.last_key] = status.loaded_values
                return
        current_text_cols = self.get("text_columns", current_category)
        if status.last_key in current_text_cols:
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
        if self.get("overload", current_category):
            result.titles[status.ended][status.last_key] = status.loaded_values
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


if __name__ == "__main__":
    bowl = SoupKitchen("Sources.aspx?ID=1")
    bowl.load_source_items()
