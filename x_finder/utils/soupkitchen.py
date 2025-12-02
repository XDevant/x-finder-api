import requests
import pandas as pd
from bs4 import BeautifulSoup
from time import time, sleep
from multiprocessing import Pool
from pathlib import Path
from utils import U
from selector import handler_selector

BASE_DIR = Path(__file__).resolve().parent.parent
Handler = handler_selector("nethys", "remaster")


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
    nav_links = None
    parsed_row = None
    item_links = None
    completed_dfs = {}
    normed_dfs = {}
    name = ""

    def __init__(self, target, edition, parser='html.parser'):
        self.target = target
        self.edition = edition
        self.parser = parser
        self.H = handler_selector("nethys", "remaster")

    def __str__(self):
        return f"Targeting {str(self.target)} {str(self.edition)} "

    def cook(self, url=None):
        name = url.split('.')[0]
        if name == self.target:
            pass

    def extract_nav_links(self, nav_soup):
        """ If our table is split among sub-tables, we fetch their urls.
        We store their names / urls as key / value pairs in a dict """
        if nav_soup:
            main = nav_soup.find(id="main").span
            nav_link_list = main.find_all('a')
            nav_links = {link.get_text(): link['href'] for link in nav_link_list}
            self.clean_nav_links(nav_links)
            print(f"{len(nav_links)} navigation links extracted.")
        else:
            print("No soup found, did you cook it?")

    @staticmethod
    def clean_nav_links(nav_links):
        """Overload in child if needed"""
        print(nav_links)

    def load_table(self, table_soup, category="default", save=True):
        if not table_soup:
            print("No soup found, did you cook it?")
            return
        table_rows, headers = U.find_table(table_soup)
        if not table_rows or not headers:
            print("No table found.")
            return

        item_url_col = self.H.get("item_url_column", category)
        text_cols = self.H.get("text_columns", category)
        url_cols = self.H.get("url_columns", category)
        url_index = self.H.get_index(headers, item_url_col.lower())
        table_rows, counter = U.build_rows(table_rows, url_index)
        if url_index < 0:
            df = pd.DataFrame(data=table_rows, columns=headers)
            print("Table successfully extracted")
            print(url_index, category, item_url_col, headers)
            return df

        if "nethys_url" not in headers:
            headers += ["nethys_url"]
        if not text_cols and not url_cols:
            df = pd.DataFrame(data=table_rows, columns=headers)
            print(f"Table extracted with {counter} missing item url{'s' if counter >1 else ''}.")
            return df

        if text_cols:
            headers += [U.format_column_name(n) for n in text_cols]
        if url_cols:
            headers += [U.format_column_name(n, url=True) for n in url_cols]

        for row in table_rows:
            item_url = row[-1]
            if item_url:
                new_bowl = self.H.provider.cook(item_url)
                parsed_row = self.parse_item_data(new_bowl, category=category)
                for column in text_cols:
                    row.append(self.get_item_data(parsed_row, column))
                for column in url_cols:
                    row.append(new_bowl.get_item_data(parsed_row, column, url=True))
            else:
                row += [None] * len(text_cols + url_cols)
        df = pd.DataFrame(data=table_rows, columns=headers)
        print(f"Table {category} extracted with {counter} missing item url{'s' if counter > 1 else ''}.")
        if save:
            U.save(df, f"{category}_sources_completed", app="core")
        return df

    def load_sub_tables(self, category="sources"):
        """After nav link extraction we load each table using load_table
        then contact all in one df"""
        dfs = []
        for key, url in self.nav_links.items():
            sub_bowl = self.H.provider.cook(url)
            sub_df = self.load_table(sub_bowl, category=category, save=False)
            sub_df["category"] = [key] * len(sub_bowl.df)
            dfs.append(sub_df)
        df = pd.concat(dfs)
        U.save(df, "sources_completed", app="core")

    def parse_item_data(self, detail_soup, show=False, category="default"):
        """Once our table or list of items is loaded, we often need to
        fetch additional data on the item's page. Here we parse that page
        thanks to the markup provided to the constructor."""
        args = ["start", "end", "row_separator", "cell_separator", "tail_start", "row_sep_bis", "traits"]
        start, end, row, cell, tail, row2, traits = (self.H.get(arg, category) for arg in args)
        raw_data = detail_soup.find(id="main")
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
            parsed_row["Traits"] = U.find_traits(raw_data)
        if category in ["spells", "feats", "equipment", "weapons", "armor", "shield"]:
            parsed_row["Spell Level"] = U.find_level(raw_data)
        if category == "deities":
            if "Pantheons" in parsed_row.keys() and parsed_row["Pantheons"] is not None:
                parsed_row["Pantheons"] = parsed_row["Pantheons"].split('<h2')[0]
            if "Cleric Spells" in parsed_row.keys() and parsed_row["Cleric Spells"] is not None:
                split_row = parsed_row["Cleric Spells"].split('<h2')
                parsed_row["Cleric Spells"] = split_row[0]
                parsed_row["Divine Intercession"] = split_row[-1].split('</h2>')[-1]
        if show:
            print(parsed_row)
        return parsed_row

    def load_source_items(self,
                          source_soup,
                          update=False,
                          offset=3,
                          from_df=None,
                          category_filter=None,
                          source_name="unknown",
                          category="default",
                          debug=False,
                          verbose=False):
        """ The base use is to extract from a source a list of links (items) and then extract data from those links.
        Links encountered will be sorted into different category according to the url found and stored into a dict
        :param source_soup: NavigableString
        :param update: Bool used as a suffix for filename of csv, used by the commands when loading into db
        :param offset: Int used to filter the first source_links only in extract_source_links
        :param from_df: Panda df used to complete a single item category previously saved, bypass extract_source_links
        :param category_filter: List of categories you want to complete. If None, all categories will be completed
        :param source_name: String, used with from_df, name of the subdirectory where completed df will be saved
        :param category: String, base name of the file if from_df is not none
        :param debug: Bool
        :param verbose: Bool
        :return: nothing
        """
        if not from_df:
            item_links = self.extract_source_links(source_soup, offset)
            if source_name == "unknown":
                source_name = self.get_source_name(update)
            category_data, no_category_data = U.parse_source_links(item_links)

            new_category_dfs = {}
            for key, value in no_category_data.items():
                df = pd.DataFrame.from_records(data=value)
                new_category_dfs[key] = df
                U.save(df, f"{key}_raw", directory=source_name)
        else:
            category_data = {category: from_df.to_dict('records')}
        if category_filter is not None and isinstance(category_filter, list):
            category_data = {key: value for key, value in category_data.items() if key in category_filter}
        completed_categories, nested_categories = self.complete_all_category_items(category_data,
                                                                                   debug=debug,
                                                                                   verbose=verbose)
        completed_dfs = self.H.build_dfs(completed_categories, source_name=source_name)
        completed_nested_dfs = self.H.build_dfs(nested_categories, source_name=source_name, suff="nested")
        for key in completed_nested_dfs.keys():
            if key not in completed_dfs.keys():
                completed_dfs[key] = completed_nested_dfs[key]
        self.completed_dfs = completed_dfs

    @staticmethod
    def extract_source_links(source_soup, offset):
        if source_soup:
            try:
                item_list = source_soup.find(id="main").find_all('u')
            except AttributeError:
                print(source_soup)
                return []
            try:
                link_list = [item.a for item in item_list[offset:] if item.a is not None]
                return link_list
            except IndexError:
                print(item_list)
        print("No soup found, did you cook it?")
        return []

    @staticmethod
    def get_source_name(source_soup, update=False):
        if source_soup:
            try:
                name = source_soup.find(id="main").find(class_="title").a.get_text()
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
        missed = []
        count = 0
        for row in data:
            try:
                item_bowl = self.H.provider.cook(row["url"])
            except requests.exceptions.ConnectTimeout:
                print(f"Connection Timeout for {row}")
                missed.append(row)
                continue
            result, nested = self.H.parser.parse_item(item_bowl, category=category, debug=debug, verbose=verbose)

            check = self.H.get("subtype", category)
            if result and result[0]["name"] in row["name"] and check and "subtype" not in result[0].keys():
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

    def get_item_data(self, parsed_row, header, url=False):
        """Here we use the missing columns' headers given to the constructor to fetch
        the missing item data in the row we just parsed.
        Args:
            parsed_row  : Dict
            header      : String
            url         : Bool
        Return String
        """
        if parsed_row and header and header in parsed_row.keys():
            value = self.H.cook(parsed_row[header])
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


if __name__ == "__main__":
    bowl = SoupKitchen("nethys", "remaster")
    print(bowl.H.provider.get("base_url"))
    test_soup = bowl.H.provider.cook(url="Sources.aspx?ID=216")
    if test_soup:
        test_links = bowl.extract_source_links(test_soup, 0)
        sorted_links, unknown = U.parse_source_links(test_links)
        weapons = sorted_links["weapons"]
        if weapons:
            parsed = bowl.complete_category_items("weapons", weapons, limit=2, debug=True, verbose=True)
