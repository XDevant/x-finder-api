import pandas as pd
from time import time, sleep
from multiprocessing import Pool
from pathlib import Path
from utils import U
from selector import handler_selector

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

    def cook_url(self, url, parser=""):
        """Sends an url to the provider to get a plate with a soup"""
        plate = self.H.cook_url(url, parser=parser)
        return plate

    def parse_item(self, plate, debug=False, verbose=False):
        """Sends a provided plate to the parser to complete it"""
        self.H.parse_item(plate, debug=debug, verbose=verbose)

    def extract_source_links(self, plate):
        self.H.extract_source_links(plate)

    def save_source_links(self, link_dict, source, suffix):
        for key, value in link_dict.items():
            self.H.build_df(value, source_name=source, category=f"{key}_links_{suffix}")

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

    def complete_all_category_items(self, source_plate, debug=False, verbose=False):
        """
        :param source_plate: Plate instance with { "category_a": [{"item_name": String, "url": String}, ...], ...}
        :param debug: Bool
        :param verbose: Bool
        :return: None
        """
        for key in source_plate.data_dict.keys():
            results, missed = self.complete_category_items(source_plate, key, debug=debug, verbose=verbose)
            for category in results.keys():
                if category not in source_plate.data_dict.keys():
                    source_plate.data_dict[category] = []
                source_plate.data_dict[category] += results[category]
        completed_dfs = self.H.build_dfs(source_plate.data_dict, source_name=source_plate.name)
        source_plate.data_dict = completed_dfs

    def complete_category(self, source_plate, category, limit=20):
        results, missed = self.complete_category_items(source_plate, category, limit=limit)
        for category in results.keys():
            if category not in source_plate.data_dict.keys():
                source_plate.data_dict[category] = []
            source_plate.data_dict[category] += results[category]
        completed_dfs = self.H.build_dfs(source_plate.data_dict, source_name=source_plate.name)
        source_plate.data_dict = completed_dfs

    @chrono
    def complete_category_items(self, source_plate, category, limit=20, debug=False, verbose=False):
        """
        :param category: String, in ica.keys()
        :param source_plate: a Plate instance,  item_links dict: {category: [{"name": String, "url": String}, ..], ..}
        :param limit: Int, if debug is True, will only complete the first 20 rows by default
        :param debug: Bool
        :param verbose: Bool, more prints
        :return: Dict of list of dict (item[category]=category), list if dicts (item[category]!=category)
        """
        results = {}
        missed = {}
        count = 0
        for row in source_plate.item_links[category]:
            item_plate = self.H.cook_url(url=row["url"])

            self.H.parse_item(item_plate, category=category, debug=debug, verbose=verbose)
            result = item_plate.data_dict[category]
            target = result[0]
            check = self.H.get("subtype", category)
            if item_plate.completed and target["name"] in row["name"] and check and "subtype" not in target.keys():
                target["name"] = row["name"]
            results += result
            for key in item_plate.data_dict.keys():
                if key not in results.keys():
                    results[key] = []
                results[key] += item_plate.data_dict[key]
            count += 1
            if debug and count == limit:
                break
        print(f"Extracted {len(results)}/{count} items")
        return results, missed

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
            value = self.H.provider.cook_from_html(parsed_row[header])  # overkill and bugged
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
            value = self.H.provider.cook_from_html(value)
            if url:
                try:
                    url = value.find('a')['href']
                    return url
                except TypeError:
                    return ""
            text = value.get_text()
            return text.strip()
        return ""

    def extract_nav_links(self, nav_soup):
        """ If our table is split among sub-tables, we fetch their urls.
        We store their names / urls as key / value pairs in a dict """
        if nav_soup:
            main = nav_soup.find(id="main").span
            nav_link_list = main.find_all('a')
            nav_links = {link.get_text(): link['href'] for link in nav_link_list}
            self.clean_nav_links(nav_links)
            print(f"{len(nav_links)} navigation links extracted.")
            return nav_links
        else:
            print("No soup found, did you cook it?")
        return {}

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


if __name__ == "__main__":
    bowl = SoupKitchen("nethys", "remaster")
    print(bowl.H.provider.get("base_url"))
