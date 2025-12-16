from time import time, sleep
from multiprocessing import Pool
from utils import U
from selector import handler_selector
from helpers import Plate
from datahandling import Dh


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
    def __init__(self, target, edition, parser='html.parser'):
        self.target: str = target
        self.edition: str = edition
        self.parser: str = parser
        self.H: Dh = handler_selector("nethys", "remaster")

    def __str__(self) -> str:
        return f"Targeting {str(self.target)} {str(self.edition)} "

    def cook_url(self, url: str, parser: str = "", keep_alive: bool = True) -> Plate:
        """Sends an url to the provider to get a plate with a soup"""
        plate = self.H.cook_url(url, parser=parser, keep_alive=keep_alive)
        return plate

    def parse_item(self, plate: Plate, debug: bool = False, verbose: bool = False) -> None:
        """Sends a provided plate to the parser to complete it"""
        self.H.validate_plate(plate)
        if plate.category == "sources":
            self.H.extract_source_links(plate)
            self.save_source_links(plate, suffix="ok")
        self.H.parse_item(plate, debug=debug, verbose=verbose)

    def extract_source_links(self, plate: Plate):
        self.H.extract_source_links(plate)

    def save_source_links(self, plate: Plate, suffix: str = "") -> None:
        for key, value in plate.item_links.items():
            self.H.build_df(value, source_name=plate.name, category=f"{key}__links", suffix=suffix)

    def update_worker(self, worker):
        self.H.update_worker(worker)

    @staticmethod
    def get_source_name(source_soup, update: bool = False) -> str:
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

    def parse_all_category(self, source_plate: Plate, debug: bool = False, verbose: bool = False) -> None:
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
                    source_plate.data_dict[category] = [results[category]]
                else:
                    source_plate.data_dict[category] += results[category]
        completed_dfs = self.H.build_dfs(source_plate.data_dict, source_name=source_plate.name)
        source_plate.dfs = completed_dfs

    def parse_category(self,
                       source_plate: Plate,
                       category: str,
                       limit: int = 20) -> None:
        results, missed = self.complete_category_items(source_plate, category, limit=limit)
        for key in results.keys():
            if key not in source_plate.data_dict.keys():
                source_plate.data_dict[key] = []
            source_plate.data_dict[key] += results[key]
        completed_dfs = self.H.build_dfs(source_plate.data_dict, source_name=source_plate.name)
        source_plate.dfs = completed_dfs

    @chrono
    def complete_category_items(self,
                                source_plate: Plate,
                                category: str,
                                limit: int = 20,
                                debug: bool = False,
                                verbose: bool = False
                                ) -> (dict[str, list[dict[str, str]]], dict[list[dict[str, str]]]):
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
            print(row)
            item_plate = self.cook_url(url=row["url"])
            self.parse_item(item_plate, debug=debug, verbose=verbose)
            if category not in item_plate.data_dict.keys():
                if category not in missed.keys():
                    missed[category] = []
                    print(item_plate.data_dict)
                missed[category] += [row]
                continue
            result = item_plate.data_dict[category]
            target = result[0]
            check = self.H.get("subtype", category)
            if item_plate.completed and target["name"] in row["name"] and check and "subtype" not in target.keys():
                target["name"] = row["name"]
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
            value = self.H.provider.cook_from_html(parsed_row[header], self.H.provider.parser)  # overkill and bugged
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
            value = self.H.provider.cook_from_html(value, self.H.provider.parser)
            if url:
                try:
                    url = value.find('a')['href']
                    return url
                except TypeError:
                    return ""
            text = value.get_text()
            return text.strip()
        return ""

    def extract_sources(self, types: list[str] | None = None) -> Plate | None:
        """  """
        if not types:
            types = ["rulebooks"]
        plate = self.H.extract_sources()
        if plate:
            self.H.build_df(plate.data_dict["sources"], source_name="remaster", category="unsorted")
            remaster, legacy = self.H.parse_sources_editions(plate, types=types)
            if legacy.data_dict and "sources" in legacy.data_dict.keys() and legacy.data_dict["sources"]:
                self.H.build_df(legacy.data_dict["sources"], source_name="sources", category="legacy")
            if remaster.data_dict and "sources" in remaster.data_dict.keys() and remaster.data_dict["sources"]:
                self.H.build_df(remaster.data_dict["sources"], source_name="sources")
                return remaster
            return plate
        return None

    def sort_sources_editions(self, plate: Plate) -> Plate | None:
        remaster, legacy = self.H.parse_sources_editions(plate, types=types)
        if legacy.data_dict and "sources" in legacy.data_dict.keys() and legacy.data_dict["sources"]:
            self.H.build_df(legacy.data_dict["sources"], source_name="sources", category="legacy")
        if remaster.data_dict and "sources" in remaster.data_dict.keys() and remaster.data_dict["sources"]:
            self.H.build_df(remaster.data_dict["sources"], source_name="sources")
            return remaster
        return None

    @staticmethod
    def clean_nav_links(nav_links) -> None:
        """Overload in child if needed"""
        print(nav_links)

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
