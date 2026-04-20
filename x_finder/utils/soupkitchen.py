from time import time, sleep
from multiprocessing import Pool

import pandas as pd

from selector import Selector
from helpers.helpers import Plate
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
        selector = Selector("nethys", "remaster")
        self.H: Dh = selector.handler
        self.targets = selector.targets
        self.editions = selector.editions

    def __str__(self) -> str:
        return f"Targeting {str(self.target)} {str(self.edition)}"

    def cook_url(self, link: dict[str, str], parser: str = "", keep_alive: bool = True) -> Plate:
        """Sends an url to the provider to get a plate with a soup"""
        plate = self.H.cook_url(link["url"], parser=parser, keep_alive=keep_alive)
        if plate.soup is not None:
            link["soup"] = str(plate.soup)
        return plate

    def cook_urls(self, source_plate: Plate):
        if source_plate.item_links is None or source_plate.item_links.empty:
            return
        else:
            df = source_plate.item_links
            soup_dict = {name: self.H.cook_url(url).soup for name, url in zip(df["name"].to_list(), df["url"].to_list())}
            for key, value in soup_dict.keys():
                df["soup"]["name" == key] = value
            source_plate.item_links = df

    def parse_item(self, plate: Plate, debug: bool = False, verbose: bool = False) -> None:
        """Sends a provided plate to the parser to complete it"""
        if plate.soup:
            self.H.parse_item(plate, debug=debug, verbose=verbose)
        elif debug:
            print("-- No soup in my plate !")

    def extract_source_links(self,
                             plate: Plate,
                             debug: bool = False,
                             verbose: bool = False,
                             to_csv: bool = False
                             ) -> None:
        self.H.extract_source_links(plate, debug=debug, verbose=verbose)
        if to_csv:
            self.save_source_links(plate)

    def save_source_links(self, plate: Plate, suffix: str = "") -> None:
        new_suffix = "links"
        if suffix:
            new_suffix += "_" + suffix
        if not suffix or suffix == "ok" and plate.item_links is not None:
            if plate.item_links is not None:
                for key, value in plate.item_links.items():
                    self.H.save(value, plate.category, directory=plate.name)

    def say_hello(self) -> tuple[str, bool]:
        title, got_cookies = self.H.say_hello()
        print(title, got_cookies)
        return title, got_cookies

    def reload_ica(self) -> None:
        self.H.load_ica()
        self.H.dispatch_ica()

    def normalize_dfs(self, plate: Plate, source: str) -> None:
        if plate.dfs is None:
            return
        for category in plate.dfs.keys():
                self.normalize_df(plate, category, source)

    def normalize_df(self, plate: Plate, category: str, source: str) -> None:
        self.H.normalize_df(plate, category, source)

    def fit_category_to_model(self, plate: Plate, category: str, source: str) -> None:
        self.H.fit_category_to_models(plate, category, source)

    def fit_source_to_model(self, plate: Plate, source: str) -> None:
        if plate.dfs is None:
            return
        for category in plate.dfs.keys():
            self.fit_category_to_model(plate, category, source)

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
        """
        if source_plate.item_links is None:
            return
        data_dict = {}
        for key in source_plate.item_links.keys():
            results, missed = self.complete_category_items(source_plate, key, debug=debug, verbose=verbose)
            for category in results.keys():
                if category not in data_dict.keys():
                    data_dict[category] = results[category]
                else:
                    data_dict[category] += results[category]
        source_plate.data_dict = data_dict
        source_plate.dfs = {key: pd.DataFrame.from_records(value) for key, value in data_dict.items()}

    def parse_category(self,
                       source_plate: Plate,
                       category: str,
                       limit: int = 20,
                       debug: bool = False,
                       verbose: bool = False) -> None:

        results, missed = self.complete_category_items(source_plate,
                                                       category,
                                                       limit=limit,
                                                       debug=debug,
                                                       verbose=verbose)
        if source_plate.data_dict is None:
            data_dict = {}
        else:
            data_dict = source_plate.data_dict
        for key in results.keys():
            if key not in data_dict.keys():
                data_dict[key] = []
            data_dict[key] += results[key]
        source_plate.data_dict = data_dict
        completed_dfs = self.H.build_dfs(data_dict, source_name=source_plate.name)
        source_plate.dfs = completed_dfs

    @chrono
    def complete_category_items(self,
                                source_plate: Plate,
                                category: str,
                                limit: int = 20,
                                debug: bool = False,
                                verbose: bool = False
                                ) -> tuple[dict[str, list[dict[str, str]]], dict[str, list[dict[str, str]]]]:
        results = {}
        missed = {}
        count = 0
        if source_plate.item_links is None:
            return results, missed
        category_df = source_plate.item_links["category" == category]
        vectoriel_zip = zip(category_df["name"].to_list(), category_df["url"].to_list())
        for name, url in vectoriel_zip:
            link = {"name": name, "url": url, "source": source_plate.name, "category": category}
            item_plate = self.cook_url(link=link)
            item_plate.category = category
            self.parse_item(item_plate, debug=debug, verbose=verbose)
            if item_plate.data_dict is None or category not in item_plate.data_dict.keys():
                if category not in missed.keys():
                    missed[category] = []
                if debug:
                    print(f"missed: {item_plate.data_dict}")
                missed[category] += {"name": name, "url": url, "category": category}
                continue
            result = item_plate.data_dict[category]
            target = result[0]
            check = self.H.sica("subtype", category)
            if item_plate.completed and target["name"] in name and check and "subtype" not in target.keys():
                target["name"] = name
            for key in item_plate.data_dict.keys():
                if key not in results.keys():
                    results[key] = []
                results[key] += item_plate.data_dict[key]
            count += 1
            if debug and count == limit:
                break
        if category in results.keys():
            print(f"Extracted {len(results[category])}/{count} items")
        else:
            print(f"No member of {category} found, check it's ICA's cell_start and next_tittles")
        return results, missed

    def extract_sources(self) -> Plate | None:
        """  """
        plate = self.H.extract_sources()
        if plate:
            return plate
        return None

    def sort_sources_editions(self,
                              source_plate: Plate,
                              groups: list[str] | None = None,
                              debug: bool = False) -> None:
        if not groups and debug:
            groups = ["rulebooks"]
        if source_plate.data_dict is None:
            return
        source_list = source_plate.data_dict["sources"]
        for edition in self.editions:
            source_plate.data_dict[edition] = []
        links = []
        for source in source_list:
            group = source["group"]
            if not groups or group.lower() in groups:
                data = self.H.sort_source(source)
                source_plate.data_dict[data["edition"]].append(data["row"])
                if "df" in data.keys():
                    links.append(data["df"])
        links = pd.concat(links)
        if source_plate.dfs is None:
            source_plate.dfs = {"links": links}
        else:
            source_plate.dfs["links"] = links
        for key, value in source_plate.data_dict.items():
            df = pd.DataFrame.from_records(data=value)
            print(df.head(), df.columns)
            if source_plate.dfs is None:
                source_plate.dfs = {key: df}
            else:
                source_plate.dfs[key] = df
            source_plate.completed = True

    @staticmethod
    def clean_nav_links(nav_links) -> None:
        """Overload in child if needed"""
        print(nav_links)


if __name__ == "__main__":
    bowl = SoupKitchen("nethys", "remaster")
    print(bowl.H.provider.sica("base_url"))
