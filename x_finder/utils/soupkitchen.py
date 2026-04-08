from time import time, sleep
from multiprocessing import Pool
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

    def cook_url(self, url: str, parser: str = "", keep_alive: bool = True) -> Plate:
        """Sends an url to the provider to get a plate with a soup"""
        plate = self.H.cook_url(url, parser=parser, keep_alive=keep_alive)
        return plate

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
        if not suffix or suffix == "ok":
            for key, value in plate.item_links.items():
                self.H.save(value, plate.category, directory=plate.name)

    def say_hello(self) -> [str, bool]:
        title, got_cookies = self.H.say_hello()
        print(title, got_cookies)
        return title, got_cookies

    def reload_ica(self) -> None:
        self.H.load_ica()
        self.H.dispatch_ica()

    def normalize_dfs(self, plate: Plate, source: str) -> None:
        self.H.normalize_dfs(plate, source)

    def normalize_df(self, plate: Plate, category: str, source: str) -> None:
        self.H.normalize_df(plate, category, source)

    def fit_category_to_model(self, plate: Plate, category: str) -> None:
        self.H.fit_category_to_models(plate, category)

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
                       limit: int = 20,
                       debug: bool = False,
                       verbose: bool = False) -> None:
        results, missed = self.complete_category_items(source_plate,
                                                       category,
                                                       limit=limit,
                                                       debug=debug,
                                                       verbose=verbose)
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
            item_plate.category = category
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
        if category in results.keys():
            print(f"Extracted {len(results[category])}/{count} items")
        else:
            print(f"No member of {category} found, check it's ICA's cell_start and next_tittles")
        return results, missed

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

    def extract_sources(self) -> Plate | None:
        """  """
        plate = self.H.extract_sources()
        if plate:
            return plate
        return None

    def sort_sources_editions(self, plate: Plate, groups: list[str] | None = None) -> None:
        if not groups:
            groups = ["rulebooks"]
        self.H.sort_sources(plate, self.editions, groups=groups)

    @staticmethod
    def clean_nav_links(nav_links) -> None:
        """Overload in child if needed"""
        print(nav_links)


if __name__ == "__main__":
    bowl = SoupKitchen("nethys", "remaster")
    print(bowl.H.provider.get("base_url"))
