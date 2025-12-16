import pandas as pd
from os import makedirs
import json
from args import Ica
from provider import Provider
from parser import Parser
from normalizer import Normalizer
from modeler import Modeler
from x_finder.x_finder.settings import BASE_DIR
from typing import Iterable
from helpers import Plate, Status, Result
from importlib import reload, invalidate_caches

"""Df is not supposed to do much but provide base methods and hooks for each handler that will inherit from Df """


class Dh:
    """Here we parse the item's data, check for nested item's and turn navigable strings into rows of data
     then we build a panda dataframe for each category of items and normalize them for data tidying
     and make sure our dfs match our database models and extract tables through.
     """

    def __init__(self, target="", edition=""):
        self.target: str = target
        self.edition: str = edition
        self.ica: dict[str, dict[str, str]] | None = None
        self.provider: Provider | None = None
        self.parser: Parser | None = None
        self.normalizer: Normalizer | None = None
        self.modeler: Modeler | None = None
        self.instantiate_provider()
        self.instantiate_parser()
        self.instantiate_normalizer()
        self.instantiate_modeler()
        self.dispatch_ica()

    def instantiate_provider(self) -> None:
        self.provider = Provider(self.target, self.edition)

    def instantiate_parser(self) -> None:
        self.parser = Parser(self.target, self.edition)

    def instantiate_normalizer(self) -> None:
        self.normalizer = Normalizer(self.target, self.edition)

    def instantiate_modeler(self) -> None:
        self.modeler = Modeler(self.target, self.edition)

    def dispatch_ica(self) -> None:
        with open(f"{BASE_DIR}\\utils\\{self.target}\\{self.edition}\\args.json") as file:
            self.ica = json.load(file)
        self.provider.ica = self.ica
        self.parser.ica = self.ica
        self.normalizer.ica = self.ica
        self.modeler.ica = self.ica

    def get(self, argument: str, category: str = "default", keys: bool = False) -> str | Iterable[str]:
        return Ica.get(self.ica, argument, category, keys)

    @staticmethod
    def save(df, name: str, directory: str | None = None, app: str = "utils") -> None:
        path = f"{BASE_DIR}\\{app}\\fixtures\\csv\\"
        if directory:
            path += directory + "\\"
        makedirs(path, exist_ok=True)
        df.to_csv(path + name + ".csv", sep='|', index=False)
        print(f"{name} successfully saved at {path}.")

    @staticmethod
    def load(name: str, app: str = "utils", directory: str | None = None, suffix: str = "") -> pd.DataFrame:
        if directory:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{directory}\\{name}{'_' if suffix else ''}{suffix}.csv"
        else:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{name}{'_' if suffix else ''}{suffix}.csv"
        df = pd.read_csv(pathfile, delimiter="|")
        return df

    def load_df(self, category: str, source: str = "", suffix: str = "", app: str = "utils") -> pd.DataFrame:
        directory = self.target + "/" + self.edition
        if source:
            directory += "/" + source
        df = self.load(category, app=app, directory=directory, suffix=suffix)
        return df

    def build_dfs(self,
                  dict_of_lists_of_dicts: dict[str, list[dict[str, str]]],
                  source_name: str = "Unknown",
                  suffix: str = ""
                  ) -> dict[str, pd.DataFrame]:
        completed_category_dfs = {}
        for key in dict_of_lists_of_dicts.keys():
            list_of_dicts = dict_of_lists_of_dicts[key]
            df = self.build_df(list_of_dicts, source_name=source_name, category=key, suffix=suffix)
            completed_category_dfs[key] = df
        return completed_category_dfs

    def build_df(self,
                 list_of_dicts: list[dict[str, str]],
                 source_name: str = "Unknown",
                 category: str = "unknown",
                 suffix: str = ""
                 ) -> pd.DataFrame:
        df = pd.DataFrame.from_records(data=list_of_dicts)
        if not suffix:
            suffix = "completed"
        suffix = f"{category}{'__' if suffix else ''}{suffix}"
        directory = self.target + "\\" + self.edition
        if source_name == self.edition or source_name == self.target:
            if source_name == self.edition:
                suffix = "sources"
            else:
                suffix = "source_groups"
            if category != "unknown":
                suffix += "_" + category
        else:
            directory += "\\" + source_name
        self.save(df, suffix, directory=directory, app="utils")
        return df

    def finalize_completed_dfs(self, dict_of_dfs: dict[str, pd.DataFrame], source_name: str) -> None:
        normed_category_dfs = {}
        finalized_category_dfs = {}
        missed_category_dfs = {}
        for key in dict_of_dfs.keys():
            app = self.get("app", key)
            df = dict_of_dfs[key]
            try:
                normed_df = self.normalizer.norm_df(df, key, source_name)
                normed_category_dfs[key] = normed_df
                suffix = "normed"
            except Exception:
                print(f"An error occurred while norming {key} df")
                missed_category_dfs[key] = df
                suffix = "missed"
            if suffix == "normed":
                try:
                    model_df = self.modeler.extract_model_dfs(df, key)
                    finalized_category_dfs[key] = model_df
                    self.save(model_df, f"{key}__finalized", directory=source_name, app=app)
                except Exception:
                    print(f"An error occurred while finalizing {key} df")

    def extract_source_links(self, plate: Plate, debug: bool = False, verbose: bool = False) -> None:
        unsorted_links = self.parser.extract_source_links(plate.soup)
        plate.item_links, plate.no_category_item_links = self.parser.parse_source_links(unsorted_links)
        if debug or verbose:
            print(plate.no_category_item_links)

    def validate_plate(self, plate: Plate, debug: bool = False, verbose: bool = False) -> None:
        validated = self.parser.validate_plate(plate)
        if validated:
            plate.status = "validated"
        if debug or verbose:
            print(f"Plate {'' if plate.validated else 'not'} validated")

    def parse_item(self, plate: Plate, debug: bool = False, verbose: bool = False) -> None:
        if plate.status == "validated":
            status = Status(plate.name, plate.url)
            result = Result()
            title = self.parser.find_start(plate, status, debug=False, verbose=False)
            result.titles.append(title)
            if debug:
                print("result title", *result.titles, status.start is not None)
            if status.start and title:
                parsed_rows = self.parser.parse_item(plate, status=status, result=result,
                                                     debug=debug, verbose=verbose)
                items = self.parser.complete_plate(plate, parsed_rows)
                plate.data_dict = items
                if items:
                    plate.status = "completed"
            else:
                print(f"title = {title}, start is {'none' if status.start is None else 'not none'}")
        else:
            print("Plate not valid")

    def cook_url(self, url: str, parser: str = "", keep_alive: bool = False) -> Plate:
        plate = Plate(url=url)
        self.provider.cook(plate, parser=parser, keep_alive=keep_alive)
        return plate

    def extract_sources(self) -> Plate | None:
        url = self.get("index_url")
        plate = self.cook_url(url, keep_alive=True)
        link_list = self.parser.extract_links_by_id(plate, self.get("nav_id"), self.get("item_title"))
        print(link_list)
        if not link_list:
            return None
        self.build_df(link_list, source_name="nethys")
        unsorted_list = []
        for source_type in link_list:
            type_plate = self.cook_url(source_type["url"], keep_alive=True)
            for source in type_plate.item_links["sources"]:
                source["type"] = source_type["name"]
                unsorted_list.append(source)
        if unsorted_list:
            print(unsorted_list)
            plate.data_dict["sources"] = unsorted_list
            plate.status = "completed"
            return plate
        return None

    def parse_sources_editions(self, source_plate: Plate, types: list[str] | None = None) -> [Plate, Plate]:
        remaster_plate = Plate(category="sources")
        remaster_plate.data_dict["sources"] = []
        legacy_plate = Plate(category="sources")
        legacy_plate.data_dict["sources"] = []
        source_list = source_plate.data_dict["sources"]
        for source in source_list:
            if not types or source["type"].lower() in types:
                plate = self.cook_url(source["url"], keep_alive=True)
                self.validate_plate(plate)
                self.parse_item(plate)
                print(plate.data_dict)
                remaster_list, legacy_list = self.parser.sort_sources(plate)
                remaster_plate.data_dict["sources"] += remaster_list
                legacy_plate.data_dict["sources"] += legacy_list
                if remaster_list:
                    self.extract_source_links(plate)
                    for key, value in plate.item_links.items():
                        self.build_df(value, source_name=plate.name, category=f"{key}__links", suffix="ok")
        return remaster_plate, legacy_plate

    def update_worker(self, worker: str) -> None:
        reload(self.__getattribute__(worker.title()))
        invalidate_caches()
        self.__getattribute__(f"instantiate_{worker}")()

    def find_nested_item_category(self, name, url, next_child=None, category="default") -> str:
        name = name.lower().strip('()[]').replace(' ', '_').replace('-', '_')
        if category == "monsters":
            if name in ["melee", "ranged"]:
                return "monster_attacks"
            if "spells" in name:
                return "monster_spells"
            return "monster_abilities"
        if name in ["melee", "ranged"]:
            return "animal_attacks"
        if name == "activate":
            return "equipment_activations"
        if name.endswith("_tasks") and name.startswith("sample_"):
            return "sample_tasks"

        if next_child is not None and next_child.get_text():
            next_text = next_child.get_text()
            if next_text:
                next_text = next_text.lower().strip(' (),;').replace(' ', '_')
                if not next_text.endswith('s'):
                    next_text += 's'
                next_model = self.get("", next_text)
                if next_model not in ["rules", "default"]:
                    return next_model

        name_model = self.get("", name)
        url_base = url.split('.')[0].strip().lower().replace(' ', '_').replace('-', '_')
        url_model = self.get("", url_base)
        if url_model is not None and url_model not in ["rules", "default"]:
            return url_model
        if name_model != "default":
            return name_model
        return ""


if __name__ == "__main__":
    with open(f"{BASE_DIR}\\utils\\nethys\\remaster\\args.json") as f:
        ica = json.load(f)
        print(ica)
