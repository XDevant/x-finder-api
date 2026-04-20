import pandas as pd
from os import makedirs
import json
import sqlite3 as lite
from args import Ica
from provider import Provider
from parser import Parser
from normalizer import Normalizer
from modeler import Modeler
from reader import Reader
from x_finder.x_finder.settings import BASE_DIR
from helpers.helpers import Plate, Status, Result
from importlib import reload, invalidate_caches, import_module

"""Df is not supposed to do much but provide base methods and hooks for each handler that will inherit from Df """


class Dh:
    """Here we parse the item's data, check for nested item's and turn navigable strings into rows of data.
     We build a panda dataframe for each category of items and normalize them for data tidying
     and make sure our dfs match our database models and extract tables through.
     """
    def __init__(self, target="", edition=""):
        self.target: str = target
        self.edition: str = edition
        self.ica: dict[str, dict[str, str | list[str]]] = {}
        self.provider: Provider = Provider(self.target, self.edition)
        self.parser: Parser = Parser(self.target, self.edition)
        self.normalizer: Normalizer = Normalizer(self.target, self.edition)
        self.modeler: Modeler = Modeler(self.target, self.edition)
        self.path: str = f"{BASE_DIR}\\utils\\"
        self.load_ica()
        self.dispatch_ica()

    def instantiate_provider(self) -> None:
        self.provider = Provider(self.target, self.edition)
        self.provider.ica = self.ica

    def instantiate_parser(self) -> None:
        parser = Parser(self.target, self.edition)
        parser.reader = Reader()
        parser.ica = self.ica
        parser.reader.ica = self.ica
        self.parser = parser

    def instantiate_normalizer(self) -> None:
        self.normalizer = Normalizer(self.target, self.edition)
        self.normalizer.ica = self.ica

    def instantiate_modeler(self) -> None:
        self.modeler = Modeler(self.target, self.edition)
        self.modeler.ica = self.ica

    def build_path(self, file: str, data: bool = False, edition: bool = True):
        path = self.path
        if data:
            path += "fixtures\\csv\\"
        if self.target:
            path += f"{self.target}\\"
        if self.edition and edition:
            path += f"{self.edition}\\"
        path += file
        return path

    def load_ica(self) -> None:
        path = self.build_path("args.json")
        with open(path) as file:
            self.ica = json.load(file)

    def dispatch_ica(self) -> None:
        self.provider.ica = self.ica
        self.parser.ica = self.ica
        self.normalizer.ica = self.ica
        self.modeler.ica = self.ica
        self.parser.reader.ica = self.ica

    def sica(self, argument: str, category: str = "default") -> str:  # index_url, nav_id, title_tag
        arguments = Ica.get(self.ica, argument, category=category)
        if isinstance(arguments, str):
            return arguments
        return ""

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

    def save_df(self, df: pd.DataFrame, name: str, target: bool = False) -> None:
        if target:
            file = f"{self.target}_index.db"
            db = self.build_path(file, data=True, edition=False)
            success = self.df_to_db(df, name, db=db)
        else:
            success = self.df_to_db(df, name)
        if success:
            print(f"df saved in table {name}")

    def df_to_db(self, df: pd.DataFrame, name: str, db: str | None = None) -> bool:
        if db is None:
            file = f"{self.target}_{self.edition}.db"
            db = self.build_path(file, data=True)
        if db is None:
            return False
        with lite.connect(db) as con:
            try:
                df.to_sql(name=name, con=con, if_exists='append', index=False)
                return True
            except ValueError:
                return False

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
    """
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
    """

    def extract_source_links(self, plate: Plate, debug: bool = False, verbose: bool = False) -> None:
        if plate.soup is None:
            return
        unsorted_links = self.parser.extract_source_links(plate.soup)
        links = self.parser.parse_source_links(unsorted_links, plate.name)
        df = pd.DataFrame.from_records(data=links)
        df["source"] = plate.name
        df["soup"] = "No soup"
        plate.item_links = df
        if verbose or debug:
            print(plate.item_links)

    def parse_item(self, plate: Plate, debug: bool = False, verbose: bool = False) -> None:
        if plate.url is None:
            return
        status = Status(plate.name, plate.url)
        result = Result()
        titles = self.parser.find_titles(plate, debug=debug, verbose=verbose)
        title = self.parser.parse_titles(plate, status, titles, debug=debug, verbose=verbose)
        result.titles.append(title)

        if debug:
            print("result title", *result.titles, status.start is not None)

        if status.start:
            parsed_rows = self.parser.parse_item(plate, status=status, result=result, debug=debug, verbose=verbose)
            if not parsed_rows:
                if debug:
                    print(f"Parsing failed for {title}\n")
            else:
                items = self.parser.complete_plate(plate, parsed_rows)
                plate.data_dict = items
                if items:
                    plate.completed = True
        else:
            print(f"title = {title}, start is {'none' if status.start is None else 'not none'}")

    def cook_url(self, url: str, parser: str = "", keep_alive: bool = False) -> Plate:
        plate = Plate(url=url)
        self.provider.cook(plate, parser=parser, keep_alive=keep_alive)
        self.parser.validate_plate(plate)
        return plate

    def extract_sources(self) -> Plate | None:
        """ Here we skip both the extract_link and parse_item processes.
        The url we target holds a result table in shadow dom.
        the call to cook_url will return the table as soup, but also extracted the links in table.
        We do not care about list data since we will go after detail data for each item.
        The plate will hold a data_dict we build here with the item_links names and urls we got from the provider.
        """
        url = self.sica("index_url")
        plate = self.cook_url(str(url), keep_alive=True)
        link_list = self.parser.extract_links_by_id(plate, str(self.sica("nav_id")), str(self.sica("title_tag")))
        if not link_list:
            return None
        df = pd.DataFrame.from_records(data=link_list)
        df["source"] = plate.name
        plate.item_links = df
        unsorted_list = []
        for source_group in link_list:
            if source_group is not None:
                group_plate = self.cook_url(source_group["url"], keep_alive=True)
                if group_plate.item_links is not None:
                    group_plate.item_links["group"] = source_group["name"]
                    group_plate.item_links["edition"] = "unsorted"
                    unsorted_list.append(group_plate.item_links)
        if unsorted_list:
            data = pd.concat(unsorted_list)
            plate.dfs = {"sources": data}
            data_dict = {"sources": data.to_dict(orient='records')}
            plate.data_dict = {str(key): value for key, value in data_dict.items()}
            plate.completed = True
            return plate
        return None

    def sort_source(self, source: dict[str, str]) -> dict:
        plate = self.cook_url(source["url"], keep_alive=True)
        self.parse_item(plate)
        if plate.data_dict is None:
            return {}
        row = plate.data_dict["sources"][0]
        data = {"row": row, "edition": self.parser.get_edition(row), "soup": plate.soup}
        if data["edition"] == self.edition:
            self.extract_source_links(plate)
            data["df"] = plate.item_links
        return data

    def say_hello(self) -> tuple[str, bool]:
        title = self.provider.say_hello()
        return title, self.provider.cookies is not None

    def normalize_df(self, plate: Plate, category: str, source_name: str) -> None:
        if plate.dfs is None:
            return
        df = plate.dfs[category]
        self.normalizer.norm_df(df, category, source_name=source_name)
        try:
            self.normalizer.__getattribute__(f"norm_{category}_df")(df, category)
        except AttributeError:
            pass

    def fit_category_to_models(self, plate: Plate, category: str, source: str) -> None:
        if plate.dfs is None:
            return
        model_dfs = self.modeler.fit_category_to_models(plate.dfs[category], category)
        if model_dfs:
            directory = self.target + '\\' + self.edition + '\\' + source
            for model in model_dfs.keys():
                self.save(model_dfs[model], name=f"{model}__finalized", directory=directory, app="utils")

    def update_worker(self, worker: str) -> None:
        module = import_module(worker)
        reload(module.__getattribute__(f"Edition{worker.title()}"))
        invalidate_caches()
        self.__getattribute__(f"instantiate_{worker}")()


if __name__ == "__main__":
    with open(f"{BASE_DIR}\\utils\\nethys\\remaster\\args.json") as f:
        ica = json.load(f)
        print(ica)
