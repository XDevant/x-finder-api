import pandas as pd
from os import makedirs
from pathlib import Path
import json
from args import Ica
from provider import Provider
from parser import Parser
from normalizer import Normalizer
from modeler import Modeler
from nethys.remaster.args import item_category_arguments

BASE_DIR = Path(__file__).resolve().parent.parent

"""Df is not supposed to do much but provide base methods and hooks for each handler that will inherit from Df """


class Plate:
    """
    Class designed to store the data relative to its url until complete parsing.
    Given to the Provider and Parser for completion. Returned to the kitchen.
    Kitchen method dealing with a single plate return it to the GUI while methods iterating lists store the result
    in panda dataframes and csv or db. All forget the plate once exited.
    The GUI only stores the last Plate it received.
    """

    def __init__(self, url=None, title=None, content=None, soup=None):
        self.url = url
        self.title = title
        self.content = content
        self.soup = soup
        self.name = "unknown"
        self.category = "default"
        self.item_links = {}
        self.data_dict = {}
        self.validated = False
        self.completed = False


class Status:
    """
    Keep track of the parsing steps, given as argument to recursive method read_node to keep it sane
    """
    def __init__(self, known_name, known_url):
        self.name = known_name
        self.url = known_url
        self.ended = 0  # number of titles, to fill the right dict
        self.last_key = ""  # we parsed a key and are loading values if truthy,
        self.loaded_values = []  # values can be in many html nodes we stack, waiting for end or key identification
        self.family = False  # we expect nested items of the same category in this page
        self.start = None  # first node after item's title in soup Navigable string


class Result:
    """
    Stores parsing results,  given as argument to and filled by recursive method read_node to keep it sane
    """
    def __init__(self):
        self.parsed = []
        self.titles = []
        self.links = []
        self.tails = []


class Dh:
    """Here we parse the item's data, check for nested item's and turn navigable strings into rows of data
     then we build a panda dataframe for each category of items and normalize them for data tidying
     and make sure our dfs match our database models and extract tables through.
     """
    provider = None
    parser = None
    normalizer = None
    modeler = None

    def __init__(self, target="", edition=""):
        self.target = target
        self.edition = edition
        self.ica = None
        self.instantiate_provider()
        self.instantiate_parser()
        self.instantiate_normalizer()
        self.instantiate_modeler()
        self.dispatch_ica()

    def instantiate_provider(self):
        self.provider = Provider(self.target, self.edition)

    def instantiate_parser(self):
        self.parser = Parser(self.target, self.edition)

    def instantiate_normalizer(self):
        self.normalizer = Normalizer(self.target, self.edition)

    def instantiate_modeler(self):
        self.modeler = Modeler(self.target, self.edition)

    def dispatch_ica(self):
        with open(f"{BASE_DIR}\\utils\\{self.target}\\{self.edition}\\args.json") as file:
            self.ica = json.load(file)
        self.provider.ica = self.ica
        self.parser.ica = self.ica
        self.normalizer.ica = self.ica
        self.modeler.ica = self.ica

    def get(self, argument, category="default", keys=False):
        return Ica.get(self.ica, argument, category, keys)

    @staticmethod
    def save(df, name, directory=None, app="utils"):
        path = f"{BASE_DIR}\\{app}\\fixtures\\csv\\"
        if directory:
            path += f"{directory}\\"
        makedirs(path, exist_ok=True)
        df.to_csv(f"{path}\\{name}.csv", sep='|', index=False)
        print(f"{name} successfully saved at {path}.")

    @staticmethod
    def load(name, app="utils", directory=None, suffix=""):
        if directory:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{directory}\\{name}{'_' if suffix else ''}{suffix}.csv"
        else:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{name}{'_' if suffix else ''}{suffix}.csv"
        df = pd.read_csv(pathfile, delimiter="|")
        return df

    def load_df(self, category, source="", suffix="", app="utils"):
        directory = self.target + "/" + self.edition
        if source:
            directory += "/" + source
        df = self.load(category, app=app, directory=directory, suffix=suffix)
        return df

    def build_dfs(self, dict_of_lists_of_dicts, source_name="Unknown", suffix=""):
        completed_category_dfs = {}
        for key in dict_of_lists_of_dicts.keys():
            list_of_dicts = dict_of_lists_of_dicts[key]
            df = self.build_df(list_of_dicts, source_name=source_name, category=key, suffix=suffix)
            completed_category_dfs[key] = df
        return completed_category_dfs

    def build_df(self, list_of_dicts, source_name="Unknown", category="unknown", suffix=""):
        df = pd.DataFrame.from_records(data=list_of_dicts)
        suffix = f"{category}_completed + {'_' if suffix else ''} + {suffix}"
        directory = self.target + "/" + self.edition + "/" + source_name
        self.save(df, suffix, directory=directory, app="utils")
        return df

    def finalize_completed_dfs(self, dict_of_dfs, source_name):
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
                    self.save(model_df, f"{key}_finalized", directory=source_name, app=app)
                except Exception:
                    print(f"An error occurred while finalizing {key} df")

    def extract_source_links(self, plate):
        unsorted_links = self.parser.extract_source_links(plate.soup)
        link_dict = self.parser.parse_source_links(unsorted_links)
        plate.item_links = link_dict

    def parse_item(self, plate, debug=False, verbose=False):
        plate.validated = self.parser.validate_plate(plate)
        if plate.validated:
            status = Status(plate.name, plate.url)
            result = Result()
            title = self.parser.find_start(plate, status, debug=False, verbose=False)
            result.titles.append(title)
            print("result title", *result.titles, status.start is not None)
            if status.start and title:
                parsed_rows = self.parser.parse_item(plate, status=status, result=result,
                                                     debug=debug, verbose=verbose)
                items = self.parser.complete_plate(plate, parsed_rows)
                plate.data_dict = items
                if items:
                    plate.completed = True
            else:
                print(f"title = {title}, start is {'none' if status.start is None else 'not none'}")

    def cook_url(self, url, parser="", keep_alive=False):
        plate = Plate(url=url)
        print(plate.data_dict)
        self.provider.cook(plate, parser=parser, keep_alive=keep_alive)
        return plate

    def find_nested_item_category(self, name, url, next_child=None, category="default"):
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
