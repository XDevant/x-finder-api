import pandas as pd
from os import makedirs
from pathlib import Path
from args import Ica, item_category_arguments as ica
from provider import Provider
from parser import Parser
from normalizer import Normalizer
from modeler import Modeler

BASE_DIR = Path(__file__).resolve().parent.parent

"""Df is not supposed to do much but provide base methods and hooks for each handler that will inherit from Df """


class Dh:
    """Here we parse the item's data, check for nested item's and turn navigable strings into rows of data
     then we build a panda dataframe for each category of items and normalize them for data tidying
     and make sure our dfs match our database models and extract tables through.
     """
    target = ""
    edition = ""

    def __init__(self, target="", edition=""):
        self.target = target
        self.edition = edition
        self.provider = Provider(target, edition)
        self.parser = Parser(target, edition)
        self.normalizer = Normalizer(target, edition)
        self.modeler = Modeler(target, edition)

    @staticmethod
    def get(argument, category="default", keys=False):
        return Ica.get(ica, argument, category, keys)

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

    def build_dfs(self, dict_of_dicts, source_name="Unknown", suff=""):
        completed_category_dfs = {}
        for key in dict_of_dicts.keys():
            df = pd.DataFrame.from_records(data=dict_of_dicts[key])
            completed_category_dfs[key] = df
            suffix = "completed"
            if suff:
                suffix += "_" + suff
            directory = self.target + "/" + self.edition + "/" + source_name
            self.save(df, f"{key}_{suffix}", directory=directory, app="utils")
        return completed_category_dfs

    def norm_dfs(self, dict_of_dfs, source_name):
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
