# coding: utf-8
import sqlite3
from tkinter import Tk, TclError, END
import os
import pandas as pd
from x_finder.x_finder.settings import BASE_DIR
from selector import Selector
from helpers.connection import Con
from mixins.tkwidget import TkWidgetMixin
from mixins.plate import PlateMixin


class GUI(TkWidgetMixin, PlateMixin):

    def __init__(self) -> None:
        self.mw = Tk()
        self.mw.title("Soup Kitchen")
        self.target: str = ""
        self.edition: str = ""
        self.targets: list[str] = []
        self.editions: list[str] = []
        self.current_group: str = ""
        self.current_source: str = ""
        self.current_url: str = ""
        self.current_category: str = ""
        self.current_item: str = ""
        self.loaded_sources: list[dict | None] = []
        self.db: str | None = None
        self.target_db: str | None = None
        self.from_filesystem: bool = False
        self.source_url_map: dict[str,str] = {}
        self.query_url_map: dict[str,str] = {}

        self.path = f"{BASE_DIR}\\utils\\fixtures\\csv\\"
        self.initialize_widgets()
        self.initialize_path()

    def update_current_labels(self) -> None:
        self.update_label(label="current_url_label", text=f"Url: {self.current_url if self.current_url else '-'}")
        self.update_label(label="current_source_label", text=f"Source: {self.current_source if self.current_source else '-'}")
        self.update_label(label="current_category_label", text=f"Category: {self.current_category if self.current_category else '-'}")
        self.update_label(label="current_item_label", text=f"Item: {self.current_item if self.current_item else '-'}")
        self.update_label(label="current_group_label", text=f"Group: {self.current_group if self.current_group else '-'}")

    def update_current_buttons(self) -> None:
        """Overload in kitchen_gui"""
        self.update_button(button="clear_message_box",color="green", default="normal")
        self.update_button(button="export_as_csv", color="green", default="normal")

    def check_db(self) -> list[str]:
        if not self.target:
            return []
        if not self.edition:
            db = self.path + f"{self.target}_index.db"
            self.target_db = db
        else:
            db = self.path + f"{self.target}_{self.edition}.db"
            self.db = db
        con = Con(db)
        headers, tables = con.run("sqlite_master", action="select", columns=["name"])
        return tables

    def initialize_path(self, target: str | None = None, edition: str | None = None) -> None:
        if target:
            self.target = target
            self.path += target + '\\'
        if not self.target:
            self.build_path("target")

        if edition:
            self.edition = edition
            self.path += edition + '\\'
        if not self.edition:
            self.build_path("edition")
        tables = []
        if self.target:
            self.update_label("title_target_label", f'Target: {self.target}')
            tables = self.check_db()
        if self.edition:
            self.update_label("title_edition_label", f'Edition: {self.edition}')
            tables = self.check_db()
        if tables:
            self.display_tables()
            self.display_category()
            self.display_sources()

    def build_path(self, stage: str) -> None:
        if self.from_filesystem:
            options = self.build_path_from_filesystem()
        else:
            options = self.build_path_from_selector(stage)
        if len(options) == 1:
            name = options[0]
            self.__setattr__(stage, name)
            self.path += name + '\\'
            self.say('-- Found {stage} {name} --')
        elif len(options) == 0:
            self.say('-- No target found, create one --')
        else:
            self.__getattribute__("target_edition_list").delete(0, 'end')
            self.say('-- Choose your {stage}--')
            for directory in options:
                self.__getattribute__("target_edition_list").insert(END, str(directory))

    def build_path_from_selector(self, stage: str) -> list[str]:
        selector = Selector(self.target, self.edition)
        try:
            stages = selector.__getattribute__(stage + 's')
        except AttributeError:
            return []
        return stages

    def build_path_from_filesystem(self) -> list[str]:
        directories = self.get_directories()
        return directories

    def build_directories(self, options: list[str]) -> None:
        self.__getattribute__("target_edition_list").delete(0, 'end')
        for directory in options:
            self.__getattribute__("target_edition_list").insert(END, str(directory))

    def get_directories(self) -> list:
        try:
            with os.scandir(self.path) as it:
                return [entry.name for entry in it if entry.is_dir()]
        except FileNotFoundError:
            return []

    def clear_message_box(self) -> None:
        self.__getattribute__("message_box").delete(0, 'end')

    def clear_query_lists(self) -> None:
        self.__getattribute__("query_list").delete(0, 'end')
        self.__getattribute__("query_name_list").delete(0, 'end')
        self.query_url_map = {}

    def display_csv(self) -> None:
        self.__getattribute__("table_list").delete(0, 'end')
        try:
            with os.scandir(self.path) as it:
                for entry in it:
                    if '.csv' in entry.name and entry.is_file():
                        self.__getattribute__("table_list").insert(END, entry.name)
        except FileNotFoundError:
            with os.scandir() as it:
                for entry in it:
                    if '.csv' in entry.name and entry.is_file():
                        self.__getattribute__("table_list").insert(END, entry.name)

    def display_db(self) -> None:
        self.__getattribute__("source_list").delete(0, 'end')
        try:
            with os.scandir(self.path) as it:
                for entry in it:
                    if '.sqlite' in entry.name and entry.is_file():
                        self.__getattribute__("source_list").insert(END, entry.name)
        except FileNotFoundError:
            with os.scandir() as it:
                for entry in it:
                    if '.sqlite' in entry.name and entry.is_file():
                        self.__getattribute__("source_list").insert(END, entry.name)

    def load_csv_in_db(self) -> None:
        try:
            file = self.__getattribute__("category_list").selection_get()
            df = self.load_csv(file)
            name = file[:-4]
            self.df_to_db(df, name)
        except TclError:
            self.say("--Select a .csv to load--")
        self.display_tables()

    def display_target_groups(self) -> bool:
        db = self.target_db
        if db is None or not db:
            self.__getattribute__("group_list").insert(END, "-- No target db --")
            return False
        self.__getattribute__("group_list").delete(0, 'end')
        conn = Con(db)
        if self.check_if_table_exists(name="groups", target=True):
            name, groups = conn.run(name="groups", action="select", columns=["name"])
            if groups:
                for group in groups:
                    self.__getattribute__("group_list").insert(END, group)
                return True
            self.__getattribute__("group_list").insert(END, "-- No group in db --")
            return False
        self.__getattribute__("group_list").insert(END, "-- No group table in db --")
        return False

    def display_files(self) -> None:
        if self.edition:
            try:
                self.__getattribute__("target_edition_list").delete(0, 'end')
                with os.scandir(self.path) as it:
                    for entry in it:
                        if '.csv' in entry.name and 'source' in entry.name and entry.is_file():
                            self.__getattribute__("target_edition_list").insert(END, entry.name)
                            if entry.name == "sources.csv":
                                self.load_sources()
            except FileNotFoundError:
                self.say('--No source csv nor db found--')

    def update_category(self, name: str) -> None:
        if name != self.current_category:
            if self.current_source != self.current_item:
                self.current_item = ""
                self.current_url = ""
        self.current_category = name

    def get_db(self) -> str | None:
        db = self.db
        if not self.edition:
            db = self.target_db
        if not db or db is None and self.target:
            self.say('--No db Found!--')
            return None
        return db

    def display_df(self, df: pd.DataFrame, clean: bool = True) -> None:
        if clean:
            self.clear_query_lists()
        url_map = {}
        for item in df.iterrows():
            data_dict = item[1]
            name = ""
            if "name" in data_dict.keys():
                name = data_dict["name"]
            url = ""
            if "url" in data_dict.keys():
                url = data_dict["url"]
            elif f"{self.target}_url" in data_dict.keys():
                url = data_dict[f"{self.target}_url"]
            data_list = list(data_dict)
            if name:
                self.__getattribute__("query_name_list").insert(END, name)
                url_map[name] = url
            if len(data_list) > 0:
                cleared_list = [str(data) for data in data_list]
                self.__getattribute__("query_list").insert(END, ' | '.join(cleared_list))
        self.query_url_map = url_map

    def display_list(self, data_list: list[str],
                     listbox: str,
                     ph: str = "",
                     succes: str = "",
                     failure: str = "") -> bool:
        try:
            box = self.__getattribute__(f"{listbox}_list")
        except AttributeError:
            self.say(f"Listbox {listbox} not found")
            return False
        box.delete(0, 'end')
        if data_list:
            for data in data_list:
                box.insert(END, data)
            if succes:
                self.say(succes)
            return True
        else:
            if ph:
                box.insert(END, ph)
            if failure:
                self.say(failure)
            return False

    def display_groups(self) -> bool:
        db = self.db
        if db is None or not db:
            return self.display_list([], "group", ph="-- No db --")
        conn = Con(db)
        if self.check_if_table_exists(name="sources"):
            name, groups = conn.run(name="sources", action="select", columns=["DISTINCT [group]"])
            return self.display_list(groups, "group", ph="-- No group in db --")
        return self.display_list([], "group", ph="-- No group table in db --")

    def display_tables(self) -> bool:
        db = self.get_db()
        if db is None:
            return self.display_list([], "table", ph="-- No db --")
        conn = Con(db)
        name, tables = conn.run(name="sqlite_master",
                                action="select",
                                columns=["name"],
                                wheres=["name NOT LIKE ?"],
                                values=('sqlite_auto%', ))
        return self.display_list(tables, "table", ph="-- No table in db --")

    def display_category(self) -> None:
        self.__getattribute__("category_list").delete(0, 'end')
        chk = self.display_category_from_db()
        if not chk:
            chk = self.display_category_from_csv()
        if not chk:
            self.__getattribute__("category_list").insert(END, "No Category extracted yet")

    def display_category_from_db(self) -> bool:
        try:
            category_list = self.get_category_list(self.current_source, self.current_group)
        except sqlite3.OperationalError:
            self.say('--No link found in db--')
            return False
        return self.display_list(category_list, "category", ph="-- No category found in db--")

    def display_category_from_csv(self) -> bool:
        folder = self.current_source
        if folder and not self.path.endswith('csv/'):
            path_to_file = f"{self.path}{folder}/"
            try:
                with os.scandir(path_to_file) as it:
                    for entry in it:
                        self.__getattribute__("category_list").insert(END, entry.name)
                return True
            except FileNotFoundError:
                self.say('--No category csv found--')
        return False

    def display_sources(self) -> None:
        self.__getattribute__("source_list").delete(0, 'end')
        if not self.edition:
            self.__getattribute__("source_list").insert(END, "  -- Edition not selected --  ")
            return
        if self.display_sources_from_db():
            return
        if self.loaded_sources:
            self.display_sources_from_csv()
        else:
            self.display_sources_from_folder()

    def display_sources_from_folder(self) -> None:
        if not self.path.endswith('csv/'):
            try:
                with os.scandir(self.path) as it:
                    for entry in it:
                        if os.path.isdir(entry):
                            self.__getattribute__("source_list").insert(END, entry.name)
            except FileNotFoundError:
                self.say('-- No source folder found --')

    def display_sources_from_csv(self) -> None:
        loaded = 0
        missed = 0
        url_map = {}
        for source in self.loaded_sources:
            if source is not None:
                name = source["name"]
                self.__getattribute__("source_list").insert(END, name)
                url_map[name] = source["url"]
                loaded += 1
            else:
                missed += 1
        self.source_url_map = url_map
        self.say(f'-- Loaded {loaded}/{loaded + missed} sources --')

    def display_sources_from_db(self) -> bool:
        db = self.db
        if db is None or not db:
            self.say('--No db found--')
            return False
        wheres = None
        values = None
        if self.current_group:
            wheres = ["[group] = ?"]
            values = (self.current_group, )
        conn = Con(db)
        try:
            url = f"{self.target}_url"
            headers, source_list = conn.run(name="sources",
                                            action="select",
                                            columns=["name", url],
                                            wheres=wheres,
                                            values=values)
        except sqlite3.OperationalError:
            self.say('--No sources in db--')
            return True
        if source_list:
            url_map = {}
            for source in source_list:
                name = source[0]
                url = source[1]
                self.__getattribute__("source_list").insert(END, name)
                url_map[name] = url
            self.source_url_map = url_map
            return True
        self.say('--No source found in db--')
        return False

    def display_edition(self) -> None:
        if self.editions:
            self.display_list(self.editions, "target_edition")
        elif self.targets:
            self.display_list(self.targets, "target_edition")

    def load_sources(self) -> None:
        source_list = []
        keys = []
        lines = self.read_csv_by_line(f"{self.path}/sources.csv")
        for line in lines:
            if not keys:
                keys = line.strip().split("|")
            else:
                source_data = line.strip().split("|")
                zipped = zip(keys, source_data)
                source_dict = {key: value for key, value in zipped}
                source_list.append(source_dict)
        self.loaded_sources = source_list

    def run(self) -> None:
        self.display_edition()
        self.display_sources()
        self.display_category()
        if self.db or self.target_db:
            self.display_tables()
            self.display_groups()
        self.update_current_buttons()
        self.mw.mainloop()


if __name__ == "__main__":
    gui = GUI()
    gui.run()
