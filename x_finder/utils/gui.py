# coding: utf-8
import sqlite3
from tkinter import Tk, Entry, Scrollbar, TclError, Event, EW, END
from tkinter.ttk import Menubutton, OptionMenu
import sqlite3 as lite
import os
import pandas as pd
from x_finder.x_finder.settings import BASE_DIR
from selector import Selector
from helpers.connection import Con
from mixins.tkwidget import TkWidgetMixin


class GUI(TkWidgetMixin):

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

        self.new_csv_entry = Entry(self.mw, width=30)
        self.new_csv_entry.insert(END, 'my_new_csv')

        self.dropbox = Menubutton(self.mw, )

        self.path = f"{BASE_DIR}\\utils\\fixtures\\csv\\"
        self.initialize_widgets()
        self.initialize_path()
        self.sql_entry = Entry(self.__getattribute__("input_frame"), width=100)
        self.sql_entry.grid(row=1, column=1, sticky=EW)
        self.inbox_entry = Entry(self.__getattribute__("input_frame"), width=60)
        self.inbox_entry.grid(row=0, column=1, sticky=EW)
        self.inbox_entry.bind('<Return>', self.load_inbox_input)

    def initialize_frames(self) -> None:
        frame_widget = {"bg": 'lightCyan2', "width": 40, "relief": "raised"}

    def initialize_listboxes(self) -> None:
        self.__setattr__("scroll_h_query_list",
                         Scrollbar(self.__getattribute__("query_frame"),
                                   orient="horizontal",
                                   command=self.__getattribute__("query_list").xview))
        self.__getattribute__("query_list")['xscrollcommand'] = self.__getattribute__("scroll_h_query_list").set
        self.__getattribute__("scroll_h_query_list").grid(row=15, column=2, columnspan=7, sticky=EW)


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
        print(tables)
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
            self.__getattribute__("message_box").insert(END, f'-- Found {stage} {name} --')
        elif len(options) == 0:
            self.__getattribute__("message_box").insert(END, '-- No target found, create one --')
        else:
            self.__getattribute__("target_edition_list").delete(0, 'end')
            self.__getattribute__("message_box").insert(END, f'-- Choose your {stage}--')
            for directory in options:
                self.__getattribute__("target_edition_list").insert(END, str(directory))

    def build_path_from_selector(self, stage: str) -> list[str]:
        selector = Selector(self.target, self.edition)
        return selector.__getattribute__(stage + 's')

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

    def sql_input(self) -> None:
        try:
            sql = self.sql_entry.get()
            with lite.connect(self.db) as con:
                con.row_factory = lite.Row
                cur = con.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
            if str(sql).startswith("select"):
                headers = list(rows[0].keys())
                self.__getattribute__("query_list").insert(END, headers)
                for row in rows:
                    drow = list(row)
                    self.__getattribute__("query_list").insert(END, drow)
            self.display_tables()
        except TclError:
            self.__getattribute__("message_box").insert(END, 'TclError: check current DB')

    def clear_message_box(self) -> None:
        self.__getattribute__("message_box").delete(0, 'end')

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
            self.__getattribute__("message_box").insert(END, '--Select a .csv to load--')
        self.display_tables()

    def df_to_db(self, df: pd.DataFrame, name: str, db: str | None = None) -> None:
        if db is None:
            db = self.db
        if db is None:
            return
        conn = Con(db)
        conn.run(name, df=df)
        self.display_tables()
        self.__getattribute__("message_box").insert(END, '--Table Created or Completed --')

    def db_to_df(self,
                 name: str | None = None,
                 columns: list[str] | None = None,
                 filters: dict[str,str | None] | None = None,
                 db: str | None = None
                 ) -> pd.DataFrame | None:
        if db is None:
            db = self.db
            if not db:
                if name in ["sources", "groups"]:
                    db = self.target_db
            if not db:
                return None
        if name is None:
            name = self.current_category
        if name is None:
            return None
        wheres = []
        values = []
        if filters is not None:
            for key, value in filters.items():
                column = key
                if value is not None:
                    if key == "group":
                        if name != "sources":
                            continue
                        column = f"sources.[{key}]"
                    wheres.append(f"{column} = ?")
                    values.append(value)
        if not wheres:
            wheres = None
        if not values:
            values = None
        else:
            values = tuple(values)
        conn = Con(db)
        headers, rows = conn.run(name, action="select", columns=columns, wheres=wheres, values=values)
        if rows:
            df = pd.DataFrame(rows, columns=headers)
            return df
        return None

    def get_category_df(self,
                        item: str | None = None,
                        source: str | None = None,
                        category: str | None = None,
                        group: str | None = None,
                        status: str | None = None) -> pd.DataFrame | None:
        db = self.db
        name = category
        if status == "modeled":
            name = f"{name}__{status}"
        df = self.db_to_df(name=name,
                           filters={"item": item, "source": source, "group": group},
                           db=db)
        return df

    def get_category_list(self,
                          source: str | None = None,
                          group: str | None = None,
                          status: str | None = None) -> list[str]:
        db = self.db
        filters = {}
        if source is not None and source:
            filters["source"] = source
        if group is not None and group:
            filters["group"] = group
        df = self.db_to_df(name="links",
                           columns=["DISTINCT category"],
                           filters=filters,
                           db=db)
        if df is not None:
            return df["category"].tolist()
        return []

    def get_table_columns(self, name: str) -> list[str]:
        db = self.db
        conn = Con(db)
        columns = conn.execute_sql(f"PRAGMA table_info({name});")
        return columns

    def load_csv(self, file_name: str) -> pd.DataFrame:
        pathfile = self.path + file_name
        df = pd.read_csv(pathfile, sep='|')
        return df

    def export_as_csv(self) -> None:
        name = self.path + self.new_csv_entry.get() + '.csv'
        df = self.db_to_df(self.sql_entry.get())
        if df is not None:
            df.to_csv(name, index=False)
        self.display_csv()

    def on_select(self, list_box_name: str, curselection: list[int]) -> None:
        super(GUI, self).on_select(list_box_name, curselection)
        if len(curselection) == 0:
            return
        index = curselection[0]
        box = self.__getattribute__(list_box_name)
        try:
            selection = box.selection_get()
        except TclError:
            return
        first_name = "_".join(list_box_name.split('_')[:-1])
        if first_name in ["category", "table", "group", "target_edition"]:
            self.__getattribute__(f"select_{first_name}")(selection)
            self.__getattribute__(list_box_name).selection_clear(0, 'end')
        if first_name == "source":
            url = self.source_url_map[selection]
            self.__getattribute__(f"{first_name}_list").selection_clear(0, 'end')
            self.select_source(selection, url)

        if first_name == "query":
            name = self.__getattribute__(f"{first_name}_name_list").get(index)
            url = self.query_url_map[name]
            self.__getattribute__(f"{first_name}_name_list").selection_clear(0, 'end')
            self.current_url = url
            self.current_item = name
        self.update_current_labels()
        self.update_current_buttons()

    @staticmethod
    def read_csv_by_line(path_name) -> list[str]:
        if path_name.endswith('.csv'):
            with open(path_name, 'r') as file:
                lines = file.readlines()
                return lines
        return [""]

    def load_inbox_input(self, event: Event) -> None:
        new_input = self.inbox_entry.get()
        inputs = new_input.split(' ')
        if len(inputs) == 0:
            self.__getattribute__("message_box").insert(END, f'-- No Input command --')
            return
        command = inputs[0]
        arg_list = []
        kwarg_dict = {}
        try:
            self.__getattribute__(command)(*arg_list, **kwarg_dict)
        except AttributeError:
            self.__getattribute__("message_box").insert(END, f'-- Wrong Input command --')

    def select_group(self, selection: str):
        self.current_group = selection
        print(selection)
        self.display_sources()

    def select_source(self, name: str, url: str) -> None:
        try:
            folder = name.lower().replace(' ', '_')
            path_to_folder = f"{self.path}{folder}{'/' if folder else ''}"
            if os.path.isdir(path_to_folder):
                self.current_source = folder
                self.current_url = url
                self.current_category = "sources"
                self.current_item = self.current_source
                self.__getattribute__("source_list").selection_clear(0, 'end')
                self.display_category()
                self.clear_query_lists()
            else:
                self.__getattribute__("message_box").insert(END, f'-- No file found for {folder} --')
        except FileNotFoundError:
            self.__getattribute__("message_box").insert(END, f'-- No category folder --')

    def select_target_edition(self, selection) -> None:
        if not selection or self.edition:
            pass
        elif not self.target:
            self.initialize_path(target=selection)
        else:
            self.initialize_path(edition=selection)
        self.display_edition()
        self.display_sources()
        if self.edition:
            self.display_tables()
            self.display_groups()

    def select_category(self, selection: str) -> None:
        if selection.endswith(".csv"):
            self.select_category_from_csv(selection)
        else:
            self.select_category_from_db(selection)

    def select_category_from_db(self, selection: str) -> None:
        if selection:
            source = ""
            group = ""
            if self.current_source:
                source = self.current_source
            if self.current_group:
                group = self.current_group
            filters = {"category": str(selection)}
            if source:
                filters["source"] = source
            if group:
                filters["group"] = group
            link_df = self.db_to_df(name="links",
                                    filters=filters)
            if link_df is not None:
                self.display_df(link_df)
                self.update_category(selection)
                self.use_my_dfs({"links": link_df})
            if self.check_if_table_exists(selection):
                print("ok")
            else:
                print("ko")

    def check_if_table_exists(self, name: str, target: bool = False, lazy: bool = True) -> bool:
        if target:
            conn = Con(self.target_db)
        else:
            conn = Con(self.db)
        where = "name = ?"
        if lazy:
            where = "name LIKE ?"
            name += "%"
        headers, tables = conn.run("sqlite_master", action="select", columns=["name"], wheres=[where], values=(name, ))
        if tables:
            return True
        return False

    def select_category_from_csv(self, selection: str) -> None:
        names = selection.split('.')[0].split('__')
        name = names[0]
        status = names[-1].split('_')[0]
        pathfile = self.path + self.current_source + '/' + selection
        try:
            df = pd.read_csv(pathfile, sep='|')
        except FileNotFoundError:
            self.__getattribute__("message_box").insert(END, '-- No category extracted for that source --')
        else:
            self.display_df(df)
            self.update_category(name)
            self.use_my_dfs({status: df})
        finally:
            self.__getattribute__("category_list").selection_clear(0, 'end')

    def update_category(self, name: str) -> None:
        if name != self.current_category:
            if self.current_source != self.current_item:
                self.current_item = ""
                self.current_url = ""
        self.current_category = name

    def display_df(self, df: pd.DataFrame) -> None:
        self.clear_query_lists()
        url_map = {}
        for item in df.iterrows():
            data_dict = item[1]
            name = data_dict["name"]
            url = ""
            if "url" in data_dict.keys():
                url = data_dict["url"]
            elif f"{self.target}_url" in data_dict.keys():
                url = data_dict[f"{self.target}_url"]
            data_list = list(data_dict)
            self.__getattribute__("query_name_list").insert(END, name)
            url_map[name] = url
            if len(data_list) >= 2:
                cleared_list = [str(row) for row in data_list]
                self.__getattribute__("query_list").insert(END, ' | '.join(cleared_list))
        self.query_url_map = url_map

    def use_my_dfs(self, dfs: dict[str, pd.DataFrame]) -> None:
        pass

    def clear_query_lists(self) -> None:
        self.__getattribute__("query_list").delete(0, 'end')
        self.__getattribute__("query_name_list").delete(0, 'end')
        # self.__getattribute__("query_url_list").delete(0, 'end')

    def select_table(self, selection) -> None:
        if selection:
            source = None
            category = None
            group = None
            if self.current_category and selection not in ["groups", "sources"]:
                category = self.current_category
            if self.current_source and selection != "sources":
                source = self.current_source
            if self.current_group:
                group = self.current_group
            df = self.db_to_df(name=selection,
                               filters={"source": source, "category": category, "group": group})
            if df is not None:
                self.display_df(df)
                status = "normalized"
                if selection == "links":
                    status = "links"
                if selection != "links" or not self.current_category:
                    self.update_category(selection)
                self.use_my_dfs({status: df})
            else:
                self.__getattribute__("message_box").insert(END, '-- No data Found --')

    def get_db(self) -> str | None:
        db = self.db
        if not self.edition:
            db = self.target_db
        if not db or db is None and self.target:
            self.__getattribute__("message_box").insert(END, '--No db Found!--')
            return None
        return db

    def display_groups(self) -> bool:
        db = self.db
        if db is None or not db:
            self.__getattribute__("group_list").insert(END, "-- No db --")
            return False
        self.__getattribute__("group_list").delete(0, 'end')
        conn = Con(db)
        if self.check_if_table_exists(name="sources"):
            name, groups = conn.run(name="sources", action="select", columns=["DISTINCT [group]"])
            if groups:
                for group in groups:
                    self.__getattribute__("group_list").insert(END, group)
                return True
            self.__getattribute__("group_list").insert(END, "-- No group in db --")
            return False
        self.__getattribute__("group_list").insert(END, "-- No group table in db --")
        return False

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

    def display_tables(self) -> None:
        db = self.get_db()
        if db is None:
            return
        conn = Con(db)
        name, tables = conn.run(name="sqlite_master",
                                action="select",
                                columns=["name"],
                                wheres=["name NOT LIKE ?"],
                                values=('sqlite_auto%', ))
        self.__getattribute__("table_list").delete(0, 'end')
        if len(tables) == 0:
            self.__getattribute__("message_box").insert(END, '--No Table Found.Create Table, Import csv or Load DB--')
        else:
            for table in tables:
                self.__getattribute__("table_list").insert(END, table)

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
            self.__getattribute__("message_box").insert(END, '--No link found in db--')
            return False
        if category_list:
            for category in category_list:
                self.__getattribute__("category_list").insert(END, category)
            return True
        self.__getattribute__("message_box").insert(END, '--No category found in db--')
        return False

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
                self.__getattribute__("message_box").insert(END, '--No category csv found--')
        return False

    def display_sources_from_folder(self) -> None:
        if not self.path.endswith('csv/'):
            try:
                with os.scandir(self.path) as it:
                    for entry in it:
                        if os.path.isdir(entry):
                            self.__getattribute__("source_list").insert(END, entry.name)
            except FileNotFoundError:
                self.__getattribute__("message_box").insert(END, '-- No source folder found --')

    def display_sources_from_csv(self) -> None:
        loaded = 0
        missed = 0
        for source in self.loaded_sources:
            if source is not None:
                self.__getattribute__("source_list").insert(END, source["name"])
                self.__getattribute__("source_url_list").insert(END, source["url"])
                loaded += 1
            else:
                missed += 1
        self.__getattribute__("message_box").insert(END, f'-- Loaded {loaded}/{loaded + missed} sources --')

    def display_sources_from_db(self) -> bool:
        db = self.db
        if db is None or not db:
            self.__getattribute__("message_box").insert(END, '--No db found--')
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
            self.__getattribute__("message_box").insert(END, '--No sources in db--')
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
        self.__getattribute__("message_box").insert(END, '--No source found in db--')
        return False

    def display_sources(self) -> None:
        self.__getattribute__("source_list").delete(0, 'end')
        if not self.edition:
            self.__getattribute__("source_list").insert(END, "  -- Edition not selected --  ")
        elif self.display_sources_from_db():
            return
        elif self.loaded_sources:
            self.display_sources_from_csv()
        else:
            self.display_sources_from_folder()

    def display_edition(self) -> None:
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
                self.__getattribute__("message_box").insert(END, '--No source csv nor db found--')

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
