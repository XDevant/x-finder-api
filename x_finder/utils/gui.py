#!/usr/bin/env python
# coding: utf-8
import sqlite3
from tkinter import Tk, Label, Listbox, Entry, Button, Scrollbar, VERTICAL, TclError, Event, HORIZONTAL, NS, EW, END
from tkinter.ttk import Menubutton, OptionMenu
import sqlite3 as lite
import os
import pandas as pd
from x_finder.x_finder.settings import BASE_DIR
from typing import Callable, Literal
from selector import Selector
from helpers.connection import Con


class GUI:

    def __init__(self) -> None:
        self.mw = Tk()
        self.mw.title("Soup Kitchen")
        self.target = ""
        self.edition = ""
        self.targets = []
        self.editions = []
        self.current_group = ""
        self.current_source = ""
        self.current_url = ""
        self.current_category = ""
        self.current_item = ""
        self.loaded_sources: list[dict | None] = []
        self.db = None
        self.target_db = None
        self.from_filesystem = False

        self.inbox_lb = Label(self.mw, text='Enter target,edition', bg='lightCyan2')
        self.inbox_entry = Entry(self.mw, width=30)

        self.inbox_lb.grid(row=7, column=14, sticky=EW)
        self.inbox_entry.grid(row=8, column=14, sticky=EW)

        self.new_csv_lb = Label(self.mw, text='name next .csv to be created', bg='lightCyan2')
        self.new_csv_entry = Entry(self.mw, width=30)
        self.new_csv_entry.insert(END, 'my_new_csv')
        self.new_csv_lb.grid(row=16, column=14, sticky=EW)
        self.new_csv_entry.grid(row=17, column=14, sticky=EW)
        self.sql_entry = Entry(self.mw, width=30)
        self.sql_entry.grid(row=29, column=14, sticky=EW)

        self.dropbox = Menubutton(self.mw, )

        self.path = f"{BASE_DIR}\\utils\\fixtures\\csv\\"
        self.inbox_entry.bind('<Return>', self.load_inbox_input)
        self.initialize_labels()
        self.initialize_listboxes()
        self.initialize_buttons()
        self.initialize_vertical_spacing()
        self.__setattr__("scroll_h_query_list",
                         Scrollbar(self.mw, orient=HORIZONTAL, command=self.__getattribute__("query_list").xview))
        self.__getattribute__("query_list")['xscrollcommand'] = self.__getattribute__("scroll_h_query_list").set
        self.__getattribute__("scroll_h_query_list").grid(row=29, column=5, columnspan=7, sticky=EW)

        self.initialize_path()

    def initialize_label(self,
                         name: str,
                         row: int = 0,
                         column: int = 0,
                         text: str = '',
                         bg: str = 'lightCyan2',
                         rowspan: int = 1,
                         columnspan: int = 1,
                         sticky: str = 'EW',
                         width: int | None = None) -> None:
        if name:
            if not text:
                text = f"{name}".capitalize().replace('_lb', '')
            self.__setattr__(name, Label(self.mw, text=text, bg=bg, width=width))
            self.__getattribute__(name).grid(row=row, column=column, sticky=sticky,
                                             rowspan=rowspan, columnspan=columnspan)

    def update_label(self, label: str, text: str) -> None:
        self.__getattribute__(label).config(text=text)

    def initialize_button(self,
                          name: str,
                          command: Callable[[], None] = None,
                          row: int = 0,
                          column: int = 0,
                          text: str = '',
                          bg: str = 'green',
                          default: Literal['normal', 'active', 'disabled'] = "disabled") -> None:
        if name:
            if not text:
                text = name.capitalize().replace('_bt', '')

            self.__setattr__(name, Button(self.mw, text=text, bg=bg, command=command, default=default))
            self.__getattribute__(name).grid(row=row, column=column)

    def update_button(self, button: str,
                      color: str = 'grey',
                      default: Literal['normal', 'active', 'disabled'] = 'disabled'
                      ) -> None:
        self.__getattribute__(button + "_bt").config(bg=color, default=default)

    def initialize_listbox(self,
                           name: str | None,
                           height: int = 4,
                           width: int = 30,
                           selectmode: str = 'single',
                           row: int = 0,
                           column: int = 0,
                           rowspan: int = 10,
                           columnspan: int = 1,
                           scrollbar: bool = True) -> None:
        self.__setattr__(name, Listbox(self.mw, height=height, width=width, selectmode=selectmode))
        if rowspan > 0:
            self.__getattribute__(name).grid(row=row, column=column, rowspan=rowspan, columnspan=columnspan)
        self.__setattr__(f"scroll_v_{name}",
                         Scrollbar(self.mw, orient=VERTICAL, command=self.__getattribute__(name).yview))
        self.__getattribute__(name)['yscrollcommand'] = self.__getattribute__(f"scroll_v_{name}").set
        self.__getattribute__(name).bind('<<ListboxSelect>>', lambda e: self.on_select(e, name))
        if scrollbar:
            self.__getattribute__(f"scroll_v_{name}").grid(row=row,
                                                           column=column+columnspan,
                                                           rowspan=rowspan,
                                                           sticky=NS)

    def initialize_buttons(self) -> None:
        self.initialize_button("clear_main_bt", command=self.clear_message_box, row=30, column=1, text="Clear Box")
        self.initialize_button("load_csv_bt", command=self.load_csv_in_db, row=30, column=3, text="Load csv into db")
        self.initialize_button("export_csv_bt", command=self.export_as_csv, row=30, column=5, text="Save as csv")
        self.initialize_button("send_sql_bt", command=self.sql_input, row=30, column=14, text="Send Request")

    def initialize_labels(self) -> None:
        self.initialize_label("target_lb", row=0, column=1)
        self.initialize_label("edition_lb", row=1, column=1)
        self.initialize_label("url_lb", row=2, column=1, text=" ")
        self.initialize_label("file_lb", row=3, column=1, text="Local Files")
        self.initialize_label("current_source_lb", row=7, column=1, text="No selected source")
        self.initialize_label("current_url_lb", row=8, column=1, text="No selected url")
        self.initialize_label("current_category_lb", row=9, column=1, text="No selected category")
        self.initialize_label("current_item_lb", row=10, column=1, text="No selected item")
        self.initialize_label("spacing_lb", row=11, column=1, text="- - - - - - -")

        self.initialize_label("source_lb", row=0, column=5)
        self.initialize_label("group_lb", row=0, column=3)
        self.initialize_label("category_lb", row=0, column=7, columnspan=2)
        self.initialize_label("table_lb", row=0, column=10, columnspan=2)

        self.initialize_label("query_name_lb", row=14, column=3, text="Item Name")
        self.initialize_label("query_lb", row=14, column=5, text="Item data", columnspan=6)
        self.initialize_label("sql_lb", row=28, column=14, text='SQlite query:')
        Listbox(self.mw).yview_moveto(0.8)

    def initialize_listboxes(self) -> None:
        self.initialize_listbox("file_list", height=4, row=3, column=1, rowspan=3, scrollbar=False)
        self.initialize_listbox("group_list", height=8, width=20, row=1, column=3, rowspan=7)
        self.initialize_listbox("source_name_list", height=13, width=20, row=1, column=5, rowspan=12)
        self.initialize_listbox("source_url_list", height=13, width=15, row=1, column=3, rowspan=0, scrollbar=False)
        self.initialize_listbox("category_list", height=13, width=35, row=1, column=7, rowspan=12, columnspan=2)
        self.initialize_listbox("table_list", height=13, width=35, row=1, column=10, rowspan=12, columnspan=2)

        self.initialize_listbox("query_name_list", height=14, width=15, row=15, column=3, rowspan=14)
        self.initialize_listbox("query_url_list", height=14, width=15, row=15, column=5, rowspan=0, scrollbar=False)
        self.initialize_listbox("query_list", height=14, width=100, row=15, column=5, rowspan=14, columnspan=7)
        self.initialize_listbox("message_box", height=5, row=1, column=14, rowspan=5, scrollbar=False)

    def initialize_vertical_spacing(self) -> None:
        self.initialize_label("space_0", text=' ', row=0, column=0, rowspan=30, sticky='NS')
        self.initialize_label("space_2", text=' ', row=0, column=2, rowspan=30, sticky='NS')
        self.initialize_label("space_8", text=' ', row=0, column=13, rowspan=30, sticky='NS')
        self.initialize_label("space_4", text=' ', row=0, column=4, rowspan=13, sticky='NS')

    def update_current_labels(self) -> None:
        self.update_label("current_url_lb", f"Url: {self.current_url if self.current_url else '-'}")
        self.update_label("current_source_lb", f"Source: {self.current_source if self.current_source else '-'}")
        self.update_label("current_category_lb", f"Category: {self.current_category if self.current_category else '-'}")
        self.update_label("current_item_lb", f"Item: {self.current_item if self.current_item else '-'}")

    def update_current_buttons(self) -> None:
        """Overload in kitchen_gui"""
        Button(default='disabled')

    def check_db(self):
        if not self.target:
            return
        if not self.edition:
            db = self.path + f"{self.target}_index.db"
            self.target_db = db
        else:
            db = self.path + f"{self.target}_{self.edition}.db"
            self.db = db
        con = Con(db)
        tables = con.run("sqlite_master", action="select", columns=["name"])
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
            self.update_label("target_lb", f'Target: {self.target}')
            tables = self.check_db()
        if self.edition:
            self.update_label("edition_lb", f'Edition: {self.edition}')
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
            self.__getattribute__("file_list").delete(0, 'end')
            self.__getattribute__("message_box").insert(END, f'-- Choose your {stage}--')
            for directory in options:
                self.__getattribute__("file_list").insert(END, str(directory))

    def build_path_from_selector(self, stage: str) -> list[str]:
        selector = Selector(self.target, self.edition)
        return selector.__getattribute__(stage + 's')

    def build_path_from_filesystem(self) -> list[str]:
        directories = self.get_directories()
        return directories

    def build_directories(self, options: list[str]) -> None:
        self.__getattribute__("file_list").delete(0, 'end')
        for directory in options:
            self.__getattribute__("file_list").insert(END, str(directory))

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

    def execute_sql(self, sql: str) -> list:
        if self.db:
            with lite.connect(self.db) as con:
                con.row_factory = lite.Row
                cur = con.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
            return rows

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
        conn = Con(db)
        conn.run(name, df=df)
        self.display_tables()
        self.__getattribute__("message_box").insert(END, '--Table Created or Completed --')

    def db_to_df(self,
                 name: str | None = None,
                 source: str | None = None,
                 category: str | None = None,
                 group: str | None = None,
                 db: str | None = None
                 ) -> pd.DataFrame | None:
        if db is None:
            db = self.db
            if not db:
                if name in ["sources", "groups"]:
                    db = self.target_db
            if not db:
                return
        if name is None:
            name = self.current_category
        wheres = []
        values = []
        if source is not None:
            wheres.append("source = ?")
            values.append(source)
        if category is not None:
            wheres.append("category = ?")
            values.append(category)
        if group is not None and name == "sources":  # need join on source to get source.group or fk
            wheres.append("group = ?")
            values.append(group)
        conn = Con(db)
        if not wheres:
            wheres = None
        if not values:
            values = None
        else:
            values = tuple(values)
        headers, rows = conn.run(name, action="select", wheres=wheres, values=values)
        if rows:
            df = pd.DataFrame(rows, columns=headers)
            return df
        return

    def load_csv(self, file_name: str) -> pd.DataFrame:
        pathfile = self.path + file_name
        df = pd.read_csv(pathfile, sep='|')
        return df

    def export_as_csv(self) -> None:
        name = self.path + self.new_csv_entry.get() + '.csv'
        df = self.db_to_df(self.sql_entry.get())
        df.to_csv(name, index=False)
        self.display_csv()

    def on_select(self, event: Event, list_box_name: str) -> None:
        w = event.widget
        try:
            index = int(w.curselection()[0])
        except IndexError:
            return
        try:
            selection = self.__getattribute__(list_box_name).selection_get()
        except AttributeError:
            self.__getattribute__("message_box").insert(END, '-- Source not found! --')
            return
        first_name = list_box_name.split('_')[0]
        if first_name in ["category", "table", "group"]:
            self.__getattribute__(f"select_{first_name}")(selection)
            self.__getattribute__(list_box_name).selection_clear(0, 'end')
        if first_name == "file":
            self.select_edition(selection)
            self.__getattribute__(list_box_name).selection_clear(0, 'end')
        if first_name == "source":
            name = self.__getattribute__(f"{first_name}_name_list").get(index)
            url = self.__getattribute__(f"{first_name}_url_list").get(index)
            self.__getattribute__(f"{first_name}_name_list").selection_clear(0, 'end')
            self.__getattribute__(f"{first_name}_url_list").selection_clear(0, 'end')
            self.select_source(name, url)

        if first_name == "query":
            name = self.__getattribute__(f"{first_name}_name_list").get(index)
            url = self.__getattribute__(f"{first_name}_url_list").get(index)
            self.__getattribute__(f"{first_name}_name_list").selection_clear(0, 'end')
            self.__getattribute__(f"{first_name}_url_list").selection_clear(0, 'end')
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
        self.display_groups()

    def select_source(self, name: str, url: str) -> None:
        try:
            folder = name.lower().replace(' ', '_')
            path_to_folder = f"{self.path}{folder}{'/' if folder else ''}"
            if os.path.isdir(path_to_folder):
                self.current_source = folder
                self.current_url = url
                self.current_category = "sources"
                self.current_item = self.current_source
                self.__getattribute__("source_name_list").selection_clear(0, 'end')
                self.__getattribute__("source_url_list").selection_clear(0, 'end')
                self.display_category()
                self.clear_query_lists()
            else:
                self.__getattribute__("message_box").insert(END, f'-- No file found for {folder} --')
        except FileNotFoundError:
            self.__getattribute__("message_box").insert(END, f'-- No category folder --')

    def select_edition(self, selection) -> None:
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
            source = None
            group = None
            if self.current_source:
                source = self.current_source
            if self.current_group:
                group = self.current_group
            link_df = self.db_to_df(name="links", source=source, category=selection, group=group)
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
        for item in df.iterrows():
            data_dict = item[1]
            name = data_dict["name"]
            url = ""
            if "url" in data_dict.keys():
                url = data_dict["url"]
            elif "nethys_url" in data_dict.keys():
                url = data_dict["nethys_url"]
            data_list = list(data_dict)
            self.__getattribute__("query_name_list").insert(END, name)
            self.__getattribute__("query_url_list").insert(END, url)
            if len(data_list) >= 2:
                cleared_list = [str(row) for row in data_list]
                self.__getattribute__("query_list").insert(END, ' | '.join(cleared_list))

    def use_my_dfs(self, dfs: dict[str, pd.DataFrame]) -> None:
        pass

    def clear_query_lists(self) -> None:
        self.__getattribute__("query_list").delete(0, 'end')
        self.__getattribute__("query_name_list").delete(0, 'end')
        self.__getattribute__("query_url_list").delete(0, 'end')

    def select_table(self, selection) -> None:
        if selection:
            source = None
            category = None
            group = None
            if self.current_category and self.current_category not in ["groups", "sources", selection, "links"]:
                category = self.current_category
            if self.current_source:
                source = self.current_source
            if self.current_group:
                group = self.current_group
            df = self.db_to_df(name=selection, source=source, category=category, group=group)
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
        if not db or db is None:
            self.__getattribute__("message_box").insert(END, '--No db Found!--')
            return
        return db

    def display_groups(self) -> bool:
        db = self.target_db
        if db is None or not db:
            self.__getattribute__("group_list").insert(END, "-- No target db --")
            return False
        self.__getattribute__("group_list").delete(0, 'end')
        conn = Con(db)
        if self.check_if_table_exists("groups", target=True):
            name, groups = conn.run("groups", action="select", columns=["name"])
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
        name, tables = conn.run("sqlite_master",
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
        db = self.db
        if db is None or not db:
            self.__getattribute__("message_box").insert(END, '--No db found--')
            return False
        conn = Con(db)
        try:
            name, category_list = conn.run("links", action="select", columns=["DISTINCT category"])
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
                            self.__getattribute__("source_name_list").insert(END, entry.name)
            except FileNotFoundError:
                self.__getattribute__("message_box").insert(END, '-- No source folder found --')

    def display_sources_from_csv(self) -> None:
        loaded = 0
        missed = 0
        for source in self.loaded_sources:
            try:
                self.__getattribute__("source_name_list").insert(END, source["name"])
                self.__getattribute__("source_url_list").insert(END, source["url"])
                loaded += 1
            except KeyError:
                missed += 1
        self.__getattribute__("message_box").insert(END, f'-- Loaded {loaded}/{loaded + missed} sources --')

    def display_sources_from_db(self) -> bool:
        db = self.db
        if db is None or not db:
            self.__getattribute__("message_box").insert(END, '--No db found--')
            return False
        conn = Con(db)
        try:
            headers, source_list = conn.run("sources", action="select", columns=["name", "nethys_url"])
        except sqlite3.OperationalError:
            self.__getattribute__("message_box").insert(END, '--No sources in db--')
            return False
        if source_list:
            for source in source_list:
                self.__getattribute__("source_name_list").insert(END, source[0])
                self.__getattribute__("source_url_list").insert(END, source[1])
            return True
        self.__getattribute__("message_box").insert(END, '--No source found in db--')
        return False

    def display_sources(self) -> None:
        self.__getattribute__("source_name_list").delete(0, 'end')
        if not self.edition:
            self.__getattribute__("source_name_list").insert(END, "  -- Edition not selected --  ")
        elif self.display_sources_from_db():
            return
        elif self.loaded_sources:
            self.display_sources_from_csv()
        else:
            self.display_sources_from_folder()

    def display_edition(self) -> None:
        if self.edition:
            try:
                self.__getattribute__("file_list").delete(0, 'end')
                with os.scandir(self.path) as it:
                    for entry in it:
                        if '.csv' in entry.name and 'source' in entry.name and entry.is_file():
                            self.__getattribute__("file_list").insert(END, entry.name)
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
