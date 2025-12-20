#!/usr/bin/env python
# coding: utf-8

from tkinter import Tk, Label, Listbox, Entry, NS, EW, END, Button, Scrollbar, VERTICAL, TclError, Event, HORIZONTAL
import sqlite3 as lite
import os
import pandas as pd
from x_finder.x_finder.settings import BASE_DIR
from typing import Callable, Literal


class GUI:

    def __init__(self) -> None:
        self.mw = Tk()
        self.mw.title("Soup Kitchen")
        self.target = ""
        self.edition = ""
        self.current_source = ""
        self.current_url = ""
        self.current_category = ""
        self.current_item = ""
        self.loaded_sources: list[dict | None] = []
        self.db = None

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

        self.path = f"{BASE_DIR}\\utils\\fixtures\\csv\\"
        print(self.path)
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
        self.initialize_button("load_csv_bt", command=self.load_csv_in_db(), row=30, column=3, text="Load csv into db")
        self.initialize_button("export_csv_bt", command=self.export_as_csv(), row=30, column=5, text="Save as csv")
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

    def initialize_listboxes(self) -> None:
        self.initialize_listbox("file_list", height=4, row=3, column=1, rowspan=3, scrollbar=False)
        self.initialize_listbox("source_list", height=13, width=20, row=1, column=5, rowspan=12)
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
        if self.target:
            self.update_label("target_lb", f'Target: {self.target}')
        if self.edition:
            self.update_label("edition_lb", f'Edition: {self.edition}')

    def build_path(self, stage: str) -> None:
        csv_directories = self.get_directories()
        if len(csv_directories) == 1:
            name = csv_directories[0]
            self.__setattr__(stage, name)
            self.path += name + '\\'
            self.__getattribute__("message_box").insert(END, f'-- Found {stage} {name} --')
        elif len(csv_directories) == 0:
            self.__getattribute__("message_box").insert(END, '-- No target found, create one --')
        else:
            self.display_edition()

    def build_directories(self) -> None:
        csv_directories = self.get_directories()
        self.__getattribute__("file_list").delete(0, 'end')
        for directory in csv_directories:
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
            con = lite.connect(self.db)
            con.row_factory = lite.Row
            with con:
                cur = con.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
            if str(sql).startswith("select"):
                headers = list(rows[0].keys())
                self.__getattribute__("query_list").insert(END, headers)
                for row in rows:
                    drow = list(row)
                    self.__getattribute__("query_list").insert(END, drow)
            self.display_table()
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

    def display_table(self) -> None:
        if self.db:
            con = lite.connect(self.db)
            with con:
                cur = con.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
                rows = cur.fetchall()
            self.__getattribute__("table_list").delete(0, 'end')
            if len(rows) == 0:
                self.__getattribute__("message_box").insert(END,
                                                            '--No Table Found.Create Table, Import csv or Load DB--')
            else:
                for row in rows:
                    self.__getattribute__("table_list").insert(END, row)

    def load_csv_in_db(self) -> None:
        try:
            file = self.__getattribute__("category_list").selection_get()
            df = self.load_csv(file)
            name = file[:-4]
            con = lite.connect(self.db)
            with con:
                try:
                    df.to_sql(name=name, con=con, if_exists='fail')
                except ValueError:
                    self.__getattribute__("message_box").insert(END, '--Table already in current DB--')
        except TclError:
            self.__getattribute__("message_box").insert(END, '--Select a .csv to load--')
        self.display_table()

    def load_csv(self, file_name: str) -> pd.DataFrame:
        pathfile = self.path + file_name
        print(pathfile)
        df = pd.read_csv(pathfile, sep='|')
        return df

    def export_as_csv(self) -> None:
        if not self.db:
            return
        con = lite.connect(self.db)
        con.row_factory = lite.Row
        with con:
            cur = con.cursor()
            cur.execute(self.sql_entry.get())
            rows = cur.fetchall()
        name = self.path + self.new_csv_entry.get() + '.csv'
        headers = list(rows[0].keys())
        data = [list(row) for row in rows]
        df = pd.DataFrame(data, columns=headers)
        df.to_csv(name, index=False)
        self.display_csv()

    def on_select(self, event: Event, list_box_name: str) -> None:
        w = event.widget
        try:
            index = int(w.curselection()[0])
        except IndexError:
            return
        if list_box_name == "file_list":
            self.select_edition()
        if list_box_name in ["source_list", "source_url_list"]:
            name = self.__getattribute__("source_list").get(index)
            url = self.__getattribute__("source_url_list").get(index)
            self.select_source(name, url)
        if list_box_name == "category_list":
            self.select_category()
        if list_box_name in ["query_url_list", "query_name_list"]:
            name = self.__getattribute__("query_name_list").get(index)
            url = self.__getattribute__("query_url_list").get(index)
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
                self.__getattribute__("source_url_list").selection_clear(0, 'end')
                self.display_category()
                self.clear_query_lists()
            else:
                self.__getattribute__("message_box").insert(END, f'-- No file found for {folder} --')
        except FileNotFoundError:
            self.__getattribute__("message_box").insert(END, f'-- No category folder --')

    def select_edition(self) -> None:
        selection = self.__getattribute__("file_list").selection_get()
        if self.edition or not selection:
            pass
        elif not self.target:
            self.initialize_path(target=selection)
        else:
            self.initialize_path(edition=selection)
        self.__getattribute__("file_list").selection_clear(0, 'end')
        self.display_edition()
        self.display_sources()

    def select_category(self) -> None:
        try:
            file = self.__getattribute__("category_list").selection_get()
            names = file.split('.')[0].split('__')
            name = names[0]
            status = names[-1].split('_')[0]
            pathfile = self.path + self.current_source + '/' + file
            df = pd.read_csv(pathfile, sep='|')
            self.clear_query_lists()
            for item in df.iterrows():
                item_name = item[1]["name"]
                item_url = item[1]["url"]
                row_list = list(item[1])
                self.__getattribute__("query_name_list").insert(END, item_name)
                self.__getattribute__("query_url_list").insert(END, item_url)
                if len(row_list) >= 2:
                    cleared_list = [str(row) for row in row_list]
                    self.__getattribute__("query_list").insert(END, ' | '.join(cleared_list))
            if name != self.current_category:
                if self.current_source != self.current_item:
                    self.current_item = ""
                    self.current_url = ""
            self.current_category = name
            self.use_my_df(df)
            if status == "completed":
                self.use_my_completed_df(df)
            if status == "normed":
                self.use_my_normed_df(df)
        except FileNotFoundError:
            self.__getattribute__("message_box").insert(END, '-- No category extracted for that source --')
        except AttributeError:
            self.__getattribute__("message_box").insert(END, '-- Source not found! --')
        self.__getattribute__("category_list").selection_clear(0, 'end')

    def use_my_df(self, df: pd.DataFrame) -> None:
        pass

    def use_my_completed_df(self, df: pd.DataFrame) -> None:
        pass

    def use_my_normed_df(self, df: pd.DataFrame) -> None:
        pass

    def clear_query_lists(self) -> None:
        self.__getattribute__("query_list").delete(0, 'end')
        self.__getattribute__("query_name_list").delete(0, 'end')
        self.__getattribute__("query_url_list").delete(0, 'end')

    def select_table(self) -> None:
        try:
            table = self.__getattribute__("table_list").selection_get()
            if table:
                pass
        except TclError:
            self.__getattribute__("message_box").insert(END, '-- Selection failed --')
        self.__getattribute__("table_list").selection_clear(0, 'end')
        self.display_table()

    def display_category(self) -> None:
        self.__getattribute__("category_list").delete(0, 'end')
        folder = self.current_source
        if folder and not self.path.endswith('csv/'):
            path_to_file = f"{self.path}{folder}/"
            try:
                with os.scandir(path_to_file) as it:
                    for entry in it:
                        self.__getattribute__("category_list").insert(END, entry.name)
            except FileNotFoundError:
                self.__getattribute__("message_box").insert(END, '--No category csv found--')

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
            try:
                self.__getattribute__("source_list").insert(END, source["name"])
                self.__getattribute__("source_url_list").insert(END, source["url"])
                loaded += 1
            except KeyError:
                missed += 1
        self.__getattribute__("message_box").insert(END, f'-- Loaded {loaded}/{loaded + missed} sources --')

    def display_sources(self) -> None:
        self.__getattribute__("source_list").delete(0, 'end')
        if not self.edition:
            pass
        elif self.loaded_sources:
            self.display_sources_from_csv()
        else:
            self.display_sources_from_folder()

    def display_edition(self) -> None:
        if not self.edition:
            self.build_directories()
        else:
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
        self.update_current_buttons()
        self.mw.mainloop()


if __name__ == "__main__":
    gui = GUI()
    gui.run()
