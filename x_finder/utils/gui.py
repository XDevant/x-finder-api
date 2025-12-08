#!/usr/bin/env python
# coding: utf-8

from tkinter import *
import sqlite3 as lite
import os
import pandas as pd

test_name = 'test.csv'


class GUI:
    mw = Tk()
    mw.title("Soup Kitchen")
    target_edition_lb = Label(mw, text='Enter target,edition', bg='lightCyan2')
    target_edition_entry = Entry(mw, width=30)

    base_path = "fixtures/csv/"
    path = ""
    current_source = ""
    current_url = ""
    current_category = ""
    db = None

    target_edition_lb.grid(row=20, column=1, sticky=EW)
    target_edition_entry.grid(row=21, column=1, sticky=EW)

    new_csv_lb = Label(mw, text='name next .csv to be created', bg='lightCyan2')
    new_csv_entry = Entry(mw, width=30)
    new_csv_entry.insert(END, 'new_csv')
    new_csv_lb.grid(row=16, column=12, sticky=EW)
    new_csv_entry.grid(row=17, column=12, sticky=EW)
    sql_entry = Entry(mw, width=30)
    sql_entry.grid(row=29, column=12, sticky=EW)

    def __init__(self):
        self.target_edition_entry.bind('<Return>', self.load_target_input)
        self.initialize_labels()
        self.initialize_buttons()
        self.initialize_listboxes()
        self.initialize_vertical_spacing()
        name = "scroll_v_source_list"
        self.__getattribute__("source_url_list")['yscrollcommand'] = self.__getattribute__(name).set

    def initialize_label(self,
                         name,
                         row=0,
                         column=0,
                         text='',
                         bg='lightCyan2',
                         rowspan=1,
                         sticky='EW',
                         width=None):
        if name:
            if not text:
                text = f"{name}".capitalize().replace('_lb', '')
            self.__setattr__(name, Label(self.mw, text=text, bg=bg, width=width))
            self.__getattribute__(name).grid(row=row, column=column, sticky=sticky, rowspan=rowspan)

    def update_label(self, label, text):
        self.__getattribute__(label).config(text=text)

    def initialize_button(self,
                          name,
                          command='',
                          row=0,
                          column=0,
                          text='',
                          bg='khaki2'):
        if name:
            if not text:
                text = name.capitalize().replace('_bt', '')
            if not command:
                command = name.lower().replace('_bt', '')
            self.__setattr__(name, Button(self.mw, text=text, bg=bg, command=command))
            self.__getattribute__(name).grid(row=row, column=column)

    def initialize_listbox(self,
                           name,
                           height=4,
                           width=30,
                           selectmode='single',
                           row=0,
                           column=0,
                           rowspan=10,
                           columnspan=1,
                           scrollbar=True):
        self.__setattr__(name, Listbox(self.mw, height=height, width=width, selectmode=selectmode))
        self.__getattribute__(name).grid(row=row, column=column, rowspan=rowspan, columnspan=columnspan)
        if scrollbar:
            self.__setattr__(f"scroll_v_{name}",
                             Scrollbar(self.mw, orient=VERTICAL, command=self.__getattribute__(name).yview))
            self.__getattribute__(name)['yscrollcommand'] = self.__getattribute__(f"scroll_v_{name}").set
            self.__getattribute__(name).bind('<<ListboxSelect>>',
                                             lambda e: self.on_select(e, self.__getattribute__(name)))
            self.__getattribute__(f"scroll_v_{name}").grid(row=row,
                                                           column=column+columnspan,
                                                           rowspan=rowspan,
                                                           sticky=NS)

    def initialize_buttons(self):
        self.initialize_button("clear_main_bt", command="clear_main", row=30, column=3, text="Clear Box")
        self.initialize_button("load_csv_bt", command="load_csv", row=30, column=5, text="Load csv into db")
        self.initialize_button("export_csv_bt", command="export_csv", row=30, column=7, text="Save as csv")
        self.initialize_button("send_sql_bt", command="text_input", row=30, column=12, text="Send Request")

    def initialize_labels(self):
        self.initialize_label("target_lb", row=0, column=1)
        self.initialize_label("edition_lb", row=1, column=1)
        self.initialize_label("url_lb", row=2, column=1, text=" ")
        self.initialize_label("file_lb", row=3, column=1, text="Local Files")
        self.initialize_label("current_source_lb", row=7, column=1, text="No selected source")
        self.initialize_label("current_url_lb", row=8, column=1, text="No selected url")
        self.initialize_label("current_category_lb", row=9, column=1, text="No selected category")
        self.initialize_label("spacing_lb", row=11, column=1, text="- - - - - - -")
        self.initialize_label("source_lb", row=0, column=3)
        self.initialize_label("source_url_lb", row=0, column=5, text="url")
        self.initialize_label("category_lb", row=0, column=7)
        self.initialize_label("table_lb", row=0, column=9)
        self.initialize_label("sql_lb", row=28, column=12, text='SQlite query:')

    def initialize_listboxes(self):
        self.initialize_listbox("file_list", row=4, column=1, rowspan=3, scrollbar=False)
        self.initialize_listbox("source_list", height=10, width=15, row=1, column=3, rowspan=10)
        self.initialize_listbox("source_url_list", height=10, width=15, row=1, column=5, rowspan=10, scrollbar=False)
        self.initialize_listbox("category_list", height=10, row=1, column=7, rowspan=10)
        self.initialize_listbox("table_list", height=10, row=1, column=9, rowspan=10)
        self.initialize_listbox("query_list", height=12, width=100, row=15, column=3, rowspan=12, columnspan=7)

    def initialize_vertical_spacing(self):
        self.initialize_label("space_0", text=' ', row=0, column=0, rowspan=22, sticky='NS')
        self.initialize_label("space_2", text=' ', row=0, column=2, rowspan=22, sticky='NS')
        self.initialize_label("space_8", text=' ', row=0, column=11, rowspan=22, sticky='NS')
        self.initialize_label("space_4", text=' ', row=0, column=6, rowspan=11, sticky='NS')

    def text_input(self):
        try:
            db = self.current_db_entry.get()
            sql = self.sql_entry.get()
            con = lite.connect(db)
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
            self.__getattribute__("query_list").insert(END, 'TclError: check current DB')

    def clear_main(self):
        self.__getattribute__("query_list").delete(0, 'end')

    def display_csv(self):
        path = self.current_csv_path_entry.get()
        self.__getattribute__("table_list").delete(0, 'end')
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if '.csv' in entry.name and entry.is_file():
                        self.__getattribute__("table_list").insert(END, entry.name)
        except FileNotFoundError:
            with os.scandir() as it:
                for entry in it:
                    if '.csv' in entry.name and entry.is_file():
                        self.__getattribute__("table_list").insert(END, entry.name)

    def display_db(self):
        path = self.current_db_path_entry.get()
        self.__getattribute__("source_list").delete(0, 'end')
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if '.sqlite' in entry.name and entry.is_file():
                        self.__getattribute__("source_list").insert(END, entry.name)
        except FileNotFoundError:
            with os.scandir() as it:
                for entry in it:
                    if '.sqlite' in entry.name and entry.is_file():
                        self.__getattribute__("source_list").insert(END, entry.name)

    def db_select(self):
        try:
            test = self.current_db_path_entry.get() + self.__getattribute__("source_list").selection_get()
            self.current_db_entry.delete(0, 'end')
            self.current_db_entry.insert(0, test)
        except TclError:
            self.__getattribute__("query_list").insert(END, '--Unable to load DB, incorrect path or name--')
        self.__getattribute__("source_list").selection_clear(0, 'end')
        self.display_table()

    def display_table(self):
        con = lite.connect(self.db)
        with con:
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            rows = cur.fetchall()
        self.__getattribute__("table_list").delete(0, 'end')
        if len(rows) == 0:
            self.__getattribute__("query_list").insert(END, '--No Table Found.Create Table, Import csv or Load DB--')
        else:
            for row in rows:
                self.__getattribute__("table_list").insert(END, row)

    def load_csv(self):
        db = self.current_db_entry.get()
        try:
            file = self.__getattribute__("category_list").selection_get()
            pathfile = self.path + file
            df = pd.read_csv(pathfile)
            name = file[:-4]
            con = lite.connect(db)
            with con:
                try:
                    df.to_sql(name=name, con=con, if_exists='fail')
                except ValueError:
                    self.__getattribute__("query_list").insert(END, '--Table already in current DB--')
        except TclError:
            self.__getattribute__("query_list").insert(END, '--Select a .csv to load--')
        self.display_table()

    def export_csv(self):
        db = self.current_db_entry.get()
        con = lite.connect(db)
        con.row_factory = lite.Row
        with con:
            cur = con.cursor()
            cur.execute(self.sql_entry.get())
            rows = cur.fetchall()
        name = self.current_csv_path_entry.get() + self.new_csv_entry.get() + '.csv'
        headers = list(rows[0].keys())
        data = [list(row) for row in rows]
        df = pd.DataFrame(data, columns=headers)
        df.to_csv(name, index=False)
        self.display_csv()

    def on_select(self, event, list_box):
        w = event.widget
        try:
            int(w.curselection()[0])
        except IndexError:
            return
        if list_box is self.__getattribute__("source_list"):
            self.select_source()
        if list_box is self.__getattribute__("category_list"):
            self.select_category()

    def read_csv(self):
        file = self.__getattribute__("category_list").selection_get()
        if file and not self.path.endswith('csv/'):
            with open(self.path + file, 'r') as file:
                lines = file.readlines()
                for line in lines:
                    self.__getattribute__("query_list").insert(END, line.strip())

    def load_target_input(self, event):
        target_input = self.target_edition_entry.get()
        targets = str(target_input).split(',')
        targets = [target.strip().lower() for target in targets]
        if len(targets) > 1:
            target = '/'.join(targets)
            path = f"{self.base_path}{target}{'/' if target else ''}"
            if os.path.isdir(path):
                self.path = path
                print(path)
                self.display_sources()
                self.display_edition()
                self.__getattribute__("query_list").insert(END, f'--{path} found--')
                self.update_label("target_lb", f'Target: {targets[0]}')
                self.update_label("edition_lb", f'Edition: {targets[1]}')
            else:
                self.__getattribute__("query_list").insert(END, f'--Invalid input: {path} not found--')
                self.__getattribute__("query_list").insert(END,
                                                           '-requires target and edition directory names to be created')
        else:
            self.__getattribute__("query_list").insert(END, f'--Invalid input--')
            self.__getattribute__("query_list").insert(END,
                                                       '-requires target and edition directory names, coma separated-')

    def select_source(self):
        try:
            folder = self.__getattribute__("source_list").selection_get()
            path_to_folder = f"{self.path}{folder}{'/' if folder else ''}"
            if os.path.isdir(path_to_folder):
                self.display_category()
                self.current_source = folder
                self.__getattribute__("source_list").selection_clear(0, 'end')
                self.display_category()
        except FileNotFoundError:
            self.__getattribute__("query_list").insert(END, '-- No category extracted for that source --')

    def select_category(self):
        self.clear_main()
        try:
            file = self.__getattribute__("category_list").selection_get()
            pathfile = self.path + self.current_source + '/' + file
            df = pd.read_csv(pathfile, sep='|')
            for item in df.iterrows():
                self.__getattribute__("query_list").insert(END, ' | '.join(list(item[1])))
            name = file[:-4]
        except FileNotFoundError:
            self.__getattribute__("query_list").insert(END, '-- No category extracted for that source --')

    def select_table(self):
        try:
            table = self.__getattribute__("table_list").selection_get()
            self.current_db_entry.delete(0, 'end')
            self.current_db_entry.insert(0, table)  # hips
        except TclError:
            self.__getattribute__("query_list").insert(END, '-- Selection failed --')
        self.__getattribute__("table_list").selection_clear(0, 'end')
        self.display_table()

    def display_category(self):
        self.__getattribute__("category_list").delete(0, 'end')
        folder = self.current_source
        if folder and not self.path.endswith('csv/'):
            path_to_file = f"{self.path}{folder}/"
            try:
                with os.scandir(path_to_file) as it:
                    for entry in it:
                        self.__getattribute__("category_list").insert(END, entry.name)
            except FileNotFoundError:
                self.__getattribute__("query_list").insert(END, '--No category csv found--')

    def display_sources(self):
        self.__getattribute__("source_list").delete(0, 'end')
        if not self.path.endswith('csv/'):
            try:
                with os.scandir(self.path) as it:
                    for entry in it:
                        if os.path.isdir(entry):
                            self.__getattribute__("source_list").insert(END, entry.name)
            except FileNotFoundError:
                self.__getattribute__("query_list").insert(END, '-- No source folder found --')

    def display_edition(self):
        self.__getattribute__("file_list").delete(0, 'end')
        try:
            with os.scandir(self.path) as it:
                for entry in it:
                    if '.csv' in entry.name and 'source' in entry.name and entry.is_file():
                        self.__getattribute__("file_list").insert(END, entry.name)
        except FileNotFoundError:
            self.__getattribute__("query_list").insert(END, '--No source csv nor db found--')

    def run(self):
        self.display_sources()
        self.mw.mainloop()


if __name__ == "__main__":
    gui = GUI()
    gui.run()
