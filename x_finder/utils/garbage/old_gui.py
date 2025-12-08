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
    target_lb = Label(mw, text='Target: Unknown', bg='lightCyan2')
    edition_lb = Label(mw, text='Edition: Unknown', bg='lightCyan2')
    target_edition_lb = Label(mw, text='Enter target,edition', bg='lightCyan2')
    target_edition_entry = Entry(mw, width=30)
    edition_files_lb = Label(mw, text='Data', bg='lightCyan2')
    sources_lb = Label(mw, text='Sources', bg='lightCyan2')
    category_lb = Label(mw, text='Categories', bg='lightCyan2')
    table_lb = Label(mw, text='Tables:', bg='lightCyan2')
    sql_lb = Label(mw, text='SQlite query:', bg='lightCyan2')
    sql_entry = Entry(mw, width=30)
    new_csv_lb = Label(mw, text='name next .csv to be created', bg='lightCyan2')
    new_csv_entry = Entry(mw, width=30)
    new_csv_entry.insert(END, 'new_csv')

    current_db_entry = Entry(mw, width=30)
    current_db_path_entry = Entry(mw, width=30)
    current_csv_path_entry = Entry(mw, width=30)
    base_path = "fixtures/csv/"
    path = ""

    edition_list = Listbox(mw, height=3, width=30, selectmode='single')
    sources_list = Listbox(mw, height=8, width=30, selectmode='single')
    category_list = Listbox(mw, height=8, width=30, selectmode='single')
    table_list = Listbox(mw, height=8, width=30)
    scroll_table = Scrollbar(mw, orient=VERTICAL, command=table_list.yview)
    table_list['yscrollcommand'] = scroll_table.set

    list_query = Listbox(mw, height=9, width=100, bg='burlywood1')
    scroll_v = Scrollbar(mw, orient=VERTICAL, command=list_query.yview)
    list_query['yscrollcommand'] = scroll_v.set

    space_0 = Label(mw, text='', bg='lightCyan2')
    space_2 = Label(mw, text='>', bg='lightCyan2')
    space_4 = Label(mw, text='', bg='lightCyan2')
    space_6 = Label(mw, text='<', bg='lightCyan2')
    space_8 = Label(mw, text='<', bg='lightCyan2')

    target_lb.grid(row=0, column=1, sticky=EW)
    edition_lb.grid(row=1, column=1, sticky=EW)
    edition_files_lb.grid(row=3, column=1, sticky=EW)
    edition_list.grid(row=4, column=1, rowspan=3, sticky=EW)
    target_edition_lb.grid(row=7, column=1, sticky=EW)
    target_edition_entry.grid(row=8, column=1, sticky=EW)

    new_csv_lb.grid(row=16, column=1, sticky=EW)
    new_csv_entry.grid(row=17, column=1, sticky=EW)
    sql_lb.grid(row=18, column=1, sticky=EW)
    sql_entry.grid(row=19, column=1, sticky=EW)

    sources_list.grid(row=1, column=3, rowspan=10)
    sources_lb.grid(row=0, column=3, sticky=EW)
    category_list.grid(row=1, column=5, rowspan=10)
    category_lb.grid(row=0, column=5, sticky=EW)
    table_list.grid(row=1, column=7, rowspan=10)
    scroll_table.grid(row=1, column=8, rowspan=10, sticky=NS)
    table_lb.grid(row=0, column=7, sticky=EW)

    space_0.grid(row=0, column=0, rowspan=22, sticky=NS)
    space_2.grid(row=0, column=2, rowspan=22, sticky=NS)
    space_4.grid(row=0, column=4, rowspan=10, sticky=NS)
    space_6.grid(row=0, column=6, rowspan=10, sticky=NS)
    space_8.grid(row=0, column=8, sticky=NS)

    list_query.grid(row=10, column=3, rowspan=8, columnspan=5)
    list_query.insert(END, "—Here comes the query-result:—")
    scroll_v.grid(row=10, column=8, rowspan=8, sticky=NS)

    def __init__(self):
        self.target_edition_entry.bind('<Return>', self.load_target_input)
        self.sources_list.bind('<<ListboxSelect>>', lambda e: self.on_select(e, self.sources_list))
        self.category_list.bind('<<ListboxSelect>>', lambda e: self.on_select(e, self.category_list))
        self.table_list.bind('<<ListboxSelect>>', lambda e: self.on_select(e, self.table_list))
        self.send_bt = Button(self.mw, text='Push to send', bg='khaki1', command=self.text_inp)
        self.clear_query = Button(self.mw, text='Clear Listbox', bg='khaki2', command=self.clear_list_query)
        self.load_csv_bt = Button(self.mw, text='Load csv into table(s)', bg='khaki2', command=self.load_csv)
        self.export_csv_bt = Button(self.mw, text='Export query as csv', bg='khaki2', command=self.export_csv)
        self.send_bt.grid(row=13, column=1)
        self.clear_query.grid(row=22, column=3)
        self.load_csv_bt.grid(row=22, column=5)
        self.export_csv_bt.grid(row=22, column=7)

    def text_inp(self):
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
                self.list_query.insert(END, headers)
                for row in rows:
                    drow = list(row)
                    self.list_query.insert(END, drow)
            self.display_table(db)
        except TclError:
            self.list_query.insert(END, 'TclError: check current DB')

    def clear_list_query(self):
        self.list_query.delete(0, 'end')

    def display_csv(self):
        path = self.current_csv_path_entry.get()
        self.table_list.delete(0, 'end')
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if '.csv' in entry.name and entry.is_file():
                        self.table_list.insert(END, entry.name)
        except FileNotFoundError:
            with os.scandir() as it:
                for entry in it:
                    if '.csv' in entry.name and entry.is_file():
                        self.table_list.insert(END, entry.name)

    def display_db(self):
        path = self.current_db_path_entry.get()
        self.sources_list.delete(0, 'end')
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if '.sqlite' in entry.name and entry.is_file():
                        self.sources_list.insert(END, entry.name)
        except FileNotFoundError:
            with os.scandir() as it:
                for entry in it:
                    if '.sqlite' in entry.name and entry.is_file():
                        self.sources_list.insert(END, entry.name)

    def db_select(self):
        try:
            test = self.current_db_path_entry.get() + self.sources_list.selection_get()
            self.current_db_entry.delete(0, 'end')
            self.current_db_entry.insert(0, test)
        except TclError:
            self.list_query.insert(END, '--Unable to load DB, incorrect path or name--')
        db = self.current_db_entry.get()
        self.sources_list.selection_clear(0, 'end')
        self.display_table(db)

    def display_table(self, db):
        con = lite.connect(db)
        with con:
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            rows = cur.fetchall()
        self.table_list.delete(0, 'end')
        if len(rows) == 0:
            self.list_query.insert(END, '--No Table Found.Create Table, Import csv or Load DB--')
        else:
            for row in rows:
                self.table_list.insert(END, row)

    def load_csv(self):
        db = self.current_db_entry.get()
        try:
            file = self.category_list.selection_get()
            pathfile = self.path + file
            df = pd.read_csv(pathfile)
            name = file[:-4]
            con = lite.connect(db)
            with con:
                try:
                    df.to_sql(name=name, con=con, if_exists='fail')
                except ValueError:
                    self.list_query.insert(END, '--Table already in current DB--')
        except TclError:
            self.list_query.insert(END, '--Select a .csv to load--')
        self.display_table(db)

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
        if list_box is self.sources_list:
            self.select_source()
        if list_box is self.category_list:
            self.select_category()

    def print_csv(self):
        file = self.category_list.selection_get()
        if file and not self.path.endswith('csv/'):
            with open(self.path + file, 'r') as file:
                lines = file.readlines()
                for line in lines:
                    self.list_query.insert(END, line.strip())

    def load_target_input(self, event):
        target_input = self.target_edition_entry.get()
        targets = str(target_input).split(',')
        targets = [target.strip().lower() for target in targets]
        if len(targets) > 1:
            target = '/'.join(targets)
            path = f"{self.base_path}{target}{'/' if target else ''}"
            if os.path.isdir(path):
                self.path = path
                self.display_sources()
                self.display_edition()
                self.list_query.insert(END, f'--{path} found--')
                self.target_lb.config(text=f'Target: {targets[0]}')
                self.edition_lb.config(text=f'Edition: {targets[1]}')
            else:
                self.list_query.insert(END, f'--Invalid input: {path} not found--')
                self.list_query.insert(END, '-requires target and edition directory names to be created')
        else:
            self.list_query.insert(END, f'--Invalid input--')
            self.list_query.insert(END, '-requires target and edition directory names, coma separated-')

    def select_source(self):
        try:
            folder = self.sources_list.selection_get()
            path_to_folder = f"{self.path}{folder}{'/' if folder else ''}"
            self.current_db_entry.delete(0, 'end')
            self.current_db_entry.insert(0, test)
        except TclError:
            self.list_query.insert(END, '-- Selection failed --')
        db = self.current_db_entry.get()
        self.sources_list.selection_clear(0, 'end')
        self.display_table(db)

    def display_category(self):
        self.category_list.delete(0, 'end')
        folder = self.sources_list.selection_get()
        if folder and not self.path.endswith('csv/'):
            path_to_file = f"{self.path}{folder}{'/' if folder else ''}"
            try:
                with os.scandir(path_to_file) as it:
                    for entry in it:
                        if os.path.isdir(entry):
                            self.category_list.insert(END, entry.name)
            except FileNotFoundError:
                self.list_query.insert(END, '--No category csv found--')

    def display_sources(self):
        self.sources_list.delete(0, 'end')
        if not self.path.endswith('csv/'):
            try:
                with os.scandir(self.path) as it:
                    for entry in it:
                        if os.path.isdir(entry):
                            self.sources_list.insert(END, entry.name)
            except FileNotFoundError:
                self.list_query.insert(END, '-- No source folder found --')

    def display_edition(self):
        self.edition_list.delete(0, 'end')
        try:
            with os.scandir(self.path) as it:
                for entry in it:
                    if '.csv' in entry.name and 'source' in entry.name and entry.is_file():
                        self.edition_list.insert(END, entry.name)
        except FileNotFoundError:
            self.list_query.insert(END, '--No source csv nor db found--')

    def run(self):
        self.display_sources()
        self.mw.mainloop()


if __name__ == "__main__":
    gui = GUI()
    gui.run()
