from tkinter import TclError, Event, END
import pandas as pd
import os
from gui import GUI
from helpers.connection import Con
from helpers.helpers import Plate


class InteractiveGui(GUI):
    """ The controller for the View (GUI). Deals with Listbox <select> and the 2 CLI inputs, and few buttons.
        Also implements the Plate interaction with the GUI and the db / file system.
        Note that on_select allows the GUI to finish its initialization by selecting the target and edition.
    """
    def __init__(self):
        super().__init__()
        self.current_plate: Plate | None = None

    def on_select(self, list_box_name: str, curselection: list[int]) -> None:
        super(GUI, self).on_select(list_box_name, curselection)
        box = self.__getattribute__(list_box_name)
        try:
            selection = box.selection_get()
        except TclError:
            return
        first_name = "_".join(list_box_name.split('_')[:-1])
        if first_name in ["category", "table", "group", "target_edition"]:
            self.__getattribute__(f"select_{first_name}")(selection)
            box.selection_clear(0, 'end')
        if first_name == "source":
            if selection in self.source_url_map.keys():
                url = self.source_url_map[selection]
                box.selection_clear(0, 'end')
                self.select_source(selection, url)
        if first_name == "query":
            pass
        if first_name == "query_name":
            if selection in self.query_url_map:
                url = self.query_url_map[selection]
                box.selection_clear(0, 'end')
                self.current_url = url
                self.current_item = selection
        self.update_current_labels()
        self.update_current_buttons()

    def select_group(self, selection: str):
        self.current_group = selection
        self.__getattribute__("group_list").selection_clear(0, 'end')
        self.display_sources()
        self.display_groups()

    def select_source(self, name: str, url: str) -> None:
        clean_name = name.lower().replace(' ', '_')
        if self.check_source(clean_name, url):
            self.current_source = clean_name
            self.current_url = url
            self.current_item = self.current_source
            self.__getattribute__("source_list").selection_clear(0, 'end')
            self.display_category()
            self.clear_query_lists()
        else:
            self.say(f'-- {name} not valid --')

    def check_source(self, name: str, url: str ) -> bool:
        if self.from_filesystem:
            try:
                folder = name.lower().replace(' ', '_')
                path_to_folder = f"{self.path}{folder}{'/' if folder else ''}"
                if os.path.isdir(path_to_folder):
                    return True
            except FileNotFoundError:
                self.say('-- No category folder --')
                return False
        return True

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

    def select_category_from_csv(self, selection: str) -> None:
        names = selection.split('.')[0].split('__')
        name = names[0]
        status = names[-1].split('_')[0]
        pathfile = self.path + self.current_source + '/' + selection
        try:
            df = pd.read_csv(pathfile, sep='|')
        except FileNotFoundError:
            self.say('-- No category extracted for that source --')
        else:
            self.display_df(df)
            self.update_category(name)
            self.use_my_dfs({status: df})
        finally:
            self.__getattribute__("category_list").selection_clear(0, 'end')

    def select_table(self, selection) -> None:
        if selection:
            source = None
            category = None
            group = None
            if self.current_source and selection != "sources":
                source = self.current_source
            if self.current_group:
                group = self.current_group
            df = self.db_to_df(name=selection,
                               filters={"source": source, "group": group})
            print(df)
            if df is not None:
                self.display_df(df)
                status = "normalized"
                if selection == "links":
                    status = "links"
                if selection != "links":
                    self.update_category(selection)
                self.use_my_dfs({status: df})
            else:
                self.say('-- No data Found --')

    def execute_sql_cli_entry(self, event) -> None:
        sql = self.get_entry_input("sql_cli_entry")
        if not sql or self.db is None:
            return
        conn = Con(self.db)
        rows = conn.execute_sql(sql)
        count = len(rows)
        if count > 0:
            headers = list(rows[0].keys())
            rows_list = [list(row) for row in rows]
            df = pd.DataFrame(columns=headers, data=rows_list)
            self.display_df(df)
        self.display_tables()

    def execute_kitchen_cli_entry(self, event: Event) -> None:
        new_input = self.get_entry_input("kitchen_cli_entry")
        inputs = new_input.split(' --')
        if len(inputs) == 0:
            self.say('-- No Input command --')
            return
        command = inputs[0]
        arg_list = []
        kwarg_dict = {}
        if len(inputs) > 1:
            kwarg_list = inputs[1:]
            kwarg_dict = {kwarg.split('=')[0].strip(): kwarg.split('=')[1].strip() for kwarg in kwarg_list}
        commands = command.split(' -')
        command = commands[0].strip()
        if len(commands) > 1:
            arg_list = [command.strip() for command in commands[1:]]
        commands = command.split('.')
        if len(commands) > 0:
            command = self.__getattribute__(commands[0])
            if len(commands) > 1:
                commands = [command.strip() for command in commands[1:]]
                for item in commands:
                    command = command.__getattribute__(item)
                args = []
                for arg in arg_list:
                    if arg.startswith('self.'):
                        args.append(self.__getattribute__(arg.split(".")[1]))
                    else:
                        args.append(arg)
                try:
                    command(*arg_list, **kwarg_dict)
                except AttributeError:
                    self.say('-- Wrong Input command --')
                except TypeError:
                    self.say('-- Unexpected argument --')

    def get_entry_input(self, name: str)-> str:
        try:
            entry = self.__getattribute__(name)
            text = entry.get()
            return text
        except AttributeError:
            pass
        except TclError:
            pass
        return ""

    def update_display(self, message: str = "") -> None:
        if message:
            self.say(message)
        self.update_current_labels()
        self.update_current_buttons()

    def check_plate(self, status: str) -> bool:
        if self.current_plate is None:
            return False
        return self.current_plate.check(status, category=self.current_category, source=self.current_source)

    def get_plate(self, status: str) -> Plate | None:
        if self.check_plate(status):
            return self.current_plate
        if status in ["unsorted", "links", "soups", "normalized", "modeled"]:
            self.db_to_plate(status=status)
            if self.check_plate(status):
                return self.current_plate
        self.csv_to_plate(self.current_category, status=status)
        return None

    def use_my_dfs(self, dfs: dict[str, pd.DataFrame]) -> None:
        links = None
        status = ""
        df = None
        keys = dfs.keys()
        for key in keys:
            if key == "links":
                links = dfs[key]
            if key != "links":
                status = key
                df = dfs[key]
        name = self.current_source + " " + self.current_category + " " + status
        new_plate = Plate(name=name.strip(), category=self.current_category, title=name + " - From Db")
        new_plate.item_links = links
        if df is not None:
            data_dict = {status: df.to_dict(orient='records')}
            new_plate.data_dict = data_dict
            if status in ["completed", "normalized", "modeled"]:
                new_plate.completed = True
                if status != "completed":
                    new_plate.normalized = True
                    if status == "modeled":
                        new_plate.modeled = True
        self.current_plate = new_plate
        self.update_current_labels()
        self.update_current_buttons()

    def display_plate(self) -> None:
        if self.current_plate is None:
            return
        self.clear_query_lists()
        out = self.__getattribute__("query_list")
        out.insert(END, f"Title: {self.current_plate.title}", f"Status: {self.current_plate.status()}")
        links = self.current_plate.item_links
        dfs = self.current_plate.dfs
        if dfs:
            for category, df in dfs.items():
                out.insert(END, f"{category}: {len(df)}", f"headers: {df.columns}")
                self.display_df(df, clean=False)
        elif self.current_plate.data_dict:
            out.insert(END, self.current_plate.data_dict)
        elif self.current_plate.soup:
            out.insert(END, "Soup:", self.current_plate.soup)
        elif self.current_plate.content:
            out.insert(END, "Content:", self.current_plate.content)
        if links is not None:
            out.insert(END, f"Links: {len(links)}", f"headers: {links.columns}")
            self.display_df(links, clean=False)
        self.update_current_buttons()
