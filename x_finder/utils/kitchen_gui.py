import pandas as pd
from gui import GUI
from soupkitchen import SoupKitchen as Kitchen
from tkinter import END
from helpers.helpers import Plate
from tkinter import Event


class KitchenGraphic(GUI):
    debug = True
    verbose = False
    links_to_csv = False
    completed_to_csv = True
    normalized_to_csv = False
    kitchen: Kitchen | None = None

    def __init__(self) -> None:
        super().__init__()
        self.base_url: str | None = None
        self.current_plate: Plate | None = None

    def load_target_input(self, event: Event) -> None:
        super().load_inbox_input(event)
        target, edition = self.path.split('/')[-2:]
        self.kitchen = Kitchen(target, edition)
        if self.kitchen is not None:
            self.base_url = str(self.kitchen.H.sica("base_url"))
        self.update_label('url_lb', f"Url :{self.base_url}")

    def initialize_path(self, target: str | None = None, edition: str | None = None) -> None:
        super().initialize_path(target=target, edition=edition)
        if self.target and self.edition:
            self.kitchen = Kitchen(target, edition)
        if self.kitchen is not None:
            self.base_url = str(self.kitchen.H.sica("base_url"))
            self.update_label('title_url_label', f"Url: {self.base_url}")
            self.__getattribute__("message_box").insert(END, f'-- Kitchen   for {self.target} {self.edition} ready--')
            self.__getattribute__("message_box").insert(END, f'-- Found {self.base_url} for site base url --')
            self.update_current_buttons()

    def update_current_buttons(self) -> None:
        super(KitchenGraphic, self).update_current_buttons()
        if self.current_url:
            self.update_button("cook_url", color="green", default='normal')
        else:
            self.update_button("cook_url", color='grey', default='disabled')
        if self.current_plate:
            if self.current_plate.soup and self.current_plate.name == self.current_item.lower().replace(' ', '_'):
                self.update_button("parse_item", color="green", default='normal')
            else:
                self.update_button("parse_item", color='grey', default='disabled')
            if self.current_category:
                self.update_button("parse_category", color="green", default='normal')
                if self.current_plate.dfs and self.current_category in self.current_plate.dfs.keys():
                    self.update_button("normalize_category", color="green", default='normal')
            if self.current_plate.dfs:
                self.update_button("normalize_selection", color="green", default='normal')
        if self.current_source:
            self.update_button("parse_selection", "green")
        if self.kitchen:
            self.update_button("update_ica", "green")
            self.update_button("update_provider", "green")
            self.update_button("update_parser", "green")
            self.update_button("update_normalizer", "green")
            self.update_button("update_modeler", "green")

    def update_current_labels(self) -> None:
        super(KitchenGraphic, self).update_current_labels()
        self.update_label("plate_name_label", f"Name: {self.current_plate.name if self.current_plate else '-'}")
        self.update_label("plate_url_label", f"Url: {self.current_plate.url if self.current_plate else '-'}")
        self.update_label("plate_category_label",
                          f"Category: {self.current_plate.category if self.current_plate else '-'}")
        self.update_label("plate_status_label", f"Status: {self.current_plate.status() if self.current_plate else '-'}")

    def update_display(self, message: str = "") -> None:
        if message:
            self.__getattribute__("message_box").insert(END, message)
        self.update_current_labels()
        self.update_current_buttons()

    def check_plate(self, status: str) -> bool:
        if self.current_plate is None:
            return False
        return self.current_plate.check(status, category=self.current_category, source=self.current_source)

    def get_plate(self, status: str) -> Plate | None:
        if self.check_plate(status):
            return self.current_plate
        if status in ["unsorted", "links", "soup", "normalized", "modeled"]:
            self.db_to_plate(status=status)
            if self.check_plate(status):
                return self.current_plate
        self.csv_to_plate(status=status)
        if self.check_plate(status):
            return self.current_plate
        return None

    def csv_to_plate(self, status: str) -> None:
        if status:
            name = f"{self.current_category}__{status}.csv"
            df = self.load_csv(name)
            self.use_my_dfs({status: df})

    def db_to_plate(self,
                    item: str | None = None,
                    source: str | None = None,
                    category: str | None = None,
                    group: str | None = None,
                    status: str | None = None) -> None:
        db = self.db
        dfs = {}
        if status is None:
            return None
        if status == "unsorted":
            db = self.target_db
            df = self.db_to_df(name="sources", filters={"item": item, "group": group}, db=db)
            dfs["unsorted"] = df
        elif status in "links soups":
            filters = {"item": item, "source": source, "category": category, "group": group}
            if status == "links":
                filters["soups"] = "No soups"
            if status == "soups":
                filters["NOT soups"] = "No soups"
            item_links = self.db_to_df(name="links",
                                       filters=filters,
                                       db=db)
            dfs["links"] = item_links
        elif status in "normalized modeled":
            if category is not None:
                df = self.get_category_df(item=item, source=source, category=category, group=group, status=status)
                if df is not None:
                    dfs[category] = df
            else:
                for category in self.get_category_list(source=source, group=group, status=status):
                    df = self.get_category_df(item=item, source=source, category=category, group=group, status=status)
                    if df is not None:
                        dfs[category] = df
        if category == "sources" and item:
            item_links = self.db_to_df(name="links",
                                       filters={"item": item, "source": source, "category": category, "group": group},
                                       db=db)
            dfs["links"] = item_links
        if dfs:
            self.use_my_dfs(dfs)
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
        new_plate = Plate(name=name.strip(), category=self.current_category)
        new_plate.item_links = links
        if df is not None:
            new_plate.dfs = {status:  df}
            new_plate.data_dict = {status: df.to_dict(orient='records')}
            if status in ["completed", "normalized", "modeled"]:
                new_plate.completed = True
                if status != "completed":
                    new_plate.normalized = True
                    if status == "modeled":
                        new_plate.modeled = True
        self.current_plate = new_plate
        self.update_current_labels()
        self.update_current_buttons()

    def plate_to_db(self, plate: Plate, category: str | None = None) -> None:
        dfs = plate.dfs
        if dfs is None:
            return
        if category is None:
            for key, value in dfs:
                self.df_to_db(value, f"{key}__{plate.status()}")
        elif category in dfs.keys():
            name = category + "__" + plate.status()
            self.df_to_db(dfs[category], name)
        elif category in ["links", "sources"] and plate.item_links is not None:
            self.df_to_db(plate.item_links, category)

    def cook_url(self) -> None:
        if self.kitchen is None:
            return
        self.clear_message_box()
        self.current_plate = None
        if self.current_url:
            link = {"name": self.current_item,
                    "url": self.current_url,
                    "source": self.current_source,
                    "category": self.current_category}
            plate = self.kitchen.cook_url(link, keep_alive=False)
            self.__getattribute__("message_box").insert(END, f'-- Plate provided --')
            if plate.soup:
                self.current_plate = plate
                self.clear_query_lists()
                self.display_plate()
                self.update_display()
        else:
            self.__getattribute__("message_box").insert(END, f'-- No Url to cook, click one --')
        self.update_current_buttons()

    def cook_urls(self) -> None:
        if self.kitchen is None:
            return
        if not self.current_plate or self.current_plate.item_links is None:
            self.db_to_plate(group=self.current_group,
                             source=self.current_source,
                             category=self.current_category,
                             status="links")
        if self.current_plate and self.current_plate.item_links is not None:
            self.kitchen.cook_urls(self.current_plate)
            self.plate_to_db(self.current_plate, category="links")
            self.clear_query_lists()
            if self.current_plate.dfs:
                self.iterate_dfs_display()
            self.update_display()

    def display_plate(self) -> None:
        if self.current_plate is None:
            return
        self.clear_query_lists()
        self.__getattribute__("query_name_list").insert(END, self.current_plate.name)
        self.__getattribute__("query_url_list").insert(END, self.current_plate.url)
        self.__getattribute__("query_list").insert(END,
                                                   "Title:", self.current_plate.title,
                                                   "Category:", self.current_plate.category,
                                                   "Status:", self.current_plate.status()
                                                   )
        if self.current_plate.data_dict:
            self.__getattribute__("query_list").insert(END, f"Item: {self.current_plate.name}")
            category_list = self.current_plate.data_dict[self.current_plate.category]
            if category_list:
                main_item = [f"{key}: {value}" for key, value in category_list[0].items()]
                self.__getattribute__("query_list").insert(END, *main_item)
                if len(category_list) > 1:
                    self.__getattribute__("query_list").insert(END, f"other items from {self.current_plate.category}")
                    for item in category_list[1:]:
                        self.__getattribute__("query_list").insert(END,
                                                                   *[f"{key}: {value}" for key, value in item.items()])
                for key in self.current_plate.data_dict:
                    if key != self.current_plate.category:
                        count = len(self.current_plate.data_dict[key])
                        self.__getattribute__("query_list").insert(END, f"found {count} items from category {key}")

        elif self.current_plate.content:
            self.__getattribute__("query_list").insert(END, "Content:", self.current_plate.content)
        if self.current_plate.item_links is not None:
            self.__getattribute__("query_list").insert(END, "Item Links:", self.current_plate.item_links)
        self.update_current_buttons()

    def parse_item(self) -> None:
        plate = self.current_plate
        if plate is not None and self.kitchen is not None:
            if plate.category == "sources":
                self.kitchen.extract_source_links(plate,
                                                  debug=self.debug,
                                                  verbose=self.verbose,
                                                  to_csv=self.links_to_csv)
                self.plate_to_db(plate,
                                 category="links")
            self.kitchen.parse_item(plate,
                                    debug=self.debug,
                                    verbose=self.verbose)
            self.clear_query_lists()
            self.display_plate()
            if plate.validated:
                self.__getattribute__("message_box").insert(END, f'-- Plate {plate.name} validated --')
            else:
                self.__getattribute__("message_box").insert(END, f'-- Plate {plate.name} Not validated --')
            if plate.completed:
                self.__getattribute__("message_box").insert(END, f'-- Plate {plate.name} completed --')
            else:
                self.__getattribute__("message_box").insert(END, f'-- Plate {plate.name} Not completed --')
            self.update_display()

    def parse_category(self) -> None:
        if not self.current_category or self.kitchen is None:
            self.__getattribute__("message_box").insert(END, '-- No category/edition selected--')
            return
        plate = self.current_plate
        if plate is not None and plate.item_links is not None and self.current_category in plate.item_links.keys():
            message = f'-- Parsing {self.current_category} for {plate.name} --'
            self.__getattribute__("message_box").insert(END, message)
            self.kitchen.parse_category(plate,
                                        self.current_category,
                                        debug=self.debug,
                                        verbose=self.verbose)
            self.update_display(message='-- Category parsed --')
        else:
            file_name = self.current_source + '\\' + self.current_category + "__links_ok.csv"
            try:
                links_from_csv = pd.read_csv(file_name, delimiter='|')
                if not links_from_csv.empty:
                    link_plate = Plate(name=f"{self.current_source}")
                    link_plate.category = self.current_category
                    link_plate.item_links = links_from_csv
                    self.__getattribute__("message_box").insert(END, f'-- Csv found for {self.current_category} --')
                    self.kitchen.parse_category(link_plate,
                                                self.current_category,
                                                debug=self.debug,
                                                verbose=self.verbose)
                    self.update_display(message='-- Category parsed --')
            except FileNotFoundError:
                if self.current_plate:
                    self.__getattribute__("message_box").insert(END, '-- No item link found in plate--')
                else:
                    self.__getattribute__("message_box").insert(END, f'-- {file_name} Not Found!--')

    def parse_source(self) -> None:
        if self.current_plate and self.current_plate.item_links is not None and self.kitchen is not None:
            self.kitchen.parse_all_category(self.current_plate, debug=self.debug, verbose=self.verbose)

    def extract_sources(self) -> None:
        if self.kitchen is None:
            return
        plate = self.kitchen.extract_sources()
        if plate is not None:
            plate.category = "sources"
            self.current_plate = plate
            self.current_item = plate.name
            self.current_source = plate.category
            self.current_category = "sources"
            self.current_url = plate.url
            self.update_display(message='-- Sources Extracted!--')
            self.display_plate()
            if plate.dfs is not None:
                self.df_to_db(plate.dfs["sources"], name="sources", db=self.target_db)
            if plate.item_links is not None:
                self.df_to_db(plate.item_links, name="groups", db=self.target_db)
        else:
            self.__getattribute__("message_box").insert(END, '-- Index Not Found!--')

    def sort_sources(self) -> None:
        if self.kitchen is None:
            return
        if not self.current_plate or self.current_plate.item_links is None:
            self.db_to_plate(group=self.current_group, status="unsorted")
        if self.current_plate:
            self.kitchen.sort_sources_editions(self.current_plate)
            if self.current_plate.dfs:
                edition_df = self.current_plate.dfs[self.edition]
                self.kitchen.H.normalizer.norm_df(edition_df, key="sources", source_name=self.edition)
                self.kitchen.H.normalizer.norm_sources_df(edition_df)
                df = edition_df.applymap(str)
                self.df_to_db(df, name="sources")
                self.df_to_db(self.current_plate.dfs["sources"], name="sources", db=self.target_db)
                self.df_to_db(self.current_plate.dfs["links"], name="links")
                self.update_display(message='-- Editions sorted!--')
            else:
                self.update_display(message='-- Failed to sort editions!--')
            self.display_plate()
        else:
            self.__getattribute__("message_box").insert(END, '-- Plate Not Found!--')

    def normalize_category(self) -> None:
        if self.kitchen is None:
            return
        if self.current_plate is None or not self.check_plate(status="completed"):
            return
        self.kitchen.normalize_df(plate=self.current_plate,
                                  category=self.current_category,
                                  source=self.current_source)

    def normalize_source(self) -> None:
        if self.kitchen is None:
            return
        if self.current_plate is None or not self.check_plate(status="completed"):
            return
        self.kitchen.normalize_dfs(plate=self.current_plate,
                                   source=self.current_source)

    def fit_category_to_model(self) -> None:
        if self.kitchen is None:
            return
        if self.current_plate is None or not self.check_plate(status="normalized"):
            return
        self.kitchen.fit_category_to_model(plate=self.current_plate,
                                           category=self.current_category,
                                           source=self.current_source)

    def fit_source_to_models(self) -> None:
        if self.kitchen is None:
            return
        if self.current_plate is None or not self.check_plate(status="normalized"):
            return
        self.kitchen.fit_source_to_model(plate=self.current_plate,
                                         source=self.current_source)

    def iterate_dfs_display(self) -> None:
        if self.current_plate is None or self.current_plate.dfs is None:
            return
        for category in self.current_plate.dfs.keys():
            self.display_df(self.current_plate.dfs[category])

    def display_category_df(self, category: str) -> None:
        if self.current_plate is None or self.current_plate.dfs is None:
            return
        if category not in self.current_plate.dfs.keys():
            self.__getattribute__("message_box").insert(END, f'-- No df found in plate for {category}--')
            return
        df = self.current_plate.dfs[category]
        for item in df.iterrows():
            row_list = list(item[1])
            self.__getattribute__("query_name_list").insert(END, row_list[0])
            self.__getattribute__("query_url_list").insert(END, row_list[1])
            if len(row_list) >= 2:
                cleared_list = [str(cell) for cell in row_list[2:]]
                self.__getattribute__("query_list").insert(END, ' | '.join(cleared_list))
        return

    def update_ica(self):
        if self.kitchen is not None:
            self.kitchen.reload_ica()

    def update_provider(self) -> None:
        if self.kitchen:
            self.kitchen.update_worker("provider")
            self.__getattribute__("message_box").insert(END, f'-- Provider updated--')

    def update_parser(self) -> None:
        if self.kitchen:
            self.kitchen.update_worker("parser")
            self.__getattribute__("message_box").insert(END, f'-- Parser updated--')

    def update_normalizer(self) -> None:
        if self.kitchen:
            self.kitchen.update_worker("normalizer")
            self.__getattribute__("message_box").insert(END, f'-- Normalizer updated--')

    def update_modeler(self) -> None:
        if self.kitchen:
            self.kitchen.update_worker("modeler")
            self.__getattribute__("message_box").insert(END, f'-- Modeler updated--')


if __name__ == "__main__":
    gui = KitchenGraphic()
    gui.run()
