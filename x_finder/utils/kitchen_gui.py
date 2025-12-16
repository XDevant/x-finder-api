from gui import GUI
from soupkitchen import SoupKitchen as Kitchen
from tkinter import messagebox, END
from helpers import Plate
from tkinter import Event


class KitchenGraphic(GUI):
    def __init__(self) -> None:
        super().__init__()
        self.kitchen: Kitchen | None = None
        self.base_url: str | None = None
        self.current_plate: Plate | None = None
        self.initialize_command_buttons()

    def load_target_input(self, event: Event) -> None:
        super().load_inbox_input(event)
        target, edition = self.path.split('/')[-2:]
        self.kitchen = Kitchen(target, edition)
        self.base_url = self.kitchen.H.get("base_url")
        self.update_label('url_lb', f"Url :{self.base_url}")

    def initialize_path(self, target: str | None = None, edition: str | None = None) -> None:
        super().initialize_path(target=target, edition=edition)
        if self.target and self.edition:
            self.kitchen = Kitchen(target, edition)
            self.base_url = self.kitchen.H.get("base_url")
            self.update_label('url_lb', f"Url: {self.base_url}")
            self.__getattribute__("message_box").insert(END, f'-- Kitchen for {self.target} {self.edition} ready--')
            self.__getattribute__("message_box").insert(END, f'-- Found {self.base_url} for site base url --')
            self.update_current_buttons()

    def initialize_command_buttons(self) -> None:
        self.initialize_button("extract_sources_bt", command=self.extract_sources,
                               row=12, column=1, text=" Extract Sources ", bg='grey')
        self.initialize_button("cook_url_bt", command=self.cook_url,
                               row=13, column=1, text=" Cook  Url ", bg='grey')
        self.initialize_button("parse_item_bt", command=self.parse_item,
                               row=14, column=1, text="Parse Item", bg='grey')
        self.initialize_button("parse_category_bt", command=self.parse_category,
                               row=15, column=1, text="Parse category", bg='grey')
        self.initialize_button("normalize_category_bt", command=self.normalize_category(),
                               row=16, column=1, text="Normalize Category", bg='grey')
        self.initialize_button("fit_category_to_model_bt", command=self.fit_category_to_model,
                               row=17, column=1, text="Fit category to Model", bg='grey')
        self.initialize_button("parse_source_bt", command=self.parse_source,
                               row=18, column=1, text="Parse Source", bg='grey')
        self.initialize_button("normalize_source_bt", command=self.normalize_source,
                               row=19, column=1, text="Normalize Source", bg='grey')
        self.initialize_button("fit_source_to_models_bt", command=self.fit_source_to_models(),
                               row=20, column=1, text="Fit source to model", bg='grey')
        self.initialize_button("update_provider_bt", command=self.update_provider(),
                               row=30, column=7, text="Update Provider", bg='grey')
        self.initialize_button("update_parser_bt", command=self.update_parser(),
                               row=30, column=8, text="Update Parser", bg='grey')
        self.initialize_button("update_normalizer_bt", command=self.update_normalizer(),
                               row=30, column=9, text="Update Parser", bg='grey')
        self.initialize_button("update_modeler_bt", command=self.update_modeler(),
                               row=30, column=10, text="Update Modeler", bg='grey')

    def update_current_buttons(self) -> None:
        super(KitchenGraphic, self).update_current_buttons()
        if self.current_url:
            self.update_button("cook_url", "green")
        if self.current_plate:
            self.update_button("parse_item", "green")
            if self.current_category:
                self.update_button("parse_category", "green")
                if self.current_plate.dfs and self.current_category in self.current_plate.dfs.keys():
                    self.update_button("normalize_category", "green")
            if self.current_plate.dfs:
                self.update_button("normalize_source", "green")
        if self.current_source:
            self.update_button("parse_source", "green")
        if self.kitchen:
            self.update_button("update_provider", "green")
            self.update_button("update_parser", "green")
            self.update_button("update_normalizer", "green")
            self.update_button("update_modeler", "green")

    def cook_url(self):
        self.clear_message_box()
        self.current_plate = None
        if self.current_url:
            plate = self.kitchen.cook_url(self.current_url)
            self.__getattribute__("message_box").insert(END, f'-- Plate provided --')
            if plate.soup:
                self.current_plate = plate
                self.clear_query_lists()
                self.display_plate()
                response = messagebox.askyesnocancel(title='Save Soup as Plate.csv?',
                                                     message='Save soup in csv with yes answer,no save in db or cancel')
                if response is None:
                    self.__getattribute__("message_box").insert(END, f"-- {self.current_plate.name}' extracted --")
                elif response is True:
                    self.__getattribute__("message_box").insert(END, f"-- {self.current_plate.name}' content in csv--")
                else:
                    self.__getattribute__("message_box").insert(END, f"-- {self.current_plate.name}' content in db--")
        else:
            self.__getattribute__("message_box").insert(END, f'-- No Url to cook, click one --')
        self.update_current_buttons()

    def display_plate(self) -> None:
        self.clear_query_lists()
        self.__getattribute__("query_name_list").insert(END, self.current_plate.name)
        self.__getattribute__("query_url_list").insert(END, self.current_plate.url)
        self.__getattribute__("query_list").insert(END,
                                                   "Title:", self.current_plate.title,
                                                   "Category:", self.current_plate.category,
                                                   "Validated:", self.current_plate.validated,
                                                   "Completed:", self.current_plate.validated
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
        if self.current_plate.item_links:
            self.__getattribute__("query_list").insert(END, "Item Links:", self.current_plate.item_links)
        self.update_current_buttons()

    def parse_item(self) -> None:
        if self.current_plate:
            self.kitchen.parse_item(self.current_plate, debug=True, verbose=True)
            self.clear_query_lists()
            self.display_plate()
            if self.current_plate.validated:
                self.__getattribute__("message_box").insert(END, f'-- Plate {self.current_plate.name} validated --')
            else:
                self.__getattribute__("message_box").insert(END, f'-- Plate {self.current_plate.name} Not validated --')
            if self.current_plate.completed:
                self.__getattribute__("message_box").insert(END, f'-- Plate {self.current_plate.name} completed --')
            else:
                self.__getattribute__("message_box").insert(END, f'-- Plate {self.current_plate.name} Not completed --')
            self.update_current_buttons()

    def parse_category(self) -> None:
        if not self.current_category:
            self.__getattribute__("message_box").insert(END, '-- No category selected--')
            return
        check = self.current_plate and self.current_plate.item_links
        if check and self.current_category in self.current_plate.item_links.keys():
            message = f'-- Parsing {self.current_category} for {self.current_plate.name} --'
            self.__getattribute__("message_box").insert(END, message)
            self.kitchen.parse_category(self.current_plate, self.current_category)
            self.__getattribute__("message_box").insert(END, '-- Category parsed --')
        else:
            if self.current_plate:
                self.__getattribute__("message_box").insert(END, '-- No item link found in plate--')
            file_name = self.current_source + '\\' + self.current_category + "__links_ok.csv"
            try:
                links_from_csv = self.load_csv(file_name)
                if not links_from_csv.empty:
                    print(links_from_csv)
                    link_plate = Plate(name=f"{self.current_source}")
                    link_plate.category = self.current_category
                    link_plate.item_links = {self.current_category: links_from_csv.to_dict('records')}
                    self.__getattribute__("message_box").insert(END, f'-- Links found for {self.current_category} --')
                    self.kitchen.parse_category(link_plate, self.current_category)
                    self.__getattribute__("message_box").insert(END, '-- Category parsed --')
            except FileNotFoundError:
                self.__getattribute__("message_box").insert(END, f'-- {file_name} Not Found!--')

    def parse_source(self) -> None:
        if self.current_plate and self.current_plate.item_links:
            self.kitchen.parse_all_category(self.current_plate)

    def extract_sources(self) -> None:
        plate = self.kitchen.extract_sources()
        if plate:
            self.current_plate = plate
            self.current_item = plate.name
            self.current_source = ""
            self.current_item = ""
            self.update_current_labels()
            self.update_current_buttons()
            self.__getattribute__("message_box").insert(END, '-- Sources Extracted!--')
            if plate.name == "remaster":
                self.__getattribute__("message_box").insert(END, f'-- Edition {plate.name} sorted!--')
        else:
            self.__getattribute__("message_box").insert(END, '-- Index Not Found!--')

    def normalize_category(self) -> None:
        pass

    def normalize_source(self) -> None:
        pass

    def fit_category_to_model(self) -> None:
        pass

    def fit_source_to_models(self) -> None:
        pass

    def iterate_dfs_display(self) -> None:
        if self.current_category:
            self.display_category_df(self.current_category)
        category = next(self.current_plate.dfs.__iter__())
        if category != self.current_category:
            self.display_category_df(category)

    def display_category_df(self, category: str) -> None:
        if category not in self.current_plate.dfs.keys():
            self.__getattribute__("message_box").insert(END, f'-- No df found in plate for {category}--')
            return
        df = self.current_plate.dfs[category]
        for item in df.iterrows():
            row_list = list(item[1])
            self.__getattribute__("query_name_list").insert(END, row_list[0])
            self.__getattribute__("query_url_list").insert(END, row_list[1])
            if len(row_list) >= 2:
                cleared_list = [str(row) for row in row_list[2:]]
                self.__getattribute__("query_list").insert(END, ' | '.join(cleared_list))

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
