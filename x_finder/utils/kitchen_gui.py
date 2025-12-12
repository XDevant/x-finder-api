import importlib
from gui import GUI
import soupkitchen as k
from tkinter import messagebox, END


class KitchenGraphic(GUI):
    kitchen = None
    base_url = None
    current_plate = None

    def __init__(self):
        super().__init__()
        self.initialize_command_buttons()

    def load_target_input(self, event):
        super().load_inbox_input(event)
        target, edition = self.path.split('/')[-2:]
        self.kitchen = k.SoupKitchen(target, edition)
        self.base_url = self.kitchen.H.get("base_url")
        self.update_label('url_lb', f"Url :{self.base_url}")

    def initialize_path(self, target=None, edition=None):
        super().initialize_path(target=target, edition=edition)
        if self.target and self.edition:
            self.kitchen = k.SoupKitchen(target, edition)
            self.base_url = self.kitchen.H.get("base_url")
            self.update_label('url_lb', f"Url: {self.base_url}")
            self.__getattribute__("message_box").insert(END, f'-- Kitchen for {self.target} {self.edition} ready--')
            self.__getattribute__("message_box").insert(END, f'-- Found {self.base_url} for site base url --')

    def initialize_command_buttons(self):
        self.initialize_button("cook_url_bt", command=self.cook_url, row=12, column=1, text=" Cook  Url ", bg='grey')
        self.initialize_button("parse_item_bt", command=self.parse_item, row=14, column=1, text="Parse Item", bg='grey')
        self.initialize_button("parse_category_bt", command=self.parse_category, row=15, column=1,
                               text="Parse category", bg='grey')
        self.initialize_button("parse_source_bt", command=self.parse_source, row=16, column=1,
                               text="Parse Source", bg='grey')
        self.initialize_button("normalizer_bt", command=self.normalize, row=17, column=1, text="Normalize", bg='grey')
        self.initialize_button("modeler_bt", command=self.fit_to_model, row=18, column=1, text=" Modeler ", bg='grey')

    def update_current_buttons(self, category=False):
        super(KitchenGraphic, self).update_current_buttons()

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

    def display_plate(self):
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

    def update_kitchen(self):
        importlib.reload(k)
        importlib.invalidate_caches()
        self.kitchen = k.SoupKitchen(self.target, self.edition)
        self.base_url = self.kitchen.H.get("base_url")
        self.update_label('url_lb', f"Url :{self.base_url}")

    def parse_item(self):
        if self.current_plate:
            self.kitchen.parse_item(self.current_plate, debug=True, verbose=True)
            self.clear_query_lists()
            self.display_plate()
            if self.current_plate.validated:
                self.__getattribute__("message_box").insert(END, f'-- Plate validated --')
            else:
                self.__getattribute__("message_box").insert(END, f'-- Plate Not validated --')
            if self.current_plate.completed:
                self.__getattribute__("message_box").insert(END, f'-- Plate completed --')
            else:
                self.__getattribute__("message_box").insert(END, f'-- Plate Not completed --')

    def parse_category(self):
        if self.current_plate and self.current_plate.item_links:
            if self.current_category in self.current_plate.item_links.keys():
                self.kitchen.complete_category(self.current_plate, self.current_category)

    def parse_source(self):
        pass

    def normalize(self):
        pass

    def fit_to_model(self):
        pass


if __name__ == "__main__":
    gui = KitchenGraphic()
    gui.run()
