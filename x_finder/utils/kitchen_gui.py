from gui import GUI
from soupkitchen import SoupKitchen

test_name = 'test.csv'


class KitchenGraphic(GUI):
    kitchen = None
    base_url = None
    current_plate = None

    def __init__(self):
        super().__init__()
        self.initialize_command_buttons()

    def load_target_input(self, event):
        super().load_target_input(event)
        target, edition = self.path.split('/')[-2:]
        self.kitchen = SoupKitchen(target, edition)
        self.base_url = self.kitchen.H.get("base_url")
        self.update_label('url_lb', f"Url :{self.base_url}")

    def initialize_path(self, target=None, edition=None):
        super().initialize_path(target=target, edition=edition)
        if self.target and self.edition:
            self.kitchen = SoupKitchen(target, edition)
            self.base_url = self.kitchen.H.get("base_url")
            self.update_label('url_lb', f"{self.base_url}")

    def initialize_command_buttons(self):
        self.initialize_button("cook_url_bt", command=self.cook_url, row=12, column=1, text=" Cook  Url ", bg='grey')
        self.initialize_button("parse_item_bt", command=self.parse_item, row=14, column=1, text="Parse Item", bg='grey')
        self.initialize_button("parse_category_bt", command=self.parse_category, row=15, column=1,
                               text="Parse category", bg='grey')
        self.initialize_button("parse_source_bt", command=self.parse_source, row=16, column=1,
                               text="Parse Source", bg='grey')
        self.initialize_button("normalizer_bt", command=self.normalize, row=17, column=1, text="Normalize", bg='grey')
        self.initialize_button("modeler_bt", command=self.fit_to_model, row=18, column=1, text=" Modeler ", bg='grey')

    def update_labels_and_buttons(self, category=False):
        super(KitchenGraphic, self).update_labels_and_buttons()

    def cook_url(self):
        if self.current_url:
            plate = self.kitchen.cook_url(self.current_url)
            if plate.soup:
                self.current_plate = plate

    def update_kitchen(self):
        pass

    def parse_item(self):
        if self.current_plate:
            self.kitchen.parse_item(self.current_plate)

    def parse_category(self):
        pass

    def parse_source(self):
        pass

    def normalize(self):
        pass

    def fit_to_model(self):
        pass

if __name__ == "__main__":
    gui = KitchenGraphic()
    gui.run()
