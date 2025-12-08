from gui import GUI
from soupkitchen import SoupKitchen

test_name = 'test.csv'


class KitchenGraphic(GUI):
    kitchen = None
    base_url = None

    def __init__(self):
        super().__init__()
        self.initialize_command_buttons()

    def load_target_input(self, event):
        super().load_target_input(event)
        target, edition = self.path.split('/')[-2:]
        self.kitchen = SoupKitchen(target, edition)
        self.base_url = self.kitchen.H.get("base_url")
        self.update_label('url_lb', f"Url :{self.base_url}")

    def initialize_command_buttons(self):
        self.initialize_button("cook_url_bt", command="cook_url", row=12, column=1, text=" Cook  Url ", bg='grey')
        self.initialize_button("parse_item_bt", command="parse_item", row=14, column=1, text="Parse Item", bg='grey')
        self.initialize_button("parse_category_bt", command="parse_category", row=15, column=1,
                               text="Parse category", bg='grey')
        self.initialize_button("parse_source_bt", command="parse_source", row=16, column=1,
                               text="Parse Source", bg='grey')
        self.initialize_button("normalizer_bt", command="normalizer", row=17, column=1, text="Normalize", bg='grey')
        self.initialize_button("modeler_bt", command="modeler", row=18, column=1, text=" Modeler ", bg='grey')

    def cook_url(self):
        print('cooked')


if __name__ == "__main__":
    gui = KitchenGraphic()
    gui.run()
