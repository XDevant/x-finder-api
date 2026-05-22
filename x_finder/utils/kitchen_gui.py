from interactive_gui import InteractiveGui
from soupkitchen import SoupKitchen as Kitchen
from tkinter import END


def is_kitchen(method):
    def wrapper(self, *args, **kwargs):
        if self.kitchen is None:
            return None
        return method(self, *args, **kwargs)
    return wrapper


class KitchenGraphic(InteractiveGui):
    debug = True
    verbose = False
    links_to_csv = False
    completed_to_csv = True
    normalized_to_csv = False
    kitchen: Kitchen | None = None

    def __init__(self) -> None:
        super().__init__()
        self.base_url: str | None = None

    def initialize_path(self, target: str | None = None, edition: str | None = None) -> None:
        super().initialize_path(target=target, edition=edition)
        if self.target and self.edition:
            self.kitchen = Kitchen(target, edition)
        if self.kitchen is not None:
            self.base_url = self.kitchen.H.sica("base_url")
            self.update_label('title_url_label', f"Url: {self.base_url}")
            self.say(f'-- Kitchen   for {self.target} {self.edition} ready--')
            self.say(f'-- Found {self.base_url} for site base url --')
            self.update_current_buttons()

    def update_current_buttons(self) -> None:
        super(KitchenGraphic, self).update_current_buttons()
        if self.current_url:
            self.update_button("cook_url", color="green", default='normal')
        else:
            self.update_button("cook_url", color='grey', default='disabled')
        for update in ["parse_item", "fit_item_to_model"]:
            name = self.current_item.lower().replace(' ', '_')
            check = self.current_plate is not None and name in self.current_plate.name
            if check:
                self.update_button(update, color="green", default='normal')
            else:
                self.update_button(update, color='grey', default='disabled')
        for update in ["cook_category", "parse_category", "fit_category_to_model"]:
            if self.current_category:
                self.update_button(update, color="green", default='normal')
            else:
                self.update_button(update, color="grey", default='disabled')
        for update in ["cook_selection", "parse_selection", "fit_selection_to_models"]:
            if self.current_source or self.current_category:
                self.update_button(update, "green", default='normal')
            else:
                self.update_button(update, "grey", default='disabled')
            self.update_button("parse_selection", "grey", default='disabled')
        for update in ["ica", "provider", "parser", "normalizer", "modeler"]:
            if self.kitchen:
                self.update_button(f"update_{update}", "green")
            else:
                self.update_button(f"update_{update}", "grey", default="disabled")

    def update_current_labels(self) -> None:
        super(KitchenGraphic, self).update_current_labels()
        self.update_label("plate_name_label",
                          f"Name: {self.current_plate.name if self.current_plate else '-'}")
        self.update_label("plate_url_label",
                          f"Url: {self.current_plate.url if self.current_plate else '-'}")
        self.update_label("plate_category_label",
                          f"Category: {self.current_plate.category if self.current_plate else '-'}")
        self.update_label("plate_status_label",
                          f"Status: {self.current_plate.status() if self.current_plate else '-'}")

    @is_kitchen
    def cook_url(self) -> None:
        self.clear_message_box()
        self.current_plate = None
        if self.current_url:
            link = {"name": self.current_item,
                    "url": self.current_url,
                    "source": self.current_source,
                    "category": self.current_category}
            plate = self.kitchen.cook_url(link, keep_alive=False)
            self.say('-- Plate provided --')
            self.plate_to_db(plate, category="links")
            if plate.soup:
                self.current_plate = plate
                self.clear_query_lists()
                self.display_plate()
                self.update_display()
        else:
            self.say('-- No Url to cook, click one --')
        self.update_current_buttons()

    @is_kitchen
    def cook_category(self) -> None:
        if self.current_category:
            self.cook_selection()
        else:
            self.say("Select a category!")

    @is_kitchen
    def cook_selection(self) -> None:
        self.get_plate("links")
        if self.current_plate is not None and self.current_plate.item_links is not None:
            self.kitchen.cook_urls(self.current_plate)
            self.plate_to_db(self.current_plate, category="links")
            self.say("Links cooked !")
            self.clear_query_lists()
            self.display_df(self.current_plate.item_links)
            self.update_display()
        else:
            self.say("No links found")

    @is_kitchen
    def parse_item(self) -> None:
        plate = self.current_plate
        if plate is not None:
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
            self.normalize_category()
            self.clear_query_lists()
            self.display_plate()
            self.say(f"-- Plate {'' if plate.validated else 'not'} validated --")
            self.say(f"-- Plate {'' if plate.completed else 'not'} completed --")
            self.update_display()

    @is_kitchen
    def parse_category(self) -> None:
        if not self.current_category:
            self.say('-- No category/edition selected--')
            return
        self.get_plate("soups")
        if self.current_plate is None:
            self.update_display(message='-- Unable to find soup to parse --')
            return
        self.say(f'-- Parsing {self.current_category} for {self.current_plate.name} --')
        self.kitchen.parse_category(self.current_plate,
                                    self.current_category,
                                    debug=self.debug,
                                    verbose=self.verbose)
        self.kitchen.normalize_dfs(self.current_plate, self.current_source)
        if self.current_plate.dfs is not None:
            for key in self.current_plate.dfs.keys():
                self.plate_to_db(self.current_plate, category=key)
        self.display_tables()
        self.update_display(message='-- Category parsed --')

    @is_kitchen
    def parse_selection(self) -> None:
        self.get_plate("soups")
        if self.current_plate and self.current_plate.item_links is not None:
            self.kitchen.parse_all_category(self.current_plate, debug=self.debug, verbose=self.verbose)
            self.kitchen.normalize_dfs(self.current_plate, self.current_source)
            if self.current_plate.dfs is not None:
                for key in self.current_plate.dfs.keys():
                    self.plate_to_db(self.current_plate, category=key)
            self.display_tables()
            self.update_display(message='-- Selection parsed --')

    @is_kitchen
    def extract_sources(self) -> None:
        plate = self.kitchen.extract_sources()
        if plate is not None:
            plate.category = "sources"
            self.current_plate = plate
            self.current_item = plate.name
            self.current_source = plate.category
            self.current_category = "sources"
            if plate.url:
                self.current_url = plate.url
            self.update_display(message='-- Sources Extracted!--')
            self.display_plate()
            if plate.dfs is not None:
                self.df_to_db(plate.dfs["sources"], name="sources", db=self.target_db)
            if plate.item_links is not None:
                self.df_to_db(plate.item_links, name="groups", db=self.target_db)
        else:
            self.say('-- Index Not Found!--')

    @is_kitchen
    def sort_sources(self) -> None:
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
            self.say('-- Plate Not Found!--')

    @is_kitchen
    def normalize_category(self) -> None:
        if self.current_category:
            self.normalize_selection()
            self.update_display(message='-- Category normalized!--')
        else:
            self.say("No category selected")
            return

    @is_kitchen
    def normalize_selection(self) -> None:
        self.get_plate("completed")
        if self.current_plate is not None:
            self.kitchen.normalize_dfs(plate=self.current_plate,
                                       source=self.current_source)

    @is_kitchen
    def fit_item_to_model(self) -> None:
        pass

    @is_kitchen
    def fit_category_to_model(self) -> None:
        if not self.current_category:
            self.say("No category selected")
            return
        self.get_plate("normalized")
        if self.current_plate is not None and self.check_plate(status="normalized"):
            self.kitchen.fit_category_to_model(plate=self.current_plate,
                                               category=self.current_category,
                                               source=self.current_source)

    @is_kitchen
    def fit_selection_to_models(self) -> None:
        self.get_plate("normalized")
        if self.current_plate is not None and self.check_plate(status="normalized"):
            self.kitchen.fit_source_to_model(plate=self.current_plate,
                                             source=self.current_source)

    @is_kitchen
    def update_ica(self):
        self.kitchen.reload_ica()
        self.say('-- Ica updated--')

    @is_kitchen
    def update_provider(self) -> None:
        self.kitchen.update_worker("provider")
        self.say('-- Provider updated--')

    @is_kitchen
    def update_parser(self) -> None:
        self.kitchen.update_worker("parser")
        self.say('-- Parser updated--')

    @is_kitchen
    def update_normalizer(self) -> None:
        self.kitchen.update_worker("normalizer")
        self.say('-- Normalizer updated--')

    @is_kitchen
    def update_modeler(self) -> None:
        self.kitchen.update_worker("modeler")
        self.say('-- Modeler updated--')

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


if __name__ == "__main__":
    gui = KitchenGraphic()
    gui.run()
