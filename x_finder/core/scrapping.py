from x_finder.utils.scrapp import SoupKitchen, BASE_DIR
from x_finder.utils.fixtures.args import item_category_arguments as ica


item_category_arguments = ica
"""
ICA is a dictionary whose keys are item catégories matching our model names.
The values are dictionaries where each key, value pair is a parameter used to
find data in an item's page. Column names and HTML tags mostly.
They should only be changed if the target site front end change overtime.
"""


class SourceSoup(SoupKitchen):
    """
    A soup kitchen specific for sources extraction that creates a csv/df matching the model.
    """

    def format_df(self):
        self.df["release_date"] = [SoupKitchen.translate_date(date) for date in self.df["release_date"]]
        self.df["errata_date"] = self.df.apply(
            lambda r: SoupKitchen.translate_date(
                str(r["latest_errata"]).split(' - ')[-1]
                ) if r["latest_errata"] else None,
            axis=1)
        self.df["errata_version"] = self.df.apply(
            lambda r: str(r["latest_errata"]).split(' - ')[0].strip() if r["latest_errata"] else "-",
            axis=1)
        self.df.rename(columns={'product_page_url': 'paizo_url', 'latest_errata_url': 'errata_url',
                                'source_group': 'group'},
                       inplace=True)
        self.df = self.df[["name",
                           "source_group",
                           "category",
                           "release_date",
                           "errata_date",
                           "errata_version",
                           "nethys_url",
                           "paizo_url",
                           "errata_url"]]

    def cook_sources(self):
        self.extract_nav_links()
        self.load_sub_tables()
        self.format_df()
        self.save(self.df, "sources_normed", app="core")


class ItemSoup(SoupKitchen):
    @staticmethod
    def format_df(df):
        return df


if __name__ == "__main__":
    # bowl = SourceSoup("Sources.aspx")
    # bowl.cook_sources()
    soup = SoupKitchen("Sources.aspx?ID=1")
    soup.load_source_items(category_filter=["backgrounds"],
                           debug=True, verbose=True)
    """
    for i in range(1, 4):
        soup = SoupKitchen(f"Domains.aspx?ID={i}")
        print(soup.url, soup.name)
        soup.parse_item(verbose=True, category="domains")
        soup = SoupKitchen(f"Deities.aspx?ID={i}")
        soup.parse_item(verbose=True, category="deities")
        soup = SoupKitchen(f"Heritages.aspx?ID={i}")
        soup.parse_item(verbose=True, category="heritages")
        soup = SoupKitchen(f"Spells.aspx?ID={i}")
        soup.parse_item(verbose=True, category="spells")
        soup = SoupKitchen(f"Weapons.aspx?ID={i}")
        soup.parse_item(verbose=True, category="weapons")
        soup = SoupKitchen(f"Traits.aspx?ID={i}")
        soup.parse_item(verbose=True, category="traits")
        soup = SoupKitchen(f"Ancestries.aspx?ID={i}")
        soup.parse_item(verbose=True, category="ancestries")
        soup = SoupKitchen(f"Feats.aspx?ID={i}")
        soup.parse_item(verbose=True, category="feats")
        soup = SoupKitchen(f"Equipment.aspx?ID={i}")
        soup.parse_item(verbose=True, category="equipment")
        soup = SoupKitchen(f"Armor.aspx?ID={i}")
        soup.parse_item(verbose=True, category="armors")
        soup = SoupKitchen(f"AnimalCompanions.aspx?ID={i}")
        soup.parse_item(verbose=True, category="animal_companions")
        soup = SoupKitchen(f"Backgrounds.aspx?ID={i}")
        soup.parse_item(verbose=True, category="backgrounds")
        soup = SoupKitchen(f"Bloodlines.aspx?ID={i}")
        soup.parse_item(verbose=True, category="bloodlines")
        soup = SoupKitchen(f"Classes.aspx?ID={i}")
        soup.parse_item(verbose=True, category="classes")
        soup = SoupKitchen(f"Skills.aspx?ID={i}")
        soup.parse_item(debug=True, category="skills")

    soup = SoupKitchen(f"Equipment.aspx?ID=1552")
    soup.parse_item(verbose=True, category="equipment")
    """
