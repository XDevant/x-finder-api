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

    @staticmethod
    def format_df(df):
        df["release_date"] = [SoupKitchen.translate_date(date) for date in df["release_date"]]
        df["errata_date"] = df.apply(
            lambda r: SoupKitchen.translate_date(
                str(r["latest_errata"]).split(' - ')[-1]
                ) if r["latest_errata"] else None,
            axis=1)
        df["errata_version"] = df.apply(
            lambda r: str(r["latest_errata"]).split(' - ')[0].strip() if r["latest_errata"] else "-",
            axis=1)
        df = df[["name",
                 "group",
                 "category",
                 "release_date",
                 "errata_date",
                 "errata_version",
                 "nethys_url",
                 "product_page_url",
                 "latest_errata_url"]]
        df.rename(columns={'product_page_url': 'paizo_url', 'latest_errata_url': 'errata_url'},
                  inplace=True)
        return df

    @staticmethod
    def cook():
        bowl = SourceSoup("Sources.aspx")
        return bowl


class ItemSoup(SoupKitchen):
    @staticmethod
    def format_df(df):
        return df


if __name__ == "__main__":
    # SourceSoup.cook()
    # soup = SoupKitchen("Sources.aspx?ID=1")
    # soup.list_df = {"traits": soup.load_fixture(app="core", name="Core Rulebook\\traits_items")}
    #  soup.norm_dfs()
    # soup.list_df["traits"].to_csv(f"{BASE_DIR}\\core\\fixtures\\csv\\Core Rulebook\\traits_items_raw.csv", sep='|', index=False)
    # soup.load_source_items()
    # soup = SoupKitchen("Traits.aspx?ID=135")
    # soup.parse_item_data(show=True, category="traits")
    soup = SoupKitchen("Weapons.aspx?ID=15")
    soup.parse_item_data(show=True, category="weapons")
