import sqlite3

from x_finder.utils.helpers.helpers import Plate
from .csvpandas import CsvPandasMixin
from .sqlitepandas import SqlitePandaMixin
from pandas import DataFrame


class PlateMixin(SqlitePandaMixin, CsvPandasMixin):

    def use_my_dfs(self, dfs: dict[str, DataFrame]) -> None:
        pass

    def csv_to_plate(self, category: str, status: str) -> None:
        if status:
            name = f"{category}__{status}.csv"
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
                filters["soup"] = "No soup"
            if status == "soups":
                filters["NOT soup"] = "No soup"
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

    def plate_to_db(self, plate: Plate, category: str | None = None) -> None:
        if category in ["links", "sources"] and plate.item_links is not None:
            try:
                self.df_to_db(plate.item_links, category)
            except sqlite3.OperationalError as e:
                print(f"Sqlite3 Operational Error: {e}")
        status: str = plate.status()
        dfs: dict[str,DataFrame] | None = None
        if status == "normalized":
            dfs = plate.dfs
        elif status == "modeled":
            dfs = plate.model_dfs
        if dfs is None:
            return
        if category is None:
            for key, value in dfs:
                name = f"{key}__{status}"
                self.df_to_db(value, name)
        elif category in dfs.keys():
            name = category + "__" + plate.status()
            self.df_to_db(dfs[category], name)

