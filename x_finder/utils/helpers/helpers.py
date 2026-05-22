from typing import Any
from pandas import DataFrame
from bs4.element import Tag


class Plate:
    """
    Class designed to store the data relative to its url until complete parsing.
    Given to the Provider and Parser for completion. Returned to the kitchen.
    Kitchen method dealing with a single plate return it to the GUI while methods iterating lists store the result
    in panda dataframes and csv or db. All forget the plate once exited.
    The GUI only stores the last Plate it received.
    """
    def __init__(self,
                 url: str | None = None,
                 title: str = "Plate",
                 name: str = "unknown",
                 category: str = "default",
                 content: bytes | str | None = None,
                 soup: Tag | None = None
                 ) -> None:
        self.url: str | None = url
        self.title: str = title
        self.content: bytes | str | None = content
        self.soup: Tag | None = soup
        self.name: str = name
        self.category: str = category
        self.item_links: DataFrame | None = None
        self.data_dict: dict[str, list[dict[str, Any]]] | None = None
        self.dfs: dict[str, DataFrame] | None = None
        self.extracted_tables: dict[str, DataFrame] = {}
        self.validated: bool = False
        self.completed: bool = False
        self.normalized: bool = False
        self.modeled: bool = False

    def status(self):
        if self.modeled:
            return "modeled"
        if self.normalized:
            return "normalized"
        if self.completed:
            return "completed"
        if self.validated:
            return "validated"
        if self.soup is not None:
            return "soup"
        if self.content is not None:
            return "content"
        links = self.item_links
        if links is not None:
            if not links[links["soup"] != "No soup"].empty:
                return "soups"
            return "links"
        return "empty"

    def check(self, status: str, category: str = None, source: str = None) -> bool:
        """
        checks if the data expected by status is in the Plate. category and source being used as filters
        usage: if false is returned to a button command, the command will load missing data from db/csv
        """
        completion = status
        if completion in ["normalized", "modeled"]:
            completion = "completed"
        match completion:
            case "soup":
                return self.soup is not None
            case "content":
                return self.content is not None
            case "links":
                return self.item_links is not None and not self.item_links.empty
            case "soups":
                if self.item_links is not None:
                    filtered_df = self.item_links[ self.item_links["soup"] != "No Soup"]
                    return not filtered_df.empty
                return False
            case "completed":
                dfs = self.dfs
                if dfs is None or not self.__getattribute__(status):
                    return False
                filtered_dfs = self.filter_dfs(dfs, category=category, source=source)
                if filtered_dfs:
                    return True
                return False
            case _:
                return False

    def filter_dfs(self,
                   dfs: dict[str, DataFrame],
                   category: str = None,
                   source: str = None
                   ) -> dict[str, DataFrame]:
        filtered_dfs = {}
        if category is None:
            for key,df in dfs.items():
                key_df = self.filter_df(dfs[key], source=source)
                if key_df is not None and not key_df.empty:
                    filtered_dfs[key] = key_df
            return filtered_dfs
        else:
            if category in dfs.keys():
                category_df = self.filter_df(dfs[category], source=source)
                if category_df is not None and not category_df.empty:
                    filtered_dfs[category] = category_df
        return filtered_dfs

    @staticmethod
    def filter_df(df: DataFrame | None, category: str = None, source: str = None) -> DataFrame | None:
        if df is None or category is None and source is None:
            return df
        if "category" not in df.columns or "source" not in df.columns:
            return df
        if category is None:
            return df[ df["source"] == source ]
        if source is not None:
            category_df = df[ df["category"] == category ]
            return category_df[ category_df["source"] == source ]
        return df[ df["category"] == category ]


class Status:
    """
    Keep track of the parsing steps, given as argument to recursive method read_node to keep it sane
    """
    def __init__(self, known_name: str, known_url: str) -> None:
        self.name: str = known_name
        self.url: str = known_url
        self.ended: int = 0  # number of titles, to fill the right dict
        self.last_key: str = ""  # we parsed a key and are loading values if truthy,
        self.loaded_values: list = []  # values can be in many nodes. We stack, waiting for end or key identification
        self.family: bool = False  # we expect nested items of the same category in this page
        self.start: Tag | None = None  # first node after item's title in soup Navigable string


class Result:
    """
    Stores parsing results,  given as argument to and filled by recursive method read_node to keep it sane
    """
    def __init__(self):
        self.parsed: list = []
        self.titles: list = []
        self.links: list = []
        self.tails: list = []
        self.tables: dict[str, DataFrame] = {}
