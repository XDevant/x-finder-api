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
                 content: bytes | None = None,
                 soup: Tag | None = None
                 ) -> None:
        self.url: str | None = url
        self.title: str = title
        self.content: bytes | None = content
        self.soup: Tag | None = soup
        self.name: str = name
        self.category: str = category
        self.item_links: dict[str | None, list[dict[str | None, str]]] = {}
        self.no_category_item_links: dict[str | None, list[dict[str | None, str]]] = {}
        self.data_dict: dict[str | None, list[dict[str | None, str]]] = {}
        self.dfs: dict[str | None, DataFrame] = {}
        self.validated: bool = False
        self.completed: bool = False
        self.status: str = "empty"


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
