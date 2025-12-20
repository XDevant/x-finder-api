from utils import U
from args import Ica
from typing import Iterable
from helpers import Plate, Result, Status
from reader import Reader
from bs4.element import Tag


class Parser:
    def __init__(self, target: str, edition: str) -> None:
        self.target: str = target
        self.edition: str = edition
        self.ica: dict[str, dict[str, str]] | None = None
        self.reader: Reader = Reader()

    def get(self, argument: str, category: str = "default", keys: bool = False) -> str | Iterable[str] | bool | int:
        return Ica.get(self.ica, argument, category, keys)

    def validate_plate(self, plate: Plate) -> bool:
        main_id = self.get("main_id")
        detail_title = self.get("detail_title")
        if plate.soup:
            main = plate.soup.find(id=main_id)
            if main:
                title = main.find(detail_title)
                if title:
                    return True
            else:
                print("Main is none for {plate.name} in category {plate.category}")
        else:
            print(f"No soup provided for {plate.name} in category {plate.category}.")
        return False

    def complete_plate(self,
                       plate: Plate,
                       parsed_rows: dict[str, list[dict[str, list | str]]]
                       ) -> dict[str, list[dict[str, list | str]]]:
        """
         Missing keys in item dicts will be nan soon. This hook is the right place to add a default value
         for some items missing data.
         parsed rows : {category: [{item_key: item_data, ...}, ...]
         """
        if "actions" not in parsed_rows.keys():
            return parsed_rows
        if plate.category != "skills_general" and plate.category != "skills_general":
            return parsed_rows
        action_list = parsed_rows["actions"]
        plate_item = parsed_rows[plate.category][0]
        name = self.clean_name(plate_item["name"])
        trained = False
        if "(Trained)" in plate_item["name"]:
            trained = True
        for action in action_list:
            if " Trained Actions" in action["name"]:
                trained = True
            if trained and plate.category == "skills" or plate.category == "skills_general":
                if "prerequisite" not in action.keys():
                    action["prerequisite"] = []
                if plate.category == "skills":
                    action["prerequisite"].append(f"Trained in {name}")
                else:
                    action["prerequisite"].append("Trained in related skill")
        return parsed_rows

    def parse_item(self,
                   plate: Plate,
                   status: Status = None,
                   result: Result = None,
                   debug: bool = False,
                   verbose: bool = False
                   ) -> dict[str, list[dict[str, str]]]:
        """Here we target the last div holding the title and the item content, extract data in the title then move
        to the start of the item content and call the read_soup method to extract the expected key, values.
        To make sure we find the title, ie the item's name and other expected data, we run a first method that
        should work if the data are found within the title tag. If not, the second method will recursively fill the
        missing parts with the text it finds.
        """
        if status.start is not None:
            self.reader.read_soup(status.start, status, result, plate.category, debug=debug, verbose=verbose)

        parsed_rows = []
        nested_rows = {}
        discarded = []
        for title in result.titles:
            if title and len(title) > 3 and 'x_finder_model' in title.keys():
                current_category = title['x_finder_model']
                if current_category == plate.category:
                    check = self.validate_title(title, result)
                    if check:
                        parsed_rows.append(title)
                    else:
                        discarded.append(title)
                else:
                    base_item_name = result.titles[0]["name"]
                    related_item = self.clean_name(base_item_name)
                    title["x_finder_related_item"] = related_item
                    title["x_finder_related_model"] = plate.category

                    if self.get("nested", current_category) and self.validate_nested_title(title, current_category):
                        if current_category not in nested_rows.keys():
                            nested_rows[current_category] = []
                        nested_rows[current_category].append(title)

        if verbose:
            for title in result.titles:
                if (title and len(title) > 3 and 'x_finder_model' in title.keys()) or debug:
                    print(title)
            if result.parsed:
                print("Parsed title")
                print(result.titles[0]["name"], result.parsed)
            if result.tails:
                print("tails")
                print(result.titles[0]["name"], result.tails)
        if debug or verbose:
            for title in parsed_rows:
                print(f"{plate.category} Items:")
                print(title)
            for key in nested_rows.keys():
                print("Nested item categories")
                print(nested_rows[key])
        nested_rows[plate.category] = parsed_rows
        return nested_rows

    @staticmethod
    def clean_name(name: str) -> str:
        return name.split('(')[0].strip()

    @staticmethod
    def validate_title(title: dict[str, str], result: Result) -> bool:
        if 'level' in result.titles[0].keys() and 'level' not in title.keys():
            print(f"{title} should have a level and is discarded")
            return False
        return True

    @staticmethod
    def validate_nested_title(title: dict[str, str], category: str) -> bool:
        if category in "actions":
            if "action" not in title.keys() and "traits" not in title.keys():
                return False
        return True

    def find_start(self,
                   plate: Plate,
                   status: Status,
                   debug: bool = False,
                   verbose: bool = False
                   ) -> None | dict[str, str]:
        title_dict = {"plate_name": plate.name,
                      "x_finder_model": plate.category}
        main = plate.soup.find(id="ctl00_RadDrawer1_Content_MainContent_DetailedOutput")
        if main is None:
            print("main is none")
        titles = main.find_all('h1')
        if titles is None:
            print("no title in main")
            titles = plate.soup.find_all('h1')
        if titles is None:
            print("no title in MainContent")
            return None
        for title in titles:
            if not title.get_text():
                continue
            title_content = title.get_text(separator=',').split(',')
            title_dict["name"] = title_content[0].strip(' ,;')
            status.start = title.next_sibling

            if len(title_content) > 2:
                text = title_content[1].strip(' ,;')
                if "action" in text:
                    title_dict['action'] = text

            if "level" in self.get("text_columns", plate.category) and len(title_content) > 1:
                level = title_content[-1].strip(' ,;')
                if level:
                    title_dict["level"] = level
                if level.endswith('+'):
                    status.family = True

            title_links = title.find_all('a')
            if not title_links:
                continue
            title_links = [link for link in title_links if link['href'] and link['href'] != 'PFS.aspx']
            if title_links:
                link = title_links[0]
                title_dict["url"] = link['href']
            else:
                title_dict["url"] = plate.url
            if verbose:
                print(f"Start found for {'family' if status.family else 'item'}: {title_dict['name']}")
                print(f"on h1: {title}")
            return title_dict
        if debug or verbose:
            print(f"Start not found for h1: {status.name}")
        status.start = None
        return title_dict

    def extract_links_by_id(self, plate: Plate, ica_id: str, ica_tag: str) -> list[dict[str, str] | None]:
        """ If our table is split among sub-tables, we fetch their urls.
        We store their names / urls as key / value pairs in a dict """
        if plate.soup:
            main = plate.soup.find(id=ica_id)
            print(main)
            if main:
                main = main.find_all(ica_tag, recursive=True)
            if not main:
                main = plate.soup.find(id=ica_id).find('nethys-search')
                print(main)
                if main:
                    print("found table")
                    main = main.find_all('td', recursive=True)
                    if not main:
                        print("No td found")
                        return []
                else:
                    print("no table found")
                return []
            nav_links = []
            for node in main:
                print(node)
                links = node.find_all('a', recursive=True)
                if links:
                    nav_list = [{"name": link.get_text(), "url": link['href']} for link in links]
                    self.clean_links(nav_list)
                    nav_links += nav_list
            if nav_links:
                print("links extracted")
                return nav_links
            print("links not found")
            return []
        else:
            print("No soup found, did you cook it?")
            return []

    def sort_sources(self, plate: Plate) -> [list, list]:
        remaster_start_year = self.get("start_date")
        remaster_list = []
        legacy_list = []
        if "sources" not in plate.data_dict.keys():
            return [], []
        for row in plate.data_dict["sources"]:
            release = "0"
            errata = "0"
            if "release_date" in row.keys() and row["release_date"]:
                release = row["release_date"][0]
            if "latest_errata" in row.keys() and row["latest_errata"]:
                errata = row["latest_errata"][0]
            if self.find_year(release) >= remaster_start_year or self.find_year(errata) >= remaster_start_year:
                remaster_list.append(row)
            else:
                legacy_list.append(row)
        return remaster_list, legacy_list

    @staticmethod
    def find_year(date: str) -> int:
        if date:
            year = date.split('/')[-1]
            if year.isalnum():
                return int(year)
        return 0

    @staticmethod
    def clean_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
        return links

    @staticmethod
    def extract_source_links(source_soup: Tag) -> list[Tag]:
        if source_soup:
            try:
                item_list = source_soup.find(id="main").find_all('u')
            except AttributeError:
                return []
            try:
                link_list = [item.a for item in item_list if item.a is not None]
                return link_list
            except IndexError:
                print(item_list)
        print("No soup found, did you cook it?")
        return []

    def parse_source_links(self,
                           item_list: list[Tag]
                           ) -> (dict[str, list[dict[str, str]]], dict[str, list[dict[str, str]]]):
        category_data = {}
        no_category_data = {}
        flags = []
        for item in item_list:
            name = item.get_text()
            url = item['href']

            if name:
                item_dict = {"name": name, "url": url}
            else:
                continue
            if url:
                snake_item_category = url.split('.')[0]
                item_category = U.snake_to_under(snake_item_category)
                if "General=true" in url:
                    item_category += "_general"
            else:
                item_category = "unknown"

            if item_category not in self.get("", keys=True):
                if item_category not in no_category_data.keys():
                    no_category_data[item_category] = []
                no_category_data[item_category].append(item_dict)
            else:
                if item_category not in category_data.keys():
                    category_data[item_category] = []
                if item_category == "equipment" and item_dict["url"] in flags:
                    continue
                flags.append(item_dict["url"])
                category_data[item_category].append(item_dict)
        return category_data, no_category_data
