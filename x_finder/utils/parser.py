from bs4.element import Tag
from tools import U
from helpers.helpers import Plate, Result, Status
from reader import Reader
from x_finder.utils.mixins.ica import IcaMixin
from pandas import DataFrame


class Parser(IcaMixin):
    def __init__(self, target: str, edition: str) -> None:
        self.target: str = target
        self.edition: str = edition
        self.ica: dict[str, dict[str, str | list[str]]] = {}
        if not self.target:
            self.reader: Reader = Reader()
            self.reader.ica = self.ica

    def validate_plate(self, plate: Plate) -> None:
        main_id = self.sica("main_id")
        if plate.soup and plate.soup.find(id=main_id):
            plate.validated = True

    def find_titles(self, plate: Plate, debug: bool = False, verbose: bool = False) -> list[Tag]:
        main_id = self.sica("main_id")
        title_id = self.sica("title_id")
        title_tag = self.sica("title_tag")
        title_class = self.sica("title_class")
        if plate.soup is None:
            return []
        main = plate.soup.find(id=main_id)
        if main is None:
            print(f"Main not found for {plate.name}")
            return []
        if title_id:
            titles = main.find_all(id=title_id)
        else:
            titles = main.find_all(title_tag, class_=title_class)
        if verbose:
            print(titles)
        if debug and len(titles) == 0:
            print("No title found")
        return titles

    def parse_titles(self,
                     plate: Plate,
                     status: Status,
                     titles: list[Tag],
                     debug: bool = False,
                     verbose: bool = False) -> None | dict[str, str | list[str]]:
        title_dict: dict[str, str | list[str]] = {"plate_name": plate.name,
                      "x_finder_model": plate.category}
        expected_length = 1
        end = "level" in self.lica("text_columns", plate.category)
        end_found = not end
        if end:
            expected_length += 1
        for title in titles:
            if not title.get_text():
                continue
            title_content = title.get_text(separator=',').split(',')
            title_content = [part.strip(' ,;') for part in title_content]
            item_name = title_content[0]
            name_split = item_name.split("(")
            if len(name_split) > 1 and self.sica("subtype", category=plate.category):
                item_name = name_split[0]
                title_dict["subtype"] = name_split[-1].strip(')')
            title_dict["name"] = item_name
            next_tag = title.next_sibling
            status.start = next_tag

            if len(title_content) >= max(expected_length, 2):
                text = title_content[1]
                if "action" in text:
                    title_dict['action'] = text
                    expected_length += 1

            if end and len(title_content) >= expected_length:
                level = title_content[-1]
                if level:
                    title_dict["level"] = level
                    expected_length += 1
                    end_found = True
                    if level.endswith('+'):
                        status.family = True

            title_links = title.find_all('a')
            if title_links:
                title_links = [link for link in title_links if link['href'] and link['href'] != 'PFS.aspx']
            if title_links:
                link = title_links[0]
                title_dict["url"] = self.sica("base_url") + link['href']
            else:
                if plate.url:
                    title_dict["url"] = plate.url
                else:
                    title_dict["url"] = ""

            if not end_found and not title_links:
                next_text = next_tag.get_text()
                if next_text and isinstance(next_text, str):
                    if "description" not in title_dict.keys():
                        title_dict["description"] = [next_text]
                    else:
                        title_dict["description"] += [next_text]
                if debug:
                    print("Possible fake title spotted")
                continue
            if verbose:
                print(f"Start found for {'family' if status.family else 'item'}: {title_dict['name']}")
                print(f"on h1: {title}")
            if debug:
                print(f"end_found: {end_found}", f"links : {title_links}")
            return title_dict

        if debug:
            print(title_dict, "Missing level or link")
        return title_dict

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
        if plate.category != "skills_general" and plate.category != "skills":
            return parsed_rows
        action_list = parsed_rows["actions"]
        plate_item = parsed_rows[plate.category][0]
        name = self.clean_name(str(plate_item["name"]))
        trained = False
        if "(Trained)" in plate_item["name"]:
            trained = True
        for action in action_list:
            if " Trained Actions" in action["name"]:
                trained = True
            if trained:
                action_prq = []
                if "prerequisite" in action.keys():
                    if isinstance(action["prerequisite"], str):
                        action_prq.append(action["prerequisite"])
                    if isinstance(action["prerequisite"], list):
                        action_prq.append(action["prerequisite"])
                if plate.category == "skills":
                    action_prq.append(f"Trained in {name}")
                else:
                    action_prq.append("Trained in related skill")

                action["prerequisite"] = action_prq
        return parsed_rows

    def parse_item(self,
                   plate: Plate,
                   status: Status,
                   result: Result,
                   debug: bool = False,
                   verbose: bool = False
                   ) -> tuple[dict[str, list[dict[str, str]]], dict[str, DataFrame]]:
        """Here we target the first tag after the title  and call the read_soup method to extract the expected
        key, values and fills the result instance.

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

                    if self.bica("nested", current_category) and self.validate_nested_title(title, current_category):
                        if current_category not in nested_rows.keys():
                            nested_rows[current_category] = []
                        nested_rows[current_category].append(title)
            elif debug:
                print(title)

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
        tables = {}
        for name, table in result.tables.items():
            if name not in tables.keys():
                tables[name] = [table]
            else:
                tables[name].append(table)
        return nested_rows, tables

    @staticmethod
    def clean_name(name: str) -> str:
        return name.split('(')[0].strip("{} ,;)[]'")

    @staticmethod
    def clean_link_name(name: str) -> str:
        return name.strip("{} ,;[]'")

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

    def sort_sources(self, plate: Plate, editions: list[str]) -> dict[str, dict[str, str]]:
        sorted_dict = {}
        if plate.data_dict is None:
            return sorted_dict
        for edition in editions:
            sorted_dict[edition] = []
        if "sources" not in plate.data_dict.keys():
            return sorted_dict
        for row in plate.data_dict["sources"]:
            edition = self.get_edition(row)
            row["edition"] = edition
            sorted_dict[edition].append(row)
        return sorted_dict

    def get_edition(self, row: dict):
        remaster_start_year = self.intca("start_date")
        release = "0"
        errata = "0"
        if "release_date" in row.keys() and row["release_date"]:
            release = row["release_date"][0]
        if "latest_errata" in row.keys() and row["latest_errata"]:
            errata = row["latest_errata"][0]
        if self.find_year(release) >= remaster_start_year or self.find_year(errata) >= remaster_start_year:
            return "remaster"
        else:
            return "legacy"

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
                print("No main id in soup")
                return []
            try:
                link_list = [item.a for item in item_list if item.a is not None]
                return link_list
            except IndexError:
                print(item_list)
        print("No soup found, did you cook it?")
        return []

    def parse_source_links(self,
                           item_list: list[Tag],
                           source_name: str
                           ) -> (list[dict[str, str]]):
        category_data = []
        flags = []
        for item in item_list:
            name = self.clean_link_name(item.get_text())
            url = item['href']
            if not name or not url:
                continue
            item_dict = {"name": name, "url": url, "source": source_name}
            snake_item_category = url.split('.com/')[-1].split('.')[0]
            item_category = U.snake_to_under(snake_item_category)
            print(item_category)
            if "Group=" in url and item_category == "sources":
                item_category += "_group"
            if "General=true" in url:
                item_category += "_general"
            if item_category == "equipment" and item_dict["url"] in flags:
                continue

            item_dict["category"] = item_category
            if item_category in self.lica("", keys=True):
                item_dict["status"] = "ok"
            else:
                item_dict["status"] = "ko"
            flags.append(item_dict["url"])
            item_dict["url"] = self.sica("base_url") + url
            category_data.append(item_dict)
        print(category_data)
        return category_data

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
                if main and not isinstance(main, int):
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
                if isinstance(node, Tag):
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
