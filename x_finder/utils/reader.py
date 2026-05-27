from typing import Literal
from bs4.element import Tag
from helpers.helpers import Status, Result
from utils import U
from mixins.ica import IcaMixin


class Reader(IcaMixin):
    def __init__(self):
        self.ica: dict[str, dict[str, str | list[str]]] = {}

    def read_soup(self,
                  child: Tag | None,
                  status: Status,
                  result: Result,
                  category: str,
                  debug: bool = False,
                  verbose: bool = False) -> None:
        if child is None:
            return None # block stop

        current_category = "default"
        if 'x_finder_model' in result.titles[status.ended].keys():
            current_category = result.titles[status.ended]['x_finder_model']  # the model name of the item (title)

        value = U.clean_text(child.get_text())   # we deal with the results we have before taking care of the data
        if current_category == "default":  # We are in an unidentified empty title we store on main item title[0] first
            store_time = child.name and child.name in self.lica("end_tags", category)
        else:  # We will store current or first, deciding witch in the store method
            store_time = child.name and child.name in self.lica("end_tags", current_category)
        steal_time = result.titles[status.ended]["name"] in ["Activate", "Melee", "Ranged"] and not status.loaded_values
        print(child.name, status.last_key, current_category, store_time, steal_time)
        if status.last_key and store_time:  # we have all the values for that key.
            if steal_time:                  # we steal the key if we have no value and are missing a title name.
                result.titles[status.ended]["name"] = value
            else:
                self.store(status, result, category, current_category)  # key not stolen, we store
                status.last_key = ""
                status.loaded_values = []

        key, tail = U.format_key(child)
        href = U.get_href(child)
        check_text_col = key and key in self.lica("text_columns", category) + self.lica("text_columns", current_category)
        hint = self.analyse_status_and_tag(child, status, current_category, key)  # return expected command in most case
        if debug:
            print(key, "tail:", tail, "hint:", hint, check_text_col)
        match hint:  # new round begins, what is the element about
            case "load":
                if value:
                    value = self.remove_description_from_value(status, result, value)
                    status.loaded_values.append(value)
            case "new_title":
                nested_category = self.find_nested_item_category(key, href, category=category)
                check_nest = nested_category is not None and nested_category not in ["default", "rules", category]
                check_key = check_text_col and not nested_category == "actions"
                print("family:", status.family, "nested_category:", nested_category)
                if check_key:
                    self.load_key_value(status, key, tail)
                elif check_nest:  # a nested category is identified
                    self.add_new_title(child, result, status, nested_category)
                elif status.family:  # this url holds several items of the same family
                    self.add_new_title(child, result, status, category)
                else:  # We open an empty title, setting current_category to default, useful to escape a nested category
                    self.add_new_title(child, result, status)
            case "new_key":
                nested_category = self.find_nested_item_category(key, href, child.next_sibling, category)
                check_nest = nested_category is not None and nested_category not in ["default", "rules", category, ""]
                if self.bica("nested", current_category) and check_text_col and check_nest:
                    if key in result.titles[status.ended].keys() and nested_category == current_category:
                        print("key stolen")
                        check_text_col = False

                index = self.get_check_column_index(status, key, category, current_category)
                print(f"nested category: {nested_category}, index:{index}")
                if index >= 0:  # we have a key that matches a column that stores a bool
                    result.titles[index][key] = True
                    if tail:
                        self.describe(value, result, index=index)
                elif check_nest and not check_text_col:  # check overload for key_terms?
                    self.add_new_title(child, result, status, category=nested_category)
                    if key in self.lica("text_columns", nested_category):
                        self.load_key_value(status, key, tail)
                else:  # normal behavior
                    self.load_key_value(status, key, tail)
            case "dive":
                next_child = child.next_element
                if next_child is not None:
                    try:
                        self.read_soup(child.next_element, status, result, category, debug=debug, verbose=verbose)
                    except StopIteration:
                        pass
            case "trait":
                if self.check_for_traits(child):
                    if "traits" not in result.titles[status.ended].keys():
                        result.titles[status.ended]['traits'] = []
                    result.titles[status.ended]['traits'].append(child.get_text())
                attributes = ["traitalignment", "traitsize", "action"]
                for attribute in attributes:
                    if attribute in child.attrs["class"]:  # legacy
                        result.titles[status.ended][attribute.split("trait")[-1]] = child.get_text().strip()
            case "table":
                key, name, description = self.find_table_key_name_and_description(status, result, child, category)

                if child.name == "table":
                    table = self.load_nested_table(child)
                else:
                    table = self.load_nested_table(child.find('table'))
                result.tables[key] = table
            case "describe":
                values = []
                if child.name in ['ol', 'ul']:
                    values = child.find_all('li')
                    values = [U.clean_text(value.get_text()) for value in values if value.get_text() is not None]
                else:
                    value = U.clean_text(child.get_text())
                    if value:
                        url = U.get_href(child)
                        if url:
                            related_model = U.snake_to_under(url.split('.')[0])
                            description = f"{value}: {related_model}"
                            self.describe(description, result, index=status.ended, link=True)
                            value = "<i>" + value + "</i>"
                        values.append(value)
                if values:
                    index = status.ended
                    if self.bica("no_description", current_category):
                        index = 0
                    if "description" not in result.titles[index].keys():
                        result.titles[index]["description"] = []
                    result.titles[index]["description"] += values
            case _:
                description = U.clean_text(child.get_text())
                if child.name == "h3" and self.find_nested_item_category(U.format_key(child)[0],
                                                                         U.get_href(child)
                                                                         ) == "actions":
                    self.add_new_title(child, result, status, "actions")
                elif description and child.name in ["h1", "h2"] and description.startswith("Table "):
                    child_name = child.get_text().replace('–', '-').replace(' ', '_').lower().strip()
                    title_name = result.titles[status.ended]["name"].replace('–', '-').replace(' ', '_').lower().strip()
                    if child_name != title_name:
                        self.add_new_title(child, result, status, current_category)
                else:
                    if child.name == "h2" and "Elite | Normal | Weak" in description:
                        description = ""
                    if child.name == "h1" and "cr" in self.lica("text_columns", category) and "Creature" in description:
                        result.titles[0]["cr"] = description.split("Creature")[-1]
                        description = ""
                    if description:
                        if child.name in ["h1", "h2", "h3", "h4"]:
                            description = "<b>" + description + "</b>"
                        index = status.ended
                        if self.bica("no_description", current_category):
                            index = 0
                        self.describe(description, result, index=index)

        if child:
            next_child = child.next_sibling
            if next_child:
                try:
                    self.read_soup(child.next_sibling,
                                   status,
                                   result,
                                   category,
                                   debug=debug,
                                   verbose=verbose)
                except RecursionError:
                    pass
                except StopIteration:
                    pass
                return None
        if status.last_key and status.loaded_values:
            self.store(status, result, category, current_category)
        return None

    def add_new_title(self,
                      child: Tag,
                      result: Result,
                      status: Status,
                      category: str = None,
                      name: str = None
                      ) -> None:
        """
        :param child: NavigableString the HTML child we try to extract data from
        :param result: the instance of Result for the current item
        :param status: the instance of Status that keeps track of the steps during item parsing
        :param category: String, the category of the item we parse
        :param name: String, the name of the item
        :return: nothing
        """

        title = child.get_text(separator=',')
        if title:
            status.ended += 1
            result.titles.append({})

            title_parts = title.strip(',; ').split(',')
            if name is None:
                name = title_parts[0]
            result.titles[status.ended]["name"] = name

            if child.name == 'a' and child['href'] and child['href'] not in [None, "PFS.aspx"]:
                url = child['href']
            elif child.name and child.find('a') and child.find('a')['href'] not in [None, "PFS.aspx"]:
                url = child.find('a')['href']
            elif status.family:
                url = result.titles[0]["url"]
            else:
                url = ""
            result.titles[status.ended]["url"] = url

            if category is None:
                category = self.find_nested_item_category(name, url)
            if category:
                result.titles[status.ended]['x_finder_model'] = category
                if result.titles[status.ended]["url"] == "":
                    result.titles[status.ended]["url"] = status.url

            if 'level' not in result.titles[status.ended].keys():
                if 'level' in self.lica("text_columns", category) and len(title_parts) > 1:
                    result.titles[status.ended]['level'] = title_parts[-1]
            if 'action' in self.lica("text_columns", category):
                try:
                    result.titles[status.ended]['action'] = child.find('span').get_text(' ,;')
                except AttributeError:
                    pass
            if category == "monster_abilities":
                subtype = "general"
                if "ac" in result.titles[0].keys():
                    subtype = "defensive"
                if "speed" in result.titles[0].keys():
                    subtype = "offensive"
                result.titles[status.ended]["subtype"] = subtype

    def find_nested_item_category(self,
                                  name: str,
                                  url: str,
                                  next_child: Tag | None = None,
                                  category: str = "default"
                                  ) -> str:
        name = name.lower().strip('()[]').replace(' ', '_').replace('-', '_')
        check_category = category in ["causes", "doctrines", "research_field"]
        if check_category:
            return "class_optional_features"
        if category == "classes":
            if "key_terms" in name:
                return "key_terms"
            return "class_features"
        if category == "monsters":
            if name in ["melee", "ranged"]:
                return "monster_attacks"
            if "spells" in name:
                return "monster_spells"
            return "monster_abilities"
        if name in ["melee", "ranged"]:
            return "animal_attacks"
        if name == "activate":
            return "equipment_activations"
        if name.endswith("_tasks") and name.startswith("sample_"):
            return "sample_tasks"
        name_model = self.sica("", name)
        url_base = url.split('.')[0].strip().lower().replace(' ', '_').replace('-', '_')
        url_model = self.sica("", url_base)
        if next_child is not None and next_child.get_text():
            next_text = next_child.get_text()
            if next_text:
                next_text = next_text.lower().strip(' (),;').replace(' ', '_')
                if not next_text.endswith('s'):
                    next_text += 's'
                next_model = self.sica("", next_text)
                if next_model not in ["rules", "default", None]:
                    return next_model
        if url_model is not None and url_model not in ["rules", "default", None, ""]:
            return url_model
        if name_model is not None  and name_model and name_model != "default":
            return name_model
        return "default"

    @staticmethod
    def check_for_traits(child: Tag):
        check = False
        for trait in ["trait", "traituncommon", "traitrare", "traitunique"]:
            if trait in child.attrs["class"]:
                check = True
        return check

    @staticmethod
    def describe(description: str, result: Result,  index: int = 0, link: bool = False) -> None:
        key = "description"
        if link:
            key += "_links"
        if key not in result.titles[index].keys():
            result.titles[index][key] = []
        result.titles[index][key].append(description)

    def analyse_status_and_tag(self,
                               child: Tag,
                               status: Status,
                               current_category: str,
                               key: str
                               ) -> Literal["load", "new_title", "new_key", "store", "trait", "action",
                                            "describe", "table", "dive", "next", ""]:
        if status.last_key:
            return "load"  # we have a key from a previous child, we look for its values
        if child.name:
            if child.name in self.lica("title_tags", current_category):
                if key:
                    return "new_title"  # so we no longer have a key we look for nested items or family of items
                return ""
            if child.name in self.lica("start_tags", current_category):
                if key:
                    return "new_key"  # or for a key
            if child.name == 'span' and "class" in child.attrs:
                if "hanging-indent" in child.attrs["class"]:
                    return "dive"
                return "trait"  # many to many data, like promos certifications etc...;
            if child.name == "table":
                return "table"  # that we will more securely crawl in sub method
            if child.name in ["div", "details"] or "rules%" in child.name:
                return "dive"  # we will recursively parse its children
        if child.name is None and key in self.lica("text_columns", current_category):
            return "new_key"
        if child.name in self.lica("desc_tags", current_category) or child.name is None:
            return "describe"
        return ""

    @staticmethod
    def find_table_key_name_and_description(status: Status, result: Result, child: Tag, category: str) -> tuple[str]:
        key, name, description = [""] * 3
        if result.titles[status.ended]["name"].startswith('Table '):
            key, name = U.format_key(text=result.titles[status.ended]["name"])
            description = result.titles[status.ended].pop("description", "")
            result.titles = result.titles[:status.ended]
            status.ended -= 1
        elif child.find('summary') is not None:
            key, name = U.format_key(child.find('summary'))
        else:
            key, name = U.format_key(text=result.titles[status.ended]["name"])
            key = "table_" + key
        if key is None:
            key = f"table_{result.titles[0]['name']}"

        if key in result.titles[0].keys():
            if category == "classes" and "spells" in result.titles[0].keys():
                test = f"table_spells_per_day_{result.titles[0]['name']}"
                if test not in result.titles[0].keys():
                    return test, name, description
        if key in result.titles[0].keys():
            for i in range(1, 50):
                test = key + "_" + str(i)
                if test not in result.titles[0].keys():
                    return test, name, description
        return key, name, description

    def remove_description_from_value(self, status, result, value: str) -> str:
        if status.last_key == "level" and len(value) > 3:
            values = value.split('. ')
            value = values[0]
            if len(values) > 1:
                description = ". ".join(values[1:])
                self.describe(description, result, index=status.ended)
        return value

    def get_check_column_index(self, status: Status, key: str, category: str, current_category: str) -> int:
        index = -1
        if key in self.lica("check_columns", current_category):  # the value of a check col is True if key
            index = status.ended
        if key in self.lica("check_columns", category):
            index = 0
        return index

    @staticmethod
    def load_key_value(status: Status, key: str, value: str | None):
        status.last_key = key.lower()
        status.loaded_values = []
        if value:
            status.loaded_values.append(value)

    def store(self, status: Status, result: Result, category: str, current_category: str) -> None:
        key = status.last_key
        values = [value for value in status.loaded_values if value.replace('\\n', '').strip()]
        print(f"storing {key}: {values}")
        match = False
        text_cols = self.lica("text_columns", category)
        if key in text_cols or key.split('_')[0] in text_cols:
            match = True
            print("match")
            if key not in result.titles[0].keys() and (not status.family or category != current_category):
                result.titles[0][key] = values
                print("stored in 0")
                return
        current_text_cols = self.lica("text_columns", current_category)
        if key in current_text_cols:
            if key not in result.titles[status.ended].keys():
                result.titles[status.ended][key] = values
                return
        if match and key == 'advanced_domain_spell':
            if 'advanced_apocryphal_domain_spell' not in result.titles[0].keys():
                result.titles[0]['advanced_apocryphal_domain_spell'] = values
                return
        if match and isinstance(result.titles[0][key], list):
            result.titles[0][key] += values
            return
        if 'x_finder_model' in result.titles[status.ended].keys():
            name = result.titles[status.ended]["name"].lower().replace(' ', '_')
            if name == result.titles[status.ended]['x_finder_model']:
                result.titles[status.ended]["name"] = key
            category = result.titles[status.ended]['x_finder_model']
            nested_cols = self.lica("nested_columns", category)
            if nested_cols:
                nested_number = len(nested_cols) // 2
                for i in range(nested_number):
                    if nested_cols[2 * i] not in result.titles[status.ended].keys() and values:
                        result.titles[status.ended][nested_cols[2 * i]] = key
                        result.titles[status.ended][nested_cols[2 * i + 1]] = values
                        return
        if self.bica("overload", current_category):
            result.titles[status.ended][key] = values
            return
        print(f"parsed {key} : {values}")
        result.parsed.append({key: values})

    @staticmethod
    def load_nested_table(child: Tag):
        headers = []
        table = []
        row_len = 0
        head = child.find('th')
        if head:
            headers = [td.get_text() for td in head.find_all('td')]
        for tr in child.find_all('tr'):
            cells = tr.find_all('td')
            if not headers:
                headers = [cell.get_text() for cell in cells]
            else:
                row = {header: cell.get_text() for header, cell in zip(headers, cells)}
                row_len = max(row_len, len(row))
                table.append(row)
        return table

