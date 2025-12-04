from utils import U
from args import Ica, item_category_arguments as ica


class Parser:

    def __init__(self, target, edition):
        self.target = target
        self.edition = edition

    @staticmethod
    def get(argument, category="default", keys=False):
        return Ica.get(ica, argument, category, keys)

    def parse_item(self, plate, name="", url="", category="default", debug=False, verbose=False):
        """Here we target the last div holding the title and the item content, extract data in the title then move
        to the start of the item content and call the read_soup method to extract the expected key, values.
        To make sure we find the title, ie the item's name and other expected data, we run a first method that
        should work if the data are found within the title tag. If not, the second method will recursively fill the
        missing parts with the text it finds.
        """
        if plate.soup:
            main = plate.soup.find(id="ctl00_RadDrawer1_Content_MainContent_DetailedOutput")
        else:
            print(f"No soup provided for {category}.")
            return {}, {}
        if not main:
            print("Main is none")
            return {}, {}

        class Status:
            type = "broken_title"
            family = False
            ended = 0
            last_key = ""
            loaded_values = []

            def __init__(self, known_name, known_url):
                self.name = known_name
                self.url = known_url

        class Result:
            parsed = []
            titles = []
            links = []
            tails = []

        status = Status(name, url)
        result = Result()
        ok_start = self.find_start_ok(main, status, result, category, debug=debug, verbose=verbose)
        if ok_start is not None:
            self.read_soup(ok_start, status, result, category, debug=debug, verbose=verbose)
        if ok_start is None or status.type == "ok_broken":
            main = plate.soup.find(id="ctl00_RadDrawer1_Content_MainContent_DetailedOutput")
            start_broken = self.find_start_broken(main, status, result, category, debug=debug, verbose=verbose)
            if start_broken is not None:
                self.read_soup(start_broken, status, result, category, debug=debug, verbose=verbose)

        parsed_rows = []
        nested_rows = {}
        trained = False
        if category == "skills_general" and "(Trained)" in result.titles[0]["name"]:
            trained = True
        for title in result.titles:
            if " Trained Actions" in title["name"]:
                trained = True
            if title and len(title) > 3 and 'x_finder_model' in title.keys():
                current_category = title['x_finder_model']
                if current_category == category:
                    if 'level' in result.titles[0].keys() and 'level' not in title.keys():
                        print(f"{title} should have a level and is discarded")
                        continue
                    parsed_rows.append(title)
                else:
                    related_item = result.titles[0]["name"].split('(')[0].strip()
                    title["x_finder_related_item"] = related_item
                    title["x_finder_related_model"] = category
                    check_1 = current_category == "actions" and trained
                    check_2 = category == "skills" or category == "skills_general"
                    if check_1 and check_2:
                        if "prerequisite" not in title.keys():
                            title["prerequisite"] = []
                        if category == "skills":
                            title["prerequisite"].append(f"Trained in {related_item}")
                        else:
                            title["prerequisite"].append("Trained in related skill")

                    if self.get("nested", current_category):
                        if current_category in "actions":
                            if "action" not in title.keys() and "traits" not in title.keys():
                                continue
                        if current_category not in nested_rows.keys():
                            nested_rows[current_category] = []
                        nested_rows[current_category].append(title)

        if verbose:
            for title in result.titles:
                if (title and len(title) > 3 and 'x_finder_model' in title.keys()) or debug:
                    print(title)
            if result.parsed:
                print(result.titles[0]["name"], result.parsed)
            if result.tails:
                print(result.titles[0]["name"], result.tails)
        if debug or verbose:
            for title in parsed_rows:
                print(title)
            for key in nested_rows.keys():
                print(nested_rows[key])

        return parsed_rows, nested_rows

    def find_start(self, plate, status, result, debug=False, verbose=False):
        title = plate.soup.find('h1')
        if title is not None and title.get_text():
            title_content = title.get_text(separator=',').split(',')
            result.titles.append({"name": title_content[0].strip(' ,;'),
                                  "x_finder_model": category})
            if title.a is not None and title.a['href'] is not None:
                result.titles[0]["url"] = title.a['href']
                status.type = "ok"
            if "level" in self.get("text_columns", category):
                level = title_content[-1].strip(' ,;')
                if level:
                    result.titles[0]["level"] = level
                if level.endswith('+'):
                    status.family = True
            if verbose:
                print(f"Start found for {'family' if status.family else 'item'}: {result.titles[0]['name']}")
                print(f"on h1: {title}")
            return title.next_sibling
        if debug or verbose:
            print(f"Start not found for h1: {status.name}")
        status.type = "broken"
        return None

    def parse_source_links(self, item_list):
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

    def find_start_ok(self, soup, status, result, category, debug=False, verbose=False):
        if soup:
            title = soup.find('h1')
            if title is not None and title.get_text():
                title_content = title.get_text(separator=',').split(',')
                result.titles.append({"name": title_content[0].strip(' ,;'),
                                      "x_finder_model": category})
                if title.a is not None and title.a['href'] is not None:
                    result.titles[0]["url"] = title.a['href']
                    status.type = "ok"
                else:
                    result.titles[0]["url"] = status.url
                    status.type = "ok_broken"
                if "level" in self.get("text_columns", category):
                    level = title_content[-1].strip(' ,;')
                    if level:
                        result.titles[0]["level"] = level
                    if level.endswith('+'):
                        status.family = True
                if verbose:
                    print(f"Start found for {'family' if status.family else 'item'}: {result.titles[0]['name']}")
                    print(f"on h1: {title}")
                return title.next_sibling
        if debug or verbose:
            print(f"Start not found for h1: {status.name}")
        status.type = "broken"
        return None

    def find_start_broken(self, content, status, result, category, debug=False, verbose=False):
        """
        """
        broken_title = content
        if broken_title is not None and broken_title.name in [None, 'a']:
            url = status.url
            if broken_title.name == 'a':
                status.type = "broken_title"
                name = broken_title.get_text().strip(' ,;')
                url = broken_title["href"]
            elif broken_title.name is None and len(broken_title) > 2:
                status.type = "broken_link"
                name = str(broken_title).strip(' ,;')
            else:
                return self.find_start_broken(broken_title.next_sibling,
                                              status,
                                              result,
                                              category,
                                              debug=debug,
                                              verbose=verbose)
            if name:
                result.titles.append({"name": name,
                                      "url": url,
                                      "x_finder_model": category})
                status.ended = len(result.titles) - 1
                if debug:
                    print(f"found name {name} for {broken_title}")
                title_end = broken_title.next_sibling
                if not title_end or title_end.name is None and not title_end.get_text().strip(' '):
                    title_end = title_end.next_sibling
                if title_end and title_end.name == "span":
                    text = title_end.get_text().strip(' ,;')
                    if "action" in text:
                        result.titles[status.ended]['action'] = text
                        title_end = title_end.next_sibling
                    if title_end and title_end.name == "span" and 'level' in self.get("text_columns", category):
                        level = title_end.get_text().strip(' ,;')
                        result.titles[status.ended]['level'] = level
                        if level.endswith('+'):
                            status.family = True
                            if verbose:
                                print(f"Secondary title: {result.titles[status.ended]} found for {result.titles[0]}")
                        return title_end.next_sibling
                return title_end
        else:
            if broken_title is None:
                if debug or verbose:
                    print(f"unable to find members of family {result.titles}")
                return None
            return self.find_start_broken(broken_title.next_sibling,
                                          status,
                                          result,
                                          category,
                                          debug=debug,
                                          verbose=verbose)

    def add_new_title(self, child, result, status, category=None, name=None):
        """
        :param child: NavigableString the html child we try to extract data from
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
                if 'level' in self.get("text_columns", category) and len(title_parts) > 1:
                    result.titles[status.ended]['level'] = title_parts[-1]
            if 'action' in self.get("text_columns", category):
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

    def find_nested_item_category(self, name, url, next_child=None, category="default"):
        name = name.lower().strip('()[]').replace(' ', '_').replace('-', '_')
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
        name_model = self.get("", name)
        url_base = url.split('.')[0].strip().lower().replace(' ', '_').replace('-', '_')
        url_model = self.get("", url_base)
        if next_child is not None and next_child.get_text():
            next_text = next_child.get_text()
            if next_text:
                next_text = next_text.lower().strip(' (),;').replace(' ', '_')
                if not next_text.endswith('s'):
                    next_text += 's'
                next_model = self.get("", next_text)
                if next_model not in ["rules", "default"]:
                    return next_model
        if url_model is not None and url_model not in ["rules", "default"]:
            return url_model
        if name_model != "default":
            return name_model
        return ""

    def read_soup(self, child, status, result, category, debug=False, verbose=False):
        if child is None:
            return
        current_category = "default"
        if 'x_finder_model' in result.titles[status.ended].keys():
            current_category = result.titles[status.ended]['x_finder_model']

        if status.last_key:
            if child.name and child.name in self.get("cell_ends", current_category):
                if result.titles[status.ended]["name"] in ["Activate", "Melee", "Ranged"] and not status.loaded_values:
                    result.titles[status.ended]["name"] = U.clean_text(child.get_text())
                else:
                    self.store(status, result, category, current_category)
                    status.last_key = ""
                    status.loaded_values = []
        if status.last_key:
            value = U.clean_text(child.get_text())
            if value and child.name is None or child.name not in self.get("cell_ends", current_category):
                if status.last_key == "level" and len(value) > 3:
                    values = value.split('. ')
                    value = values[0]
                    if len(values) > 1:
                        description = ". ".join(values[1:])
                        if "description" not in result.titles[status.ended].keys():
                            result.titles[status.ended]["description"] = []
                        result.titles[status.ended]["description"].append(description)
                status.loaded_values.append(value)

        elif child.name and child.name in self.get("next_titles", current_category):
            key, value = U.format_key(child)
            href = U.get_href(child)
            if key:
                test_category = self.find_nested_item_category(key, href)
                check = key in self.get("text_columns", category) + self.get("text_columns", current_category)
                if check and not test_category == "actions":
                    status.last_key = key
                    status.loaded_values = []
                    if value:
                        status.loaded_values.append(value)
                elif test_category is not None and test_category not in ["default", "rules", category]:
                    self.add_new_title(child, result, status, test_category)
                elif status.family:
                    self.add_new_title(child, result, status, category)
                elif category == "classes":
                    if child.name != "h3":
                        self.add_new_title(child, result, status, "class_features")
                    else:
                        status.last_key = key
                elif category in ["causes", "doctrines", "research_field"]:
                    if child.name != "h3":
                        self.add_new_title(child, result, status, "class_optional_features")
                    else:
                        status.last_key = key
                else:
                    self.add_new_title(child, result, status)

        elif child.name and child.name in self.get("cell_starts", current_category):
            key, value = U.format_key(child)
            if key:
                check_text = key in self.get("text_columns", category) + self.get("text_columns", current_category)
                check_current = key in self.get("check_columns", current_category)
                check_base = key in self.get("check_columns", category)
                test_category = self.find_nested_item_category(key, U.get_href(child), child.next_sibling, category)
                if self.get("nested", current_category) and check_text:
                    if key in result.titles[status.ended].keys() and test_category == current_category:
                        check_text = False
                index = -1
                if check_current:
                    index = status.ended
                if check_base:
                    index = 0
                if index >= 0:
                    result.titles[index][key] = True
                    if "description" not in result.titles[index].keys():
                        result.titles[index]["description"] = []
                    result.titles[index]["description"].append(key)
                elif not check_text and test_category and test_category not in [category, "rules", "default"]:
                    self.add_new_title(child, result, status, category=test_category)
                    if key in self.get("text_columns", test_category):
                        status.last_key = key.lower()
                        status.loaded_values = []
                        if value:
                            status.loaded_values.append(value)
                else:
                    status.last_key = key.lower()
                    status.loaded_values = []
                    if value:
                        status.loaded_values.append(value)

        elif child.name == 'span':
            if "class" in child.attrs:
                check = False
                for trait in ["trait", "traituncommon", "traitrare", "traitunique"]:
                    if trait in child.attrs["class"]:
                        check = True
                if check:
                    if "traits" not in result.titles[status.ended].keys():
                        result.titles[status.ended]['traits'] = []
                    result.titles[status.ended]['traits'].append(child.get_text())
                if "traitalignment" in child.attrs["class"]:
                    result.titles[status.ended]['alignement'] = child.get_text().strip()
                if "traitsize" in child.attrs["class"]:
                    result.titles[status.ended]['size'] = child.get_text().strip()
                if "hanging-indent" in child.attrs["class"]:
                    next_child = next(child.children)
                    if next_child:
                        self.read_soup(next_child, status, result, category, debug=debug, verbose=verbose)

        elif child.name is None or child.name in self.get("desc_tags", current_category):
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
                        if "description_links" not in result.titles[status.ended].keys():
                            result.titles[status.ended]["description_links"] = []
                        result.titles[status.ended]["description_links"].append(f"{value}: {related_model}")
                        value = "<i>" + value + "</i>"
                    values.append(value)
            if values:
                index = status.ended
                if self.get("no_description", current_category):
                    index = 0
                if "description" not in result.titles[index].keys():
                    result.titles[index]["description"] = []
                result.titles[index]["description"] += values

        elif child.name == "table" or child.name == "details" and 'table' in [c.name for c in child.children]:
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

            if child.name == "table":
                table = U.load_nested_table(child)
            else:
                table = U.load_nested_table(child.find('table'))

            if key and key not in result.titles[0].keys():
                result.titles[0][key] = {"name": name, "description": description, "table": table}
            else:
                if key is None:
                    key = f"table_{result.titles[0]['name']}"
                if category == "classes" and "spells" in result.titles[0].keys():
                    key = f"table_spells_per_day_{result.titles[0]['name']}"
                    result.titles[0][key] = {"name": name, "description": description, "table": table}
                else:
                    for i in range(1, 50):
                        test = key + "_" + str(i)
                        if test not in result.titles[0].keys():
                            result.titles[0][test] = {"name": name, "description": description, "table": table}
                            break

        elif child.name in ["div", "details", "br"] or "rules%" in child.name:
            children = child.children
            try:
                next_child = next(children)
                if next_child:
                    self.read_soup(next_child, status, result, category, debug=debug, verbose=verbose)
            except StopIteration:
                pass

        elif child.name == "h3" and self.find_nested_item_category(U.format_key(child)[0],
                                                                   U.get_href(child)
                                                                   ) == "actions":
            self.add_new_title(child, result, status, "actions")

        elif child.get_text().strip(' ,;'):
            if child.name in ["h1", "h2"] and child.get_text().startswith("Table "):
                child_name = child.get_text().replace('–', '-').replace(' ', '_').lower().strip()
                title_name = result.titles[status.ended]["name"].replace('–', '-').replace(' ', '_').lower().strip()
                if child_name != title_name:
                    self.add_new_title(child, result, status, current_category)
            else:
                text = U.clean_text(child.get_text())
                if child.name == "h2" and "Elite | Normal | Weak" in text:
                    text = ""
                if child.name == "h1" and "cr" in self.get("text_columns", category) and "Creature" in text:
                    result.titles[0]["cr"] = text.split("Creature")[-1]
                    text = ""
                if text:
                    if child.name in ["h1", "h2", "h3", "h4"]:
                        text = "<b>" + text + "</b>"
                    index = status.ended
                    if self.get("no_description", current_category):
                        index = 0
                    if "description" not in result.titles[index].keys():
                        result.titles[index]["description"] = []
                    result.titles[index]["description"].append(text)
        if child:
            next_child = child.next_sibling
            if next_child:
                try:
                    return self.read_soup(next_child, status, result, category, debug=debug, verbose=verbose)
                except RecursionError:
                    pass
        if status.last_key and status.loaded_values:
            self.store(status, result, category, current_category)
        return None

    def store(self, status, result, category, current_category):
        match = False
        text_cols = self.get("text_columns", category)
        if status.last_key in text_cols or status.last_key.split('_')[0] in text_cols:
            match = True
            if status.last_key not in result.titles[0].keys() and (not status.family or category != current_category):
                result.titles[0][status.last_key] = status.loaded_values
                return
        current_text_cols = self.get("text_columns", current_category)
        if status.last_key in current_text_cols:
            if status.last_key not in result.titles[status.ended].keys():
                result.titles[status.ended][status.last_key] = status.loaded_values
                return
        if match and status.last_key == 'advanced_domain_spell':
            if 'advanced_apocryphal_domain_spell' not in result.titles[0].keys():
                result.titles[0]['advanced_apocryphal_domain_spell'] = status.loaded_values
                return
        if match and isinstance(result.titles[0][status.last_key], list):
            result.titles[0][status.last_key] += status.loaded_values
            return
        if 'x_finder_model' in result.titles[status.ended].keys():
            name = result.titles[status.ended]["name"].lower().replace(' ', '_')
            if name == result.titles[status.ended]['x_finder_model']:
                result.titles[status.ended]["name"] = status.last_key
            category = result.titles[status.ended]['x_finder_model']
            nested_cols = self.get("nested_pairs", category)
            if nested_cols:
                nested_number = len(nested_cols) // 2
                for i in range(nested_number):
                    if nested_cols[2 * i] not in result.titles[status.ended].keys() and status.loaded_values:
                        result.titles[status.ended][nested_cols[2 * i]] = status.last_key
                        result.titles[status.ended][nested_cols[2 * i + 1]] = status.loaded_values
                        return
        if self.get("overload", current_category):
            result.titles[status.ended][status.last_key] = status.loaded_values
            return
        result.parsed.append({status.last_key: status.loaded_values})
