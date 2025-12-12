from x_finder.utils.parser import Parser


class RemasterParser(Parser):
    def __init__(self):
        super().__init__("nethys", "remaster")

    def old_parse_item(self, plate, debug=False, verbose=False):
        """Here we target the last div holding the title and the item content, extract data in the title then move
        to the start of the item content and call the read_soup method to extract the expected key, values.
        To make sure we find the title, ie the item's name and other expected data, we run a first method that
        should work if the data are found within the title tag. If not, the second method will recursively fill the
        missing parts with the text it finds.
        """
        if plate.soup:
            main = plate.soup.find(id="ctl00_RadDrawer1_Content_MainContent_DetailedOutput")
        else:
            print(f"No soup provided for {plate.name}.")
            return {}, {}
        if not main:
            print("Main is none")
            return {}, {}

        class Status:
            """
            Keep track of the parsing steps, given as argument to recursive method read_node to keep it sane
            """
            type = "broken_title"  # identifier for badly parsed/writen html
            family = False  # we expect nested items of the same category in this page
            ended = 0  # number of titles, to fill the right dict
            last_key = ""  # we parsed a key and are loading values if truthy,
            loaded_values = []  # values can be in many html nodes we stack, waiting for end or key identification

            def __init__(self, known_name, known_url):
                self.name = known_name
                self.url = known_url

        class Result:
            """
            Stores parsing results,  given as argument to and filled by recursive method read_node to keep it sane
            """
            parsed = []  # things seen but not kept, useful in debug to spot missed nested items
            titles = []  # once sorted, will match items we look for, list of dicts
            links = []  # links left aside
            tails = []  # hopefully empty, left-overs, empty strings

        status = Status(plate.name, plate.url)
        result = Result()
        ok_start = self.find_start_ok(main, status, result, plate.category, debug=debug, verbose=verbose)
        if ok_start is not None:
            self.read_soup(ok_start, status, result, plate.category, debug=debug, verbose=verbose)
        if ok_start is None or status.type == "ok_broken":
            main = plate.soup.find(id="ctl00_RadDrawer1_Content_MainContent_DetailedOutput")
            start_broken = self.find_start_broken(main, status, result, plate.category, debug=debug, verbose=verbose)
            if start_broken is not None:
                self.read_soup(start_broken, status, result, plate.category, debug=debug, verbose=verbose)

        parsed_rows = []
        nested_rows = {}
        trained = False
        if plate.category == "skills_general" and "(Trained)" in result.titles[0]["name"]:
            trained = True
        for title in result.titles:
            if " Trained Actions" in title["name"]:
                trained = True
            if title and len(title) > 3 and 'x_finder_model' in title.keys():
                current_category = title['x_finder_model']
                if current_category == plate.category:
                    if 'level' in result.titles[0].keys() and 'level' not in title.keys():
                        print(f"{title} should have a level and is discarded")
                        continue
                    parsed_rows.append(title)
                else:
                    related_item = result.titles[0]["name"].split('(')[0].strip()
                    title["x_finder_related_item"] = related_item
                    title["x_finder_related_model"] = plate.category
                    check_1 = current_category == "actions" and trained
                    check_2 = plate.category == "skills" or plate.category == "skills_general"
                    if check_1 and check_2:
                        if "prerequisite" not in title.keys():
                            title["prerequisite"] = []
                        if plate.category == "skills":
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
