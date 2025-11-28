import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from django.conf import settings
from os import makedirs
from pathlib import Path
from x_finder.utils.fixtures.args import item_category_arguments as ica
import pandas as pd
from bs4 import BeautifulSoup

item_category_arguments = ica
BASE_DIR = Path(__file__).resolve().parent.parent


def run_sql(sql):
    c = psycopg2.connect(database=settings.DATABASES['default']['USER'],
                         user=settings.DATABASES['default']['USER'],
                         password=settings.DATABASES['default']['PASSWORD'])
    c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = c.cursor()
    cur.execute(sql)
    c.close()


class U:
    @staticmethod
    def get(argument, category="default"):
        if not argument and category in item_category_arguments.keys():
            return category
        if category in item_category_arguments.keys() and argument in item_category_arguments[category].keys():
            return item_category_arguments[category][argument]
        if argument in item_category_arguments["default"].keys():
            return item_category_arguments["default"][argument]
        return None

    @staticmethod
    def save(df, name, directory=None, app="utils"):
        path = f"{BASE_DIR}\\{app}\\fixtures\\csv\\"
        if directory:
            path += f"{directory}\\"
        makedirs(path, exist_ok=True)
        df.to_csv(f"{path}\\{name}.csv", sep='|', index=False)
        print(f"{name} successfully saved at {path}.")

    @staticmethod
    def load(name, app="utils", directory=None, suffix="raw"):
        if directory:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{directory}\\{name}_{suffix}.csv"
        else:
            pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{name}{suffix}.csv"
        df = pd.read_csv(pathfile, delimiter="|")
        return df

    @staticmethod
    def snake_to_under(snake):
        if snake is None or not isinstance(snake, str) or len(snake) == 0:
            print("No string to convert")
            return snake
        under = snake[0].lower()
        if len(snake) == 1:
            return under
        for char in snake[1:]:
            if char.isupper() or char == " ":
                under += "_"
            under += char.lower()
        return under

    @staticmethod
    def extract_model_dfs(df, key):
        columns = U.get("model_columns", key)
        excluded = U.get("model_excluded_columns", key)
        if excluded:
            columns = [column for column in df.columns if column not in excluded]
        else:
            columns = [column for column in columns if column in df.columns]

        for column in columns:
            join = " "
            if column.endswith("s"):
                join = "; "
            df[column] = df.apply(
                lambda r: join.join(r[column]) if isinstance(r[column], list) else r[column],
                axis=1)
        if "description_links" in df.columns and "description" in columns:
            columns.append("description_links")
        model_df = df[columns]
        return model_df

    @staticmethod
    def load_nested_table(child):
        table = []
        row_len = 0
        for tr in child.find_all('tr'):
            row = [td.get_text() for td in tr.find_all('td')]
            if len(row) < row_len:
                row = [table[-1][0]] + row
            row_len = max(row_len, len(row))
            table.append(row)
        return table

    @staticmethod
    def clean_text(raw_string):
        if raw_string is None:
            return ''
        text = raw_string.strip(' ,;\n\r—').replace('\xa0', ' ')
        if text.startswith(']'):
            if len(text) < 2:
                return ''
            text = text[1:]
        if text.endswith('['):
            text = text[:-1]
        return text

    @staticmethod
    def format_key(nav_string=None, text=""):
        if nav_string is not None:
            key = nav_string.get_text()
        else:
            key = text
        separator = '('
        if ':' in key:
            separator = ':'
        key_parts = key.split(separator)
        key = key_parts[0].strip(' ,;').lower().replace(' ', '_').replace('-', '_').replace('–', '_')
        value = ""
        if len(key_parts) > 1:
            value = separator.join(key_parts[1:]).strip(' ,;)')
        return key, value

    @staticmethod
    def get_href(child):
        if child is None:
            return ""
        if child.name == 'a':
            return child["href"]
        try:
            link = child.find('a')["href"]
            return link
        except AttributeError:
            return ""
        except TypeError:
            return ""

    @staticmethod
    def parse_source_links(item_list):
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

            if item_category not in item_category_arguments.keys():
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

    @staticmethod
    def clean(value):
        clean_value = BeautifulSoup(value, 'html.parser')
        if clean_value:
            text = clean_value.get_text()
            return text.strip()

    @staticmethod
    def find_nested_item_category(name, url, next_child=None, category="default"):
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
        name_model = U.get("", name)
        url_base = url.split('.')[0].strip().lower().replace(' ', '_').replace('-', '_')
        url_model = U.get("", url_base)
        if next_child is not None and next_child.get_text():
            next_text = next_child.get_text()
            if next_text:
                next_text = next_text.lower().strip(' (),;').replace(' ', '_')
                if not next_text.endswith('s'):
                    next_text += 's'
                next_model = U.get("", next_text)
                if next_model not in ["rules", "default"]:
                    return next_model
        if url_model is not None and url_model not in ["rules", "default"]:
            return url_model
        if name_model != "default":
            return name_model
        return ""

    @staticmethod
    def format_column_name(name, url=False):
        header = name.replace(" ", "_").replace("-", "_")
        if url and "url" not in header:
            header += "_url"
        return header.lower()

    @staticmethod
    def load_fixture(app, name, raw=True):
        suffix = ""
        if raw:
            suffix = "_raw"
        pathfile = f"{BASE_DIR}\\{app}\\fixtures\\csv\\{name}{suffix}.csv"
        df = pd.read_csv(pathfile, delimiter="|")
        return df

    @staticmethod
    def translate_date(mm_dd_yyyy):
        if '/' in mm_dd_yyyy:
            parts = mm_dd_yyyy.split('/')
        elif '-' in mm_dd_yyyy:
            parts = mm_dd_yyyy.split('-')
        else:
            return None
        return f"{parts[-1]}-{parts[0]}-{parts[1]}"

    @staticmethod
    def find_table(soup):
        table = soup.find(id="main").find('table')
        table_rows = table.find_all('tr')
        if table_rows:
            try:
                headers = [U.format_column_name(th.get_text()) for th in table_rows[0].find_all('th')]
            except TypeError:
                headers = []
                print("headers not found")
            if len(table_rows) > 0:
                return table_rows[1:], headers
        print("Table not found")
        return [], []

    @staticmethod
    def find_traits(data, separator="!", singles=("traituncommon", "traitrare", "traitunique", ), multiple="trait"):
        """Traits have a many-to-many relationship with most of the items we scrap. We need to extract their name.
        Arg: BS4 String
        Return : String """
        traits = []
        for single in singles:
            traits += [data.find(class_=single)]
        traits += data.find_all(class_=multiple)
        return separator.join([trait.a.get_text() for trait in traits if trait is not None])

    @staticmethod
    def find_level(data):
        """We target 1 text in a broken title supposed to hold the 'Level' of the target item as subtitle.
        Arg: data: String like object
        Return : String or None if str(data) is None
        """
        try:
            sub_title = str(data).split('<span style="margin-left:auto; margin-right:0">')[-1].split('</span>')[0]
            return sub_title
        except TypeError:
            return None

    @staticmethod
    def store(status, result, category, current_category):
        match = False
        text_cols = U.get("text_columns", category)
        if status.last_key in text_cols or status.last_key.split('_')[0] in text_cols:
            match = True
            if status.last_key not in result.titles[0].keys() and (not status.family or category != current_category):
                result.titles[0][status.last_key] = status.loaded_values
                return
        current_text_cols = U.get("text_columns", current_category)
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
            nested_cols = U.get("nested_pairs", category)
            if nested_cols:
                nested_number = len(nested_cols) // 2
                for i in range(nested_number):
                    if nested_cols[2 * i] not in result.titles[status.ended].keys() and status.loaded_values:
                        result.titles[status.ended][nested_cols[2 * i]] = status.last_key
                        result.titles[status.ended][nested_cols[2 * i + 1]] = status.loaded_values
                        return
        if U.get("overload", current_category):
            result.titles[status.ended][status.last_key] = status.loaded_values
            return
        result.parsed.append({status.last_key: status.loaded_values})

    @staticmethod
    def get_ability_modifier(boosts, flaw, ability):
        print(flaw, boosts)
        if isinstance(flaw, list) and ability in "".join(flaw).lower():
            print(flaw)
            return -2
        if isinstance(boosts, list):
            for boost in boosts:
                print(boosts)
                if boost and ability in boost.lower():
                    return 2
        return 0

    @staticmethod
    def split_column(df, name, new_names, strip=' ', step=1):
        if name in df.columns and new_names:
            for i in range(len(new_names)):
                new_name = new_names[i]
                df[new_name] = df.apply(
                    lambda r: r[name][i * step + 1].strip(strip) if r[name] and isinstance(r[name], list) else '',
                    axis=1)
            if step == 1:
                df[name] = df.apply(
                    lambda r: r[name][0].strip(strip) if r[name] and isinstance(r[name], list) else '',
                    axis=1)

    @staticmethod
    def split_text_column(df, name, new_name, separator=' ', strip=' '):
        if name in df.columns and new_name:
            df[new_name] = df.apply(
                    lambda r: r[name].split(separator)[1].strip(strip) if r[name] and separator in r[name] else '',
                    axis=1)
            df[name] = df.apply(
                    lambda r: r[name].split(separator)[0].strip(strip) if r[name] and separator in r[name] else '',
                    axis=1)

    @staticmethod
    def numerize_level(level):
        if level is None:
            return 0
        if isinstance(level, list):
            level = level[0]
        if isinstance(level, int):
            return level
        level = str(level)
        if level:
            level = str(level).split(' ')[-1].strip()
            if level.isnumeric():
                return int(level)
            if level.endswith('+'):
                return -1
        return 0

    @staticmethod
    def norm_df(df, key, source_name):
        subtype = U.get("subtype", key)
        name_nest = U.get("name_nest", key)
        U.split_text_column(df, "name", name_nest, separator='[', strip=' ]')
        U.split_text_column(df, "name", subtype, separator='(', strip=' )')
        if "level" in df.columns:
            if "spell_type" in U.get("text_columns", key):
                df["spell_type"] = df.apply(
                    lambda r: r["level"].split(' ')[0].strip(),
                    axis=1)
            df["level"] = df.apply(
                lambda r: U.numerize_level(r["level"]),
                axis=1)
        if key == "deities" and "cleric_spells" in df.columns:
            new_columns = ["first_cleric_spell", "second_cleric_spell_level", "second_cleric_spell",
                           "third_cleric_spell_level", "third_cleric_spell"]
            U.split_column(df, "cleric_spells", new_columns, strip=' ')
            df.rename(columns={"cleric_spells": "first_cleric_spell_level"}, inplace=True)

        if key == "ancestries":
            abilities = ["strength", "dexterity", "constitution", "intelligence", "wisdom", "charisma"]
            for ability in abilities:
                df[ability] = df.apply(
                    lambda r: U.get_ability_modifier(r["ability_boosts"], r["ability_flaw"], ability),
                    axis=1)
            df["free_boosts"] = df.apply(
                lambda r: [c.split(' ')[0] for c in r["ability_boosts"] if "free" in c.lower()][0] if isinstance(
                    r["ability_boosts"], list) else "Two",
                axis=1).map({"Two": 2, "Free": 1})

        if key == "backgrounds" and "description_links" in df.columns:
            df["skill"] = df.apply(
                lambda r: r["description_links"][0].split(": ")[0],
                axis=1)
            df["lore"] = df.apply(
                lambda r: [cell.split(": ")[0] for cell in r["description_links"] if "Lore: skill" in cell],
                axis=1)
            df["skill_feat"] = df.apply(
                lambda r: [cell.split(": ")[0] for cell in r["description_links"] if ": feat" in cell],
                axis=1)
            df["free"] = df.apply(
                lambda r: [cell.split(" ")[1] for cell in r["description"] if "free ability boost" in cell],
                axis=1)

        if key == "bloodlines" and "granted_spells" in df.columns:
            new_columns = ["granted_cantrip"] + [f"granted_{i}" for i in range(1, 10)]
            U.split_column(df, "granted_spells", new_columns, strip=' ', step=2)

        if key == "bloodlines" and "bloodline_spells" in df.columns:
            new_columns = ["initial_bloodline_spell", "advanced_bloodline_spell", "greater_bloodline_spell"]
            U.split_column(df, "bloodline_spells", new_columns, strip=' ', step=2)

        if key == "classes" and "saving_throws" in df.columns:
            for save in ["Fortitude", "Reflex", "Will"]:
                df[save.lower()] = df.apply(
                    lambda r: [el.split(" in ")[0].strip() for el in r["saving_throws"] if save in el],
                    axis=1)
        if key == "classes" and "attacks" in df.columns:
            categories = ["unarmed attacks", "simple weapons", "martial weapons", "advanced_weapons"]
            for attack in categories:
                df[attack.replace(' ', '_')] = df.apply(
                    lambda r: [el.split(" in ")[0].strip() for el in r["attacks"] if attack in el],
                    axis=1)
            df["single_weapons"] = df.apply(
                lambda r: [el.strip() for el in r["attacks"] if el.strip() and " in" not in el and el != "and"],
                axis=1)
        if key == "classes" and "defenses" in df.columns:
            for armor in ["light armor", "medium armor", "heavy armor"]:
                df[armor.replace(' ', '_')] = df.apply(
                    lambda r: [el.split(" in ")[0].strip() for el in r["defenses"] if armor in el or "all armor" in el],
                    axis=1)
            df["unarmored_defense"] = df.apply(
                lambda r: [el.split(" in ")[0].strip() for el in r["defenses"] if "unarmored defense" in el],
                axis=1)
        if key == "classes" and "perception" in df.columns:
            df["perception"] = df.apply(
                lambda r: [el.split(" in ")[0].strip() for el in r["perception"] if el],
                axis=1)
        if key == "classes" and "hit_points" in df.columns:
            df["hit_points"] = df.apply(
                lambda r: [el.split(" plus your ")[0].strip() for el in r["hit_points"]],
                axis=1)
        if key == "classes" and "key_ability" in df.columns:
            df["key_ability"] = df.apply(
                lambda r: "".join(r["key_ability"]).lower(),
                axis=1)
            df["alternate_key_ability"] = df.apply(
                lambda r: r["key_ability"].split(" or ")[-1].strip() if " or " in r["key_ability"] else "",
                axis=1)
            df["key_ability"] = df.apply(
                lambda r: r["key_ability"].split(" or ")[0].strip(),
                axis=1)

        if "description" not in df.columns and "other" in df.columns:
            df.rename(columns={"other": "description"}, inplace=True)
        if "source" in df.columns and "source_page" not in df.columns:
            df.rename(columns={"source": "sources"}, inplace=True)
            df["source"] = df.apply(lambda r: [src for src in r["sources"] if source_name in src], axis=1)
            df["source_page"] = df.apply(
                lambda r: int(r["source"][0].split('pg. ')[-1]) if r['source'] and 'pg. ' in r["source"][0] else 0,
                axis=1)
            df["source"] = df.apply(lambda r: r["source"][0].split('pg. ')[0].strip() if r['source'] else "unknown",
                                    axis=1)
        columns = {'url': 'nethys_url'}
        if "x_finder_related_model" in df.columns and "subtype" not in df.columns:
            columns['x_finder_related_model'] = "subtype"
        df.rename(columns=columns, inplace=True)
        return df

    @staticmethod
    def get_index(list_of_strings, string):
        try:
            index = list_of_strings.index(string)
            return index
        except ValueError:
            return -1

    @staticmethod
    def norm_row(row, length):
        if length > 0:
            diff = len(row) - length
            if diff < 0:
                row += [None] * abs(diff)
                print(f"Warning: A row was shorter than expected: {row}.")
            if diff > 0:
                excess = row[length - 1:]
                row = row[:length-1]
                row = row[:-1] + "!".join([row[-1]] + excess)
                print(f"Warning: A row was longer than expected. Last table column hold concatenated data: {row}.")
        return row

    @staticmethod
    def build_dfs(dict_of_dicts, source_name="Unknown", suff=""):
        completed_category_dfs = {}
        normed_category_dfs = {}
        finalized_category_dfs = {}
        for key in dict_of_dicts.keys():
            app = U.get("app", key)
            df = pd.DataFrame.from_records(data=dict_of_dicts[key])
            try:
                df = U.norm_df(df, key, source_name)
                normed_category_dfs[key] = df
                suffix = "normed"
            except Exception:
                print(f"An error occurred while norming {key} df")
                completed_category_dfs[key] = df
                suffix = "completed"
            if suffix == "normed":
                try:
                    model_df = U.extract_model_dfs(df, key)
                    finalized_category_dfs[key] = model_df
                    U.save(model_df, f"{key}_finalized", directory=source_name, app=app)
                except Exception:
                    print(f"An error occurred while finalizing {key} df")
            if suff:
                suffix += "_" + suff
            U.save(df, f"{key}_{suffix}", directory=source_name, app=app)
        return normed_category_dfs, completed_category_dfs, finalized_category_dfs

    @staticmethod
    def build_rows(rows, index, length=0):
        parsed_rows = []
        counter = 0
        for row in rows:
            raw_cells = row.find_all('td')
            parsed_row = [cell.get_text() for cell in raw_cells]
            if index < 0:
                U.norm_row(parsed_row, length)
                parsed_rows.append(parsed_row)
                continue
            try:
                item_url = raw_cells[index].a['href']
            except TypeError:
                item_url = ""
                print(f"Warning: item url link not found for row {row}.")
                counter += 1
            except IndexError:
                item_url = ""
                counter += 1
                print(f"Warning: row {row}is too short for url index: {raw_cells}")
            U.norm_row(parsed_row, length)
            parsed_row.append(item_url)
            parsed_rows.append(parsed_row)
        return parsed_rows, counter
