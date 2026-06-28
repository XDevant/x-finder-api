from x_finder.utils.nethys.nomalizer import TargetNormalizer
from pandas import DataFrame


class EditionNormalizer(TargetNormalizer):
    def __init__(self, target, edition):
        super().__init__(target, edition)


    def norm_bloodlines_df(self, df: DataFrame, category: str) -> None:
        if "sorcerous_gifts" in df.columns:
            new_columns = ["granted_cantrip"] + [f"granted_{i}" for i in range(1, 10)]
            self.split_column(df, "granted_spells", new_columns, strip=' ', step=2)

        if "bloodline_spells" in df.columns:
            new_columns = ["initial_bloodline_spell", "advanced_bloodline_spell", "greater_bloodline_spell"]
            self.split_column(df, "bloodline_spells", new_columns, strip=' ', step=2)

    @staticmethod
    def norm_focus_spells(df: DataFrame, category: str) -> None:
        category_name = category
        if category_name.endswith("s"):
            category_name = category_name[:-1]
        column = category_name + "_spells"
        if column in df.columns:
            ranks = ["initial", "advanced","greater"]
            for i in range(len(ranks)):
                df[ranks[i]] = df.apply(
                    lambda r: r[column][0].split(',')[i].split(':')[-1] if len(r[column][0].split(',')) > i else "",
                    axis=1
                )

    @staticmethod
    def norm_bonus_spells(df: DataFrame, category: str) -> None:
        category_dict = {"bloodlines": "sorcerous_gifts"}
        column = category_dict[category]
        if column in df.columns:
            ranks = ["cantrip", "1st","2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th"]
            for i in range(len(ranks)):
                df[ranks[i]] = df.apply(
        lambda r: r[column][0].split(',')[i].split(':')[-1].strip() if len(r[column][0].split(',')) > i and r[column][0].split(',')[i].split(':')[0].strip() == ranks[i] else "",
        axis=1
                )

    def norm_ancestries_df(self, df: DataFrame, category: str) -> None:
        df["attribute_bonus_1"] = df.apply(
                lambda r: self.get_in_list(r["attribute_boosts"], index=0),
                axis=1
            )
        df["attribute_bonus_2"] = df.apply(
            lambda r: self.get_in_list(r["attribute_boosts"], index=1),
            axis=1
        )
        df["attribute_flaw"] = df.apply(
            lambda r: self.get_in_list(r["attribute_flaw"], index=0),
            axis=1
        )
        df["free_bonus"] = df.apply(
                lambda r: [c.split(' ')[0] for c in r["attribute_boosts"] if "free" in c.lower()][0] if isinstance(
                    r["attribute_boosts"], list) else "Two",
                axis=1
            ).map({"Two": 2, "Free": 1})
        df["speed"] = df.apply(lambda r: self.norm_speed(r["speed"]), axis=1)
        df["size"] = df.apply(lambda r: self.first_char(r["size"]), axis=1)
        df["free_languages"] = df.apply(lambda r: self.get_bonus_languages(r["languages"]), axis=1)
        df["languages__tt_and"] = df.apply(lambda r: self.get_spoken_languages(r["languages"]), axis=1)
        df["languages__tt_or"] = df.apply(lambda r: self.get_language_lists(r["languages"]), axis=1)
        df["common_names"] = df.apply(
            lambda r: r["names"] if isinstance(r["names"], list) else [] + r["sample_names"] if isinstance(r["sample_names"], list) else [],
            axis=1)
        columns = {name: name.strip('.') for name in df.columns if str(name).endswith("...")}
        df["special"] = df.apply(lambda r: " ".join(r["special"]) if isinstance(r["special"], list) else "-", axis=1)
        df["augmented_sense"] = df.apply(
            lambda r: " ".join(r["augmented_sense"]) if isinstance(r["augmented_sense"], list) else "-",
            axis=1)
        df.rename(columns=columns, inplace=True)


    @staticmethod
    def get_bonus_languages(languages: list[str]) -> int:
        if not isinstance(languages, list):
            return 0
        for language in languages:
            if language.startswith("Addition"):
                if " + your Int" in language:
                    start = language.split(" + your Int")[0].strip()
                    return int(start[-1])
        return 0

    @staticmethod
    def get_language_lists(languages: list[str]) -> str:
        language_list: list[str] = []
        check: bool = False
        if not isinstance(languages, list):
            return ""
        for language in languages:
            if language.startswith("Addition"):
                check = True
            elif check and language and not language.startswith("and any"):
                language_list.append(language)
        return " or ".join(language_list)

    @staticmethod
    def get_spoken_languages(languages: list[str]) -> str:
        spoken: list[str] = []
        if not isinstance(languages, list):
            return ""
        for language in languages:
            if language.startswith("Addition"):
                return ", ".join(spoken)
            spoken.append(language)
        return ", ".join(spoken)


    @staticmethod
    def first_char(string_list: list[str] | str) -> str:
        if isinstance(string_list, list) and string_list[0]:
            return string_list[0][0]
        if isinstance(string_list, str) and string_list:
            return string_list[0]
        return str(string_list)

    @staticmethod
    def norm_speed(string_list: list[str]) -> int:
        if isinstance(string_list, list) and string_list[0].endswith("ft."):
            return int(string_list[0][:-3])//5
        if isinstance(string_list, list) and string_list[0].endswith("feet"):
            return int(string_list[0][:-5])//5
        return 0


    @staticmethod
    def get_in_list(string_list: list[str], index: int = 0) -> str:
        if isinstance(string_list, list) and isinstance(index, int) and len(string_list) > index and not string_list[index].startswith("Two free"):
            return string_list[index]
        return "-"

    @staticmethod
    def get_in_strings(strings: list[str], needle: str, separator: str = ": ", ignore: str = "") -> str:
        if isinstance(strings, list) and strings:
            results =  []
            for string in strings:
                if isinstance(string, str) and needle in string and (not ignore or ignore not in string):
                    result = string.split(separator)[0]
                    if result not in results:
                        results.append(result)
            if results:
                return ", ".join(results)
        return ""

    @staticmethod
    def clean_free_boost(string: str) -> int:
        if not isinstance(string, str):
            return 0
        if "one" in string.lower():
            return 1
        if "three" in string.lower():
            return 3
        if "two" in string.lower():
            return 2
        return 0


    def norm_languages_df(self, df: DataFrame, category: str) -> None:
        df["subtype"] = df.apply(
            lambda r: self.get_language_subtype(r),
            axis=1)
        df.rename(columns={"ancestries": "ancestries__tt", "regions": "regions__tt", "creatures": "creatures__tt"},
                  inplace=True)


    @staticmethod
    def get_language_subtype(row: dict[str, list[str] | None | str]) -> str:
        ancestors = []
        if "ancestries" in row.keys() and isinstance(row["ancestries"], list):
            ancestors = row["ancestries"]
        regions = []
        name = row["name"]
        if "regions" in row.keys() and isinstance(row["regions"], list):
            regions = row["regions"]
        if ancestors:
            return "Ancestral"
        elif regions:
            return "Regional"
        elif name in ["Daemonic", "Empyrean", "Diabolic", "Chthonian"]:
            return "Planar"
        elif name in ["Pyric", "Petran", "Sussuran", "Thalassic"]:
            return "Elemental"
        elif name in ["Wildsong"]:
            return "Ritual"
        return "Creatures"


    def norm_backgrounds_df(self, df: DataFrame, category: str) -> None:
        if "description_links" in df.columns:
            df["skill"] = df.apply(
                lambda r: self.get_in_strings(r["description_links"], ": skills", ignore="Lore:"),
                axis=1)
            df["lore"] = df.apply(
                lambda r: self.get_in_strings(r["description_links"], "Lore: skill"),
                axis=1)
            df["skill_feat"] = df.apply(
                lambda r: self.get_in_strings(r["description_links"], ": feat"),
                axis=1)
        df["free"] = df.apply(
            lambda r: self.clean_free_boost(self.get_in_strings(r["description"], "free attribute boost", separator=" free")),
            axis=1)
        for attribute in ["strength", "dexterity", "constitution", "intelligence", "wisdom", "charisma"]:
            df[attribute] = df.apply(lambda r: r[attribute] if isinstance(r[attribute],bool) else False , axis=1)



    @staticmethod
    def norm_classes_df(df: DataFrame, category: str) -> None:
        if "perception" in df.columns:
            df["perception"] = df.apply(
                lambda r: [el.split(" in ")[0].strip() for el in r["perception"] if el],
                axis=1)
        if "hit_points" in df.columns:
            df["hit_points"] = df.apply(
                lambda r: int(r["hit_points"][0].split(" plus your ")[0].strip()) if isinstance(r["hit_points"], list) else 0,
                axis=1)
        if "key_attribute" in df.columns:
            df["key_attribute"] = df.apply(
                lambda r: r["key_attribute"][0].lower() if isinstance(r["key_attribute"], list) else "",
                axis=1)
            df["alt_key_attribute"] = df.apply(
                lambda r: r["key_attribute"].split(" or ")[-1].strip().title() if " or " in r["key_attribute"] else "",
                axis=1)
            df["key_attribute"] = df.apply(
                lambda r: r["key_attribute"].split(" or ")[0].strip().title(),
                axis=1)
        if "saving_throws" in df.columns:
            for save in ["Fortitude", "Reflex", "Will"]:
                df[save.lower()] = df.apply(
                    lambda r: [el.split(" in ")[0].strip() for el in r["saving_throws"] if save in el],
                    axis=1)
        if "attacks" in df.columns:
            categories = ["unarmed attacks", "simple weapons", "martial weapons", "advanced weapons"]
            for attack in categories:
                df[attack.split(" ")[0]] = df.apply(
                    lambda r: ([el.split(" in ")[0].strip() for el in r["attacks"] if attack in el] + ["Untrained"])[0],
                    axis=1)
            df["favored"] = df.apply(
                lambda r: ([el.split(" in ")[0].strip() for el in r["attacks"] if el.strip() and "favored weapon" in el] + ["Untrained"])[0],
                axis=1)
        if "defenses" in df.columns:
            for armor in ["light armor", "medium armor", "heavy armor"]:
                df[armor.split(" ")[0]] = df.apply(
                    lambda r: ([el.split(" in ")[0].strip() for el in r["defenses"] if armor in el or "all armor" in el] + ["Untrained"])[0],
                    axis=1)
            df["unarmored"] = df.apply(
                lambda r: [el.split(" in ")[0].strip() for el in r["defenses"] if "unarmored defense" in el],
                axis=1)
        if "spells" in df.columns:
            df["spells"] = df.apply(lambda r: r["spells"][0].split(" ")[0] if isinstance(r["spells"], list) else "", axis=1)
        if "class_dc" in df.columns:
            df["class_dc"] = df.apply(lambda r: r["class_dc"][0].split(" ")[0] if isinstance(r["class_dc"], list) else "", axis=1)
        if "skills" in df.columns:
            df["free_skills"] = df.apply(lambda r: int(r["skills"][-1].split("equal to ")[-1].split(" plus your")[0]), axis=1)
            df["skills__tt_or"] = df.apply(lambda r: " or ".join([el for el in r["skills"] if len(el.strip()) > 2 and "Trained in" not in el]), axis=1)
        columns = {name: name.strip('.') for name in df.columns if str(name).endswith("...")}
        df.rename(columns=columns, inplace=True)


    def norm_heritages_df(self, df: DataFrame, category: str):
        if "name" in df.columns:
            df["ancestrie__fk"] = df.apply(
                lambda  r: r["name"].split(" ")[-1] if len(r["name"].split(" ")) > 1 else "Versatile",
                axis=1)
        if "description_links" in df.columns:
            df["skill_feat"] = df.apply(
                lambda r: self.get_in_strings(r["description_links"], ": feat"),
                axis=1)
            df.rename(columns={"description_links": "links"}, inplace=True)


    def norm_domains_df(self, df: DataFrame, category: str) -> None:
        df["name"] = df.apply(lambda r: r["name"].split(" ")[0], axis=1)
        df.rename(columns={"domain_spell": "domain_spell__fk",
                           "advanced_domain_spell": "advanced_domain_spell__fk"},
                  inplace=True)


    def norm_skills_df(self, df: DataFrame, category: str) -> None:
        df["attribute"] = df["attribute"].map(self.att_to_attribute)


    def norm_spells_df(self, df: DataFrame, category: str) -> None:
        for tradition in ["arcane", "divine", "occult", "primal"]:
            df[tradition] = df.apply(
                lambda r: True if isinstance(r["traditions"], list) and tradition in r["traditions"] else False,
                axis=1)
        df["action"] = df.apply(
            lambda r: r["action"].strip('[]') if isinstance(r["action"], str) else r["action"], axis=1)
        df.rename(columns={"level": "rank"},
                  inplace=True)

    def norm_weapons_df(self,df: DataFrame, category: str) -> None:
        df["damage_dice_number"] = df.apply(
            lambda r: r["damage"][0].split("d")[0] if isinstance(r["damage"], list) and r["damage"] else "-",
            axis=1)
        df["damage_dice_size"] = df.apply(
            lambda r: r["damage"][0].split("d")[-1].split(" ")[0] if isinstance(r["damage"], list) and r["damage"] else "-",
            axis=1)
        df["damage_type"] = df.apply(
            lambda r: r["damage"][0].split(" ")[-1] if isinstance(r["damage"], list) and r["damage"] else "-",
            axis=1)
        df["range"] = df.apply(
            lambda r: self.norm_speed(r["range"]),
            axis=1
        )
        df.rename(columns={"group": "group__pk"},
                  inplace=True)


    def norm_armors_df(self,df: DataFrame, category: str) -> None:
        df.rename(columns={"group": "group__pk"},
                  inplace=True)


    def norm_deities_df(self, df: DataFrame, category: str) -> None:
        if "cleric_spells" in df.columns:
            new_columns = ["first_cleric_spell__fk", "second_cleric_spell_rank", "second_cleric_spell__fk",
                           "third_cleric_spell_rank", "third_cleric_spell__fk"]
            self.split_column(df, "cleric_spells", new_columns, strip=' ')
            df.rename(columns={"cleric_spells": "first_cleric_spell_rank"}, inplace=True)
