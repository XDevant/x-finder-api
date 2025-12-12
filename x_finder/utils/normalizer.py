from utils import U
from args import Ica


class Normalizer:
    def __init__(self, target, edition):
        self.target = target
        self.edition = edition
        self.ica = None

    def get(self, argument, category="default", keys=False):
        return Ica.get(self.ica, argument, category)

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

    def norm_df(self, df, key, source_name):
        subtype = self.get("subtype", key)
        name_nest = self.get("name_nest", key)
        self.split_text_column(df, "name", name_nest, separator='[', strip=' ]')
        self.split_text_column(df, "name", subtype, separator='(', strip=' )')
        if "level" in df.columns:
            if "spell_type" in self.get("text_columns", key):
                df["spell_type"] = df.apply(
                    lambda r: r["level"].split(' ')[0].strip(),
                    axis=1)
            df["level"] = df.apply(
                lambda r: U.numerize_level(r["level"]),
                axis=1)
        if key == "deities" and "cleric_spells" in df.columns:
            new_columns = ["first_cleric_spell", "second_cleric_spell_level", "second_cleric_spell",
                           "third_cleric_spell_level", "third_cleric_spell"]
            self.split_column(df, "cleric_spells", new_columns, strip=' ')
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
            self.split_column(df, "granted_spells", new_columns, strip=' ', step=2)

        if key == "bloodlines" and "bloodline_spells" in df.columns:
            new_columns = ["initial_bloodline_spell", "advanced_bloodline_spell", "greater_bloodline_spell"]
            self.split_column(df, "bloodline_spells", new_columns, strip=' ', step=2)

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
