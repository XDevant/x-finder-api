from utils import U
from pandas import DataFrame
from x_finder.utils.mixins.ica import IcaMixin


class Normalizer(IcaMixin):
    def __init__(self, target: str, edition: str) -> None:
        self.target: str = target
        self.edition: str = edition
        self.ica: dict[str, dict[str, str | list[str]]] = {}

    @staticmethod
    def split_text_column(df: DataFrame, name: str, new_name: str, separator: str = ' ', strip: str = ' ') -> None:
        if name in df.columns and new_name:
            df[new_name] = df.apply(
                    lambda r: r[name].split(separator)[1].strip(strip) if r[name] and separator in r[name] else '',
                    axis=1)
            df[name] = df.apply(
                    lambda r: r[name].split(separator)[0].strip(strip) if r[name] and separator in r[name] else '',
                    axis=1)

    @staticmethod
    def split_column(df: DataFrame, name: str, new_names: list[str], strip: str = ' ', step: int = 1) -> None:
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

    def norm_df(self, df: DataFrame, key: str, source_name: str) -> DataFrame:
        subtype = self.sica("subtype", category=key)
        name_nest = self.sica("name_nest", category=key)
        self.split_text_column(df, "name", name_nest, separator='[', strip=' ]')
        self.split_text_column(df, "name", subtype, separator='(', strip=' )')
        if "level" in df.columns:
            self.norm_level(df, category=key)
        if "description" not in df.columns and "other" in df.columns:
            df.rename(columns={"other": "description"}, inplace=True)
        self.norm_sources(df, source_name=source_name)
        if key in ["bloodlines"]:
            self.norm_focus_spells(df, key)
            self.norm_bonus_spells(df, key)
        columns = {'url': 'nethys_url'}
        if "x_finder_related_model" in df.columns and "subtype" not in df.columns:
            columns['x_finder_related_model'] = "subtype"
        df.rename(columns=columns, inplace=True)
        return df

    @staticmethod
    def stringify_df(df: DataFrame) -> None:
        for column in df.columns:
            join = " "
            if column.endswith("s"):
                join = "; "
            df[column] = df.apply(
                lambda r: join.join([str(e) for e in set(r[column])]) if isinstance(r[column], list) else r[column],
                axis=1)

    @staticmethod
    def norm_sources(df: DataFrame, source_name: str) -> None:
        if "source" in df.columns and "source_page" not in df.columns:
            df.rename(columns={"source": "sources"}, inplace=True)
        if "sources" in df.columns:
            df["source"] = df.apply(
                lambda r: [src for src in r["sources"] if source_name in src] if isinstance(r["sources"], list) else [
                    r["sources"]],
                axis=1
            )
            df["source_page"] = df.apply(
                lambda r: int(r["source"][0].split('pg. ')[-1]) if isinstance(r['source'], list) and isinstance(
                    r["source"][0], str) else 0,
                axis=1
            )
            df["source"] = df.apply(
                lambda r: r["source"][0].split('pg. ')[0].strip() if isinstance(r['source'], list) and isinstance(
                    r["source"][0], str) else "unknown",
                axis=1
            )

    def norm_level(self, df: DataFrame, category: str) -> None:
        if "spell_type" in self.lica("text_columns", category=category):
            df["spell_type"] = df.apply(
                lambda r: r["level"].split(' ')[0].strip(),
                axis=1)
        df["level"] = df.apply(
            lambda r: U.numerize_level(r["level"]),
            axis=1)

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

    @staticmethod
    def norm_sources_df(df: DataFrame) -> None:
        pass
