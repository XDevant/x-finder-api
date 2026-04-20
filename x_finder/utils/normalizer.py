from utils import U
from args import Ica
from pandas import DataFrame


class Normalizer:
    def __init__(self, target: str, edition: str) -> None:
        self.target: str = target
        self.edition: str = edition
        self.ica: dict[str, dict[str, str | list[str]]] = {}

    def sica(self, argument: str, category: str = "default") -> str:  # subtype, name_nest
        arguments = Ica.get(self.ica, argument, category=category)
        if isinstance(arguments, str):
            return arguments
        return ""

    def lica(self, argument: str, category: str = "default", keys: bool = False) -> list[str]:  # text_columns
        arguments = Ica.get(self.ica, argument, category=category, keys=keys)
        if isinstance(arguments, list):
            return arguments
        return []

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
            if "spell_type" in self.lica("text_columns", category=key):
                df["spell_type"] = df.apply(
                    lambda r: r["level"].split(' ')[0].strip(),
                    axis=1)
            df["level"] = df.apply(
                lambda r: U.numerize_level(r["level"]),
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
    def norm_sources_df(df: DataFrame) -> None:
        pass
