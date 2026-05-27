from x_finder.utils.normalizer import Normalizer
from x_finder.utils.utils import U
from pandas import DataFrame


class TargetNormalizer(Normalizer):
    def __init__(self, target, edition):
        super().__init__(target, edition)

    @staticmethod
    def norm_sources_df(df: DataFrame) -> None:
        df["release_date"] = df.apply(lambda r: U.translate_date(str(r["release_date"])), axis=1)
        df["errata_date"] = df.apply(
            lambda r: U.translate_date(str(r["latest_errata"]).split(' - ')[-1].strip()) if r["latest_errata"] else "-",
            axis=1)
        df["errata_version"] = df.apply(
            lambda r: str(r["latest_errata"]).split(' - ')[0].strip() if r["latest_errata"] else "-",
            axis=1)
        if "product_page_url" not in df.columns:
            df["product_page_url"] = None
        df.rename(columns={'product_page_url': 'paizo_url', 'product_line': 'group'},
                  inplace=True)

    def norm_deities_df(self, df: DataFrame) -> None:
        if "cleric_spells" in df.columns:
            new_columns = ["first_cleric_spell", "second_cleric_spell_level", "second_cleric_spell",
                           "third_cleric_spell_level", "third_cleric_spell"]
            self.split_column(df, "cleric_spells", new_columns, strip=' ')
            df.rename(columns={"cleric_spells": "first_cleric_spell_level"}, inplace=True)
