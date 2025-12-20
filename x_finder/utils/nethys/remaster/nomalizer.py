from x_finder.utils.normalizer import Normalizer
from x_finder.utils.utils import U
from pandas import DataFrame


class RemasterNormalizer(Normalizer):
    def __init__(self):
        super().__init__("nethys", "remaster")

    @staticmethod
    def norm_source_df(df: DataFrame) -> None:
        df["release_date"] = [U.translate_date(date) for date in df["release_date"]]
        df["errata_date"] = df.apply(
            lambda r: U.translate_date(str(r["latest_errata"]).split(' - ')[-1]) if r["latest_errata"] else None,
            axis=1)
        df["errata_version"] = df.apply(
            lambda r: str(r["latest_errata"]).split(' - ')[0].strip() if r["latest_errata"] else "-",
            axis=1)
        df.rename(columns={'product_page_url': 'paizo_url', 'latest_errata_url': 'errata_url', 'source_group': 'group'},
                  inplace=True)
