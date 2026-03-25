from x_finder.utils.modeler import Modeler
from pandas import DataFrame


class RemasterModeler(Modeler):
    def __init__(self):
        super().__init__("nethys", "remaster")

    @staticmethod
    def fit_source_to_model(df: DataFrame) -> DataFrame:
        fit_df = df[["name", "source_group", "category", "release_date", "errata_date", "errata_version",
                     "nethys_url", "paizo_url", "errata_url"]]
        return fit_df
