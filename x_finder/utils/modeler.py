from args import Ica
from pandas import DataFrame
from typing import Iterable


class Modeler:
    def __init__(self, target: str, edition: str) -> None:
        self.target: str = target
        self.edition: str = edition
        self.ica: dict[str, dict[str, str]] | None = None

    def get(self, argument: str, category: str = "default", keys: bool = False) -> str | Iterable[str] | bool | int:
        return Ica.get(self.ica, argument, category, keys)

    def extract_model_dfs(self, df: DataFrame, key: str):
        columns = self.get("model_columns", key)
        excluded = self.get("model_excluded_columns", key)
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
    def fit_source_to_model(df: DataFrame) -> DataFrame:
        fit_df = df[["name", "source_group", "category", "release_date", "errata_date", "errata_version",
                     "nethys_url", "paizo_url", "errata_url"]]
        return fit_df
