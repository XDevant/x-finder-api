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

    def fit_category_to_models(self, df: DataFrame, key: str) -> dict[str, DataFrame]:
        tables_dict = {key: self.extract_model_df(df, key)}
        tables_through = self.get("tables_through", key)
        if tables_through:
            for table_name in tables_through:
                tables_dict[table_name] = self.extract_model_df(df, key)
        return tables_dict

    def extract_model_df(self, df: DataFrame, key: str) -> DataFrame:
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
