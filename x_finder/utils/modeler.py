from args import Ica
from pandas import DataFrame


class Modeler:
    def __init__(self, target: str, edition: str) -> None:
        self.target: str = target
        self.edition: str = edition
        self.ica: dict[str, dict[str, str | list[str]]] = {}

    def sica(self, argument: str, category: str = "default") -> str:  #
        arguments = Ica.get(self.ica, argument, category=category)
        if isinstance(arguments, str):
            return arguments
        return ""

    def lica(self, argument: str, category: str = "default", keys: bool = False) -> list[str]:  # through_columns, model_cols, model excluded_cols
        arguments = Ica.get(self.ica, argument, category=category, keys=keys)
        if isinstance(arguments, list):
            return arguments
        return []

    def fit_category_to_models(self, df: DataFrame, key: str) -> dict[str, DataFrame]:
        tables_dict = {key: self.extract_model_df(df, key)}
        tables_through = self.lica("through_columns", key)
        if tables_through:
            for table_name in tables_through:
                tables_dict[table_name] = self.extract_model_df(df, key)
        return tables_dict

    def extract_model_df(self, df: DataFrame, key: str) -> DataFrame:
        columns = self.lica("model_columns", key)
        excluded = self.lica("model_excluded_columns", key)
        if excluded:
            columns = [str(column) for column in df.columns if column not in excluded]
        else:
            columns = [str(column) for column in columns if column in df.columns]

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
