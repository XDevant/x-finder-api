from pandas import DataFrame
from x_finder.utils.mixins.ica import IcaMixin


class Modeler(IcaMixin):
    def __init__(self, target: str, edition: str) -> None:
        self.target: str = target
        self.edition: str = edition
        self.ica: dict[str, dict[str, str | list[str]]] = {}

    def fit_category_to_models(self, df: DataFrame, key: str) -> dict[str, DataFrame]:
        tables_dict = {key: self.extract_model_df(df, key)}
        tables_through = self.lica("through_columns", key)
        if tables_through:
            for table_name in tables_through:
                tables_dict[table_name] = self.extract_through_df(df, table_name)
        return tables_dict

    def extract_model_df(self, df: DataFrame, key: str) -> DataFrame:
        columns = self.lica("model_columns", key)
        if "description_links" in df.columns and "description" in columns:
            columns.append("description_links")
        excluded = self.lica("model_excluded_columns", key)
        if excluded:
            columns = [str(column) for column in df.columns if column not in excluded]
        else:
            columns = [str(column) for column in columns if column in df.columns]

        model_df = df[columns]
        return model_df

    def extract_through_df(self, df: DataFrame, key: str) -> DataFrame:
        return df