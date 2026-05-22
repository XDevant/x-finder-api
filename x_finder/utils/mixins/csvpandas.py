import pandas as pd


class CsvPandasMixin:
    path: str = ""

    def load_csv(self, file_name: str) -> pd.DataFrame:
        pathfile = self.path + file_name
        df = pd.read_csv(pathfile, sep='|')
        return df

    def export_df_as_csv(self, df: pd.DataFrame) -> None:
        name = self.path + 'self.new_csv_entry.get()' + '.csv'
        if df is not None:
            df.to_csv(name, index=False)

    @staticmethod
    def read_csv_by_line(path_name) -> list[str]:
        if path_name.endswith('.csv'):
            with open(path_name, 'r') as file:
                lines = file.readlines()
                return lines
        return [""]
