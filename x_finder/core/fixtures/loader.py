from pandas import DataFrame
from x_finder.settings import BASE_DIR
from utils.mixins.sqlitepandas import SqlitePandaMixin



class FixtureLoader(SqlitePandaMixin):
    ica: dict[str, dict[str, str | list | bool]] = None
    df: DataFrame | None = None
    records: list[dict[str, str]] = []
    fks: list[str] = []
    tts: list[tuple[str]] = []

    def __init__(self, model_name: str, app: str ="core", db_path: str | None = None):
        self.app: str = app
        self.model_name: str = model_name
        if db_path is None:
            self.db = f"{BASE_DIR}\\remaster\\fixtures\\remaster.db"
        else:
            self.db: str = db_path
        self.load_fixture()
        if self.df is None:
            return
        self.type_df()
        self.records = self.df.to_dict('records')


    def load_fixture(self):
        df: DataFrame | None = self.db_to_df(self.model_name, db=self.db)
        self.df = df


    def type_df(self):
        if self.df is None:
            return
        for column in self.df.columns:
            if "__" not in column:
                continue
            name, d_type = column.split("__")

            d_types = {"int": int, "float": float, "bool": bool, "str": str}
            if d_type in d_types.keys():
                self.df[column] = d_types[d_type](self.df[column])
            self.df.rename(columns={column: name}, inplace=True)
            if d_type == "fk":
                self.fks.append(name)
            if d_type.startswith("tt_"):
                self.tts.append((name, d_type.split('t')[-1], ))
