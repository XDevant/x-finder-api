import sqlite3 as lite
from pandas import DataFrame
from .sql import SQL


class Con:
    def __init__(self, db: str, *args, **kwargs):
        self.db = db
        self.args = args
        self.kwargs = kwargs

    def execute_sql(self, sql: str, values: tuple | None = None) -> list:
        with lite.connect(self.db, **self.kwargs) as conn:
            conn.row_factory = lite.Row
            cur = conn.cursor()
            cur.execute("PRAGMA foreign_keys = ON;")
            if values is not None:
                cur.execute(sql, values)
            else:
                cur.execute(sql)
            conn.commit()
            row_list = cur.fetchall()
        return row_list

    def run(self,
            name: str,
            df=None,
            action: str | None = None,
            columns: list[str] | None = None,
            wheres: list[str] | None = None,
            values: tuple | None = None
            ) -> tuple[list, list]:
        headers = []
        rows_list = []
        if self.db is None or not self.db:
            return [], []
        if name:
            if df is not None:
                sql = self.prepare_table(name, df)
                print(sql)
                self.execute_sql(sql)
                with lite.connect(self.db, **self.kwargs) as conn:
                    inserted = df.to_sql(name=name, con=conn, if_exists='append', index=False)
                    rows_list = [1]*inserted
            if action is not None:
                sql_builder = SQL(action, name, columns=columns, values=values, wheres=wheres)
                sql = sql_builder.get()
                print(sql, values)
                rows = self.execute_sql(sql, values)
                count = len(rows)
                if count > 0:
                    headers = list(rows[0].keys())
                    rows_list = [list(row) for row in rows]
        return headers, rows_list

    @staticmethod
    def prepare_table(name: str, df: DataFrame | None = None) -> str:
        if df is not None:
            columns = list(df.columns)
            action = "create"
            sql = SQL(action, name, columns=columns)
            return sql.get()
        return ""


if __name__ == "__main__":
    con = Con("test.db")
    con.run("test3", action="create", columns=["name", "speed", "checks"])
    con.run("test3", action="insert", columns=["name", "speed", "checks"], values=('barg', 2, 1,))
    con.run("test3", action="insert", columns=["name", "speed", "checks"], values=('zarg', 2, 1,))
    con.run("test3", action="insert", columns=["name", "speed", "checks"], values=('zorg', 2, 1,))
    con.run("test3", action="insert", columns=["name", "speed", "checks"], values=('morg', 2, 0,))
    con.run("test3", action="insert", columns=["name", "speed", "checks"], values=('cyborg', 1, 0,))
    cols, data = con.run("test3", action="select", wheres=["(name = ?)"], values=('barg', ))
    print(cols, data)
    cols, data = con.run("test3", action="select")
    print(data)
