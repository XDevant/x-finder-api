import pandas as pd
from x_finder.utils.helpers.connection import Con

class SqlitePandaMixin:
    db: str | None = None
    target_db: str | None = None

    def df_to_db(self, df: pd.DataFrame, name: str, db: str | None = None) -> None:
        if db is None:
            db = self.db
            if db is None:
                return
        conn = Con(db)
        h, r = conn.run(name, df=df)
        self.__getattribute__("message_box").insert('end', f'-- {len(r)} rows inserted --')

    def db_to_df(self,
                 name: str | None = None,
                 columns: list[str] | None = None,
                 filters: dict[str,str | None] | None = None,
                 db: str | None = None
                 ) -> pd.DataFrame | None:
        if db is None:
            db = self.db
        if db is None or name is None:
            return None
        wheres = []
        values = []
        if filters is not None:
            for key, value in filters.items():
                column = key
                if value is not None:
                    if key == "group":
                        if name != "sources":
                            continue
                        column = f"sources.[{key}]"
                    wheres.append(f"{column} = ?")
                    values.append(value)
        if not wheres:
            wheres = None
        if not values:
            values = None
        else:
            values = tuple(values)
        conn = Con(db)
        headers, rows = conn.run(name, action="select", columns=columns, wheres=wheres, values=values)
        if rows:
            df = pd.DataFrame(rows, columns=headers)
            return df
        return None

    def get_category_df(self,
                        item: str | None = None,
                        source: str | None = None,
                        category: str | None = None,
                        group: str | None = None,
                        status: str | None = None) -> pd.DataFrame | None:
        db = self.db
        name = category
        if status == "modeled":
            name = f"{name}__{status}"
        df = self.db_to_df(name=name,
                           filters={"item": item, "source": source, "group": group},
                           db=db)
        return df

    def get_category_list(self,
                          source: str | None = None,
                          group: str | None = None,
                          status: str | None = None) -> list[str]:
        db = self.db
        filters = {}
        if source is not None and source:
            filters["source"] = source
        if group is not None and group:
            filters["group"] = group
        df = self.db_to_df(name="links",
                           columns=["DISTINCT category"],
                           filters=filters,
                           db=db)
        if df is not None:
            return df["category"].tolist()
        return []

    def get_table_columns(self, name: str) -> list[str]:
        db = self.db
        if db is not None:
            conn = Con(db)
            columns = conn.execute_sql(f"PRAGMA table_info({name});")
            return columns
        return []


    def check_if_table_exists(self, name: str, target: bool = False, lazy: bool = True) -> bool:
        if target:
            db = self.target_db
        else:
            db = self.db
        if db is None:
            return False
        conn = Con(db)
        where = "name = ?"
        if lazy:
            where = "name LIKE ?"
            name += "%"
        headers, tables = conn.run("sqlite_master", action="select", columns=["name"], wheres=[where], values=(name, ))
        if tables:
            return True
        return False
