
class SQL:
    actions = {"select": "SELECT",
               "create": "CREATE TABLE IF NOT EXISTS",
               "insert": "INSERT INTO",
               "delete": "DELETE FROM "}

    def __init__(self,
                 action: str,
                 table: str = "links",
                 columns: list[str] | None = None,
                 values: tuple | None = None,  # select: where_ph, insert: values
                 wheres: list[str] | None = None,
                 orders: list[str] | None = None,
                 limit: int = None
                 ) -> None:
        self.action = action
        self.table = " " + table
        self.columns = columns
        if values is not None:
            self.values_ph = " VALUES (" + ", ".join(["?" for value in values]) + ")"
            self.values = values
        else:
            self.values = None
            self.values_ph = None
        self.where = ""
        if wheres is not None:
            self.where = " WHERE " + " AND ".join(wheres)
        self.order = ""
        if orders is not None:
            self.order = " ORDER BY " + ", ".join(orders)
        self.limit = ";"
        if limit:
            self.limit = f" LIMIT = {limit};"

    def stringify_select(self) -> str:
        if self.columns is not None:
            col_string = " " + ", ".join(self.columns)
        else:
            col_string = " *"
        return self.actions[self.action] + col_string + " FROM" + self.table + self.where + self.order + self.limit

    def stringify_create(self) -> str:
        typed_cols = self.add_column_type()
        return self.actions[self.action] + self.table + " (" + ", ".join(typed_cols) + ")" + ";"

    def stringify_insert(self) -> str:
        if self.values_ph:
            return self.actions[self.action] + self.table + self.values_ph + ";"
        return ""

    def stringify_delete(self) -> str:
        return self.actions[self.action] + self.table + self.where + ";"

    def get(self):
        return self.__getattribute__(f"stringify_{self.action}")()

    def __str__(self):
        if self.values:
            values = (self.prettyfy(value) for value in self.values)
            sql = self.get()
            return sql.replace('?', '{}').format(*values)
        return self.get()

    @staticmethod
    def prettyfy(value: str | bool | int) -> str | bool | int:
        if isinstance(value, str):
            return "'" + value + "'"
        return value

    def add_column_type(self) -> list[str]:
        table_name = self.table.strip()
        typed_cols: list[str] = []
        uk: str = ""
        if self.columns is None:
            return []
        for column in self.columns:
            if column != "name" or table_name == "links" or "quality" in self.columns or "x_finder_related_item" in self.columns:
                typed_cols.append(f"'{column}' TEXT")
            else:
                typed_cols.append(f"'{column}' TEXT UNIQUE ON CONFLICT REPLACE")
        if table_name == "links":
            uk = "UNIQUE (name, category) ON CONFLICT REPLACE"
        if "name" in self.columns:
            if "quality" in self.columns:
                uk = "UNIQUE (name, quality, level) ON CONFLICT REPLACE"
            if "x_finder_related_item" in self.columns:
                uk = "UNIQUE (name, x_finder_related_item) ON CONFLICT REPLACE"
        if uk:
            typed_cols.append(uk)
        if "source" in self.columns and table_name != "sources":
            fk = "FOREIGN KEY (source) REFERENCES sources(name) ON DELETE CASCADE"
            typed_cols.append(fk)
        return typed_cols


if __name__ == "__main__":
    query = SQL("select", "links", wheres=["group = ?", "family = ?"], values=('rulebooks', 'player'))
    print(query)
    query2 = SQL("create", "links", columns=["name TEXT UNIQUE ON CONFLICT REPLACE",
                                             "url TEXT",
                                             "group TEXT",
                                             "check INTEGER"])
    print(query2)
    query3 = SQL("insert", "links", values=("pim", "url", "rulebooks", True))
    print(query3.get(), "\n", query3)
