from args import Ica, item_category_arguments as ica


class Modeler:
    def __init__(self, target, edition):
        self.target = target
        self.edition = edition

    @staticmethod
    def get(argument, category="default"):
        return Ica.get(ica, argument, category)

    def extract_model_dfs(self, df, key):
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
