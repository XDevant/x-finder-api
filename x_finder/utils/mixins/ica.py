
class IcaMixin:
    ica: dict = {"default": {}}

    @staticmethod
    def get_item_category_argument(ica, argument, category="default", keys=False):
        if keys:
            return ica.keys()
        if not argument and category in ica.keys():
            return category
        if category in ica.keys() and argument in ica[category].keys():
            return ica[category][argument]
        if argument in ica["default"].keys():
            return ica["default"][argument]
        return ""

    def sica(self, argument: str, category: str = "default") -> str:  # index_url, nav_id, title_tag
        arguments = IcaMixin.get_item_category_argument(self.ica, argument, category=category)
        if isinstance(arguments, str):
            return arguments
        return ""

    def lica(self, argument: str, category: str = "default", keys: bool = False) -> list[str]:  # tex_columns, chk_cols, nested_columns, description_tags
        arguments = list(IcaMixin.get_item_category_argument(self.ica, argument, category=category, keys=keys))
        if isinstance(arguments, list):
            return arguments
        return []

    def bica(self, argument: str, category: str = "default", keys: bool = False) -> bool:  # nested, no_description, overload
        arguments = IcaMixin.get_item_category_argument(self.ica, argument, category=category, keys=keys)
        if isinstance(arguments, bool):
            return arguments
        return False

    def intca(self, argument: str, category: str = "default", keys: bool = False) -> int:  # start_date
        arguments = IcaMixin.get_item_category_argument(self.ica, argument, category=category, keys=keys)
        if isinstance(arguments, int):
            return arguments
        return 0
