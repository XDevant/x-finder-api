
class Ica:
    @staticmethod
    def get(ica, argument, category="default", keys=False):
        if keys:
            return ica.keys()
        if not argument and category in ica.keys():
            return category
        if category in ica.keys() and argument in ica[category].keys():
            return ica[category][argument]
        if argument in ica["default"].keys():
            return ica["default"][argument]
        return None


item_category_arguments = {"default": {
    "base_url": "ex: https://2e.aonprd.com/",
    "app": "mostly used to save final csv in the right app/fixture/csv directory or read a csv",
    "item_url_column": 'the header of the column where we should find the name of the item',
    "text_columns": [],
    "check_columns": ["if one these strings is checked as a potential key, the value is set to true/false, and stored"],
    "url_columns": ["we expect a link around this key and tell the Parser to extract it"],
    "model_columns": ["name", "nethys_url", "source", "source_page", "description"],
    "model_excluded_columns": [],
                                      },
                           }
