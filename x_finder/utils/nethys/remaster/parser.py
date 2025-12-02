from x_finder.utils.parser import Parser
from x_finder.utils.args import Ica
from x_finder.utils.nethys.remaster.args import item_category_arguments as ica


class RemasterParser(Parser):
    def __init__(self):
        super().__init__("nethys", "remaster")

    @staticmethod
    def get(argument, category="default"):
        return Ica.get(ica, argument, category)
