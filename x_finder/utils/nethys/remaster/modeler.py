from x_finder.utils.modeler import Modeler
from x_finder.utils.args import Ica
from .args import item_category_arguments as ica


class RemasterModeler(Modeler):
    def __init__(self):
        super().__init__("nethys", "remaster")

    @staticmethod
    def get(argument, category="default"):
        return Ica.get(ica, argument, category)
