from x_finder.utils.normalizer import Normalizer
from x_finder.utils.args import Ica
from .args import item_category_arguments as ica


class RemasterNormalizer(Normalizer):
    def __init__(self):
        super().__init__("nethys", "remaster")

    @staticmethod
    def get(argument, category="default"):
        return Ica.get(ica, argument, category)
