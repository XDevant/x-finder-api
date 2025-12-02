from .parser import RemasterParser
from .nomalizer import RemasterNormalizer
from .modeler import RemasterModeler
from .provider import RemasterProvider
from x_finder.utils.datahandling import Dh
from x_finder.utils.args import Ica
from .args import item_category_arguments as ica


class RemasterDh(Dh):
    def __init__(self):
        super().__init__("nethys", "remaster")
        self.provider = RemasterProvider()
        self.parser = RemasterParser()
        self.normalizer = RemasterNormalizer()
        self.modeler = RemasterModeler()

    @staticmethod
    def get(argument, category="default"):
        return Ica.get(ica, argument, category)
