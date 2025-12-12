from .parser import RemasterParser
from .nomalizer import RemasterNormalizer
from .modeler import RemasterModeler
from .provider import RemasterProvider
from x_finder.utils.datahandling import Dh


class RemasterDh(Dh):
    def __init__(self):
        super().__init__("nethys", "remaster")

    def instantiate_provider(self):
        self.provider = RemasterProvider()

    def instantiate_parser(self):
        self.parser = RemasterParser()

    def instantiate_normalizer(self):
        self.normalizer = RemasterNormalizer()

    def instantiate_modeler(self):
        self.modeler = RemasterModeler()
