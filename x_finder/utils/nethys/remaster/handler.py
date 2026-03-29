from .parser import EditionParser
from .nomalizer import EditionNormalizer
from .modeler import EditionModeler
from .provider import EditionProvider
from .reader import EditionReader
from x_finder.utils.nethys.handler import TargetDh


class EditionDh(TargetDh):
    def __init__(self, target, edition):
        super().__init__(target, edition)

    def instantiate_provider(self):
        self.provider = EditionProvider(self.target, self.edition)
        self.provider.ica = self.ica

    def instantiate_parser(self):
        self.parser = EditionParser(self.target, self.edition)
        self.parser.reader = EditionReader()
        self.parser.reader.ica = self.ica
        self.parser.ica = self.ica

    def instantiate_normalizer(self):
        self.normalizer = EditionNormalizer(self.target, self.edition)
        self.normalizer.ica = self.ica

    def instantiate_modeler(self):
        self.modeler = EditionModeler(self.target, self.edition)
        self.modeler.ica = self.ica
