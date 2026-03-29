from .parser import TargetParser
from .nomalizer import TargetNormalizer
from .modeler import TargetModeler
from .provider import TargetProvider
from .reader import TargetReader
from x_finder.utils.datahandling import Dh


class TargetDh(Dh):
    def __init__(self, target, edition):
        super().__init__(target, edition)

    def instantiate_provider(self):
        self.provider = TargetProvider(self.target, self.edition)
        self.provider.ica = self.ica

    def instantiate_parser(self):
        self.parser = TargetParser(self.target, self.edition)
        self.parser.reader = TargetReader()
        self.parser.ica = self.ica
        self.parser.reader.ica = self.ica

    def instantiate_normalizer(self):
        self.normalizer = TargetNormalizer(self.target, self.edition)
        self.normalizer.ica = self.ica

    def instantiate_modeler(self):
        self.modeler = TargetModeler(self.target, self.edition)
        self.modeler.ica = self.ica
