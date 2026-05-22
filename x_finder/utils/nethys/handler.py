from .parser import TargetParser
from .nomalizer import TargetNormalizer
from .modeler import TargetModeler
from .provider import TargetProvider
from .reader import TargetReader
from x_finder.utils.datahandling import Dh


class TargetDh(Dh):
    def __init__(self, target, edition):
        super().__init__(target, edition)
        if not edition:
            self.provider = TargetProvider(self.target, self.edition)
            self.parser = TargetParser(self.target, self.edition)
            self.normalizer = TargetNormalizer(self.target, self.edition)
            self.modeler = TargetModeler(self.target, self.edition)
            self.dispatch_ica()

    def instantiate_workers(self) -> None:
        self.instantiate_provider(TargetProvider(self.target, self.edition))
        self.instantiate_parser(TargetParser(self.target, self.edition))
        self.instantiate_normalizer(TargetNormalizer(self.target, self.edition))
        self.instantiate_modeler(TargetModeler(self.target, self.edition))
