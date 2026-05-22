from .parser import EditionParser
from .nomalizer import EditionNormalizer
from .modeler import EditionModeler
from .provider import EditionProvider
from .reader import EditionReader
from x_finder.utils.nethys.handler import TargetDh


class EditionDh(TargetDh):
    def __init__(self, target, edition):
        super().__init__(target, edition)
        self.instantiate_workers()

    def instantiate_workers(self) -> None:
        print("remaster worker instantiated")
        print(self.ica.keys())
        self.instantiate_provider(EditionProvider(self.target, self.edition))
        self.instantiate_parser(EditionParser(self.target, self.edition))
        self.instantiate_normalizer(EditionNormalizer(self.target, self.edition))
        self.instantiate_modeler(EditionModeler(self.target, self.edition))
