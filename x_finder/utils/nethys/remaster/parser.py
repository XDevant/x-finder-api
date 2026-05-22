from x_finder.utils.nethys.parser import TargetParser
from.reader import EditionReader


class EditionParser(TargetParser):
    def __init__(self, target, edition):
        super().__init__(target, edition)
        self.reader = EditionReader()

