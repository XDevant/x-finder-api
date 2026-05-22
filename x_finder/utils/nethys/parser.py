from x_finder.utils.parser import Parser
from .reader import TargetReader


class TargetParser(Parser):
    def __init__(self, target, edition):
        super().__init__(target, edition)
        if not self.edition:
            self.reader = TargetReader()
            self.reader.ica = self.ica
