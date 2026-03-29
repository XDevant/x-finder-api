from x_finder.utils.nethys.parser import TargetParser


class EditionParser(TargetParser):
    def __init__(self, target, edition):
        super().__init__(target, edition)
