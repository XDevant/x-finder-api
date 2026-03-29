from x_finder.utils.nethys.modeler import TargetModeler
from pandas import DataFrame


class EditionModeler(TargetModeler):
    def __init__(self, target, edition):
        super().__init__(target, edition)
