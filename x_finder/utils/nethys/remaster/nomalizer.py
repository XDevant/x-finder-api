from x_finder.utils.normalizer import Normalizer


class RemasterNormalizer(Normalizer):
    def __init__(self):
        super().__init__("nethys", "remaster")
