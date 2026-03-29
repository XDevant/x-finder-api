from x_finder.utils.nethys.nomalizer import TargetNormalizer


class EditionNormalizer(TargetNormalizer):
    def __init__(self, target, edition):
        super().__init__(target, edition)
