from x_finder.utils.nethys.provider import TargetProvider


class EditionProvider(TargetProvider):

    def __init__(self, target, edition, parser=None):
        super().__init__(target, edition, parser=parser)
