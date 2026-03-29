from x_finder.utils.provider import Provider


class TargetProvider(Provider):
    name = ""

    def __init__(self, target, edition, parser=None):
        super().__init__(target, edition, parser=parser)
