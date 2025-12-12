from x_finder.utils.provider import Provider


class RemasterProvider(Provider):
    name = ""

    def __init__(self, parser=None):
        super().__init__("nethys", "remaster", parser=parser)
