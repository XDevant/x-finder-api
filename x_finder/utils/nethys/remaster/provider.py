from x_finder.utils.provider import Provider
from x_finder.utils.nethys.remaster.args import item_category_arguments as ica
from x_finder.utils.args import Ica


class RemasterProvider(Provider):
    name = ""

    def __init__(self, parser="html.parser"):
        super().__init__("nethys", "remaster", parser=parser)

    @staticmethod
    def get(argument, category="default"):
        return Ica.get(ica, argument, category)
