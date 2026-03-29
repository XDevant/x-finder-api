from .remaster.handler import EditionDh as Remaster
from .legacy.handler import EditionDh as Legacy
from .starfinder.handler import EditionDh as Starfinder
from .handler import TargetDh
from x_finder.utils.datahandling import Dh


class EditionSelector:
    handlers: dict[str, Dh()] = {"remaster": Remaster, "legacy": Legacy, "starfinder": Starfinder}

    def __init__(self, target: str, edition: str):
        if edition in self.handlers.keys():
            self.handler = self.handlers[edition](target, edition)
        else:
            self.handler = TargetDh(target, edition)


if __name__ == "__main__":
    H = EditionSelector('nethys', 'remaster').handler
    print(H.provider.get("base_url"))
    print(H.get("base_url"))
    print(H.parser.get("base_url"))
    print(H.normalizer.get("base_url"))
    print(H.modeler.get("base_url"))
