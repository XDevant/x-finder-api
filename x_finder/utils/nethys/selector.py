from typing import Callable
from .remaster.handler import EditionDh as Remaster
from .legacy.handler import EditionDh as Legacy
from .starfinder.handler import EditionDh as Starfinder
from .handler import TargetDh


class EditionSelector:
    handlers: dict[str, Callable[[str,str], TargetDh]] = {"remaster": Remaster, "legacy": Legacy, "starfinder": Starfinder}

    def __init__(self, target: str, edition: str) -> None:
        if edition in self.handlers.keys():
            handler = self.handlers[edition]
            self.handler = handler(target, edition)
        else:
            self.handler = TargetDh(target, edition)


if __name__ == "__main__":
    H = EditionSelector('nethys', 'remaster').handler
    if H is not None:
        print(H.provider.get("base_url"))
        print(H.sica("base_url"))
        print(H.parser.sica("base_url"))
        print(H.normalizer.sica("base_url"))
        print(H.modeler.sica("base_url"))
