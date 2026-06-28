from typing import Callable, Any

from x_finder.utils.nethys.selector import EditionSelector as NethysSelector
from x_finder.utils.datahandling import Dh


class Selector:
    handlers: dict[str, Callable[[str, str], Any]] = {"nethys": NethysSelector}
    targets = []
    editions = []
    handler: Dh | None = None
    target: str | None = None
    edition: str | None = None
    validated: bool = False

    def __init__(self, target: str, edition: str) -> None:
        self.target = target
        self.edition = edition
        self.targets = list(self.handlers.keys())
        if target in self.targets:
            selector = self.handlers[target](target, edition)
            if selector is not None:
                self.handler = selector.handler
                self.editions = list(selector.handlers.keys())
            if edition in self.editions:
                self.validated = True

    def handler_selector(self, target: str, edition: str) -> Dh:
        if target in self.handlers.keys():
            return self.handlers[target](target, edition).handler
        return Dh(target, edition)


if __name__ == "__main__":
    H = Selector('nethys', 'remaster').handler
    if isinstance(H, Dh):
        print(H.provider.sica("base_url"))
        print(H.sica("base_url"))
        print(H.parser.sica("base_url"))
        print(H.normalizer.sica("base_url"))
        print(H.modeler.sica("base_url"))
