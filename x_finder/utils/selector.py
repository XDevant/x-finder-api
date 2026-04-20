from typing import Callable, Any

from nethys.selector import EditionSelector as NethysSelector
from datahandling import Dh


class Selector:
    handlers: dict[str, Callable[[str, str], Any]] = {"nethys": NethysSelector}
    targets = []
    editions = []
    handler = None
    target = None
    edition = None
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
    print(H.provider.get("base_url"))
    print(H.get("base_url"))
    print(H.parser.get("base_url"))
    print(H.normalizer.get("base_url"))
    print(H.modeler.get("base_url"))
