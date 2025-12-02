from nethys.remaster.handler import RemasterDh
from nethys.legacy.handler import LegacyDh


def handler_selector(target, edition):
    if target == 'nethys' and edition == 'remaster':
        return RemasterDh()
    if target == 'nethys' and edition == 'legacy':
        return LegacyDh()


if __name__ == "__main__":
    H = handler_selector('nethys', 'remaster')
    print(H.provider.get("base_url"))
    print(H.get("base_url"))
    print(H.parser.get("base_url"))
    print(H.normalizer.get("base_url"))
    print(H.modeler.get("base_url"))
