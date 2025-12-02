import requests
from bs4 import BeautifulSoup
from args import Ica, item_category_arguments as ica


class Provider:
    name = ""

    def __init__(self, target, edition, parser="html.parser"):
        """"""
        self.target = target
        self.edition = edition
        self.parser = parser

    @staticmethod
    def get(argument, category="default"):
        return Ica.get(ica, argument, category)

    def request_content(self, url):
        if url:
            if not url.startswith("http:"):
                url = self.get("base_url") + url
            response = requests.get(url)
            if response.status_code == 200:
                return response.content
            print(f"Received status code {response.status_code} for url {url}")
        print("No url provided.")
        return None

    def cook_from_url(self, url):
        pass

    @staticmethod
    def cook_from_html(html, parser):
        raw_soup = BeautifulSoup(html, parser)
        return raw_soup

    def cook(self, url=None, html=None, parser=None):
        if parser is None:
            parser = self.parser
        if html:
            return self.cook_from_html(html, parser)
        if url:
            html = self.request_content(url)
            return self.cook_from_html(html, self.parser)
        return
