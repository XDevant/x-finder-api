import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from args import Ica, item_category_arguments as ica


class Plate:
    def __init__(self, url=None, title=None, content=None, soup=None):
        self.url = url
        self.title = title
        self.content = content
        self.soup = soup


class Provider:
    name = ""
    parser = "html.parser"
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=options)

    def __init__(self, target, edition, parser=None):
        """"""
        self.target = target
        self.edition = edition
        if parser:
            self.parser = parser
        self.options = self.get_driver_options()
        self.driver = webdriver.Chrome(options=self.options)

    @staticmethod
    def get(argument, category="default"):
        return Ica.get(ica, argument, category)

    @staticmethod
    def get_driver_options():
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        return options

    def get_content(self, url):
        if url:
            if not url.startswith("http:"):
                url = self.get("base_url") + url
            self.driver.get(url)
            title = self.driver.title
            print(title)
            content = self.driver.page_source
            plate = Plate(url=url, title=title, content=content)
            return plate
        print("No url provided.")
        return None

    @staticmethod
    def request_contents(url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        print(f"Received status code {response.status_code} for url {url}")
        return None

    def cook_from_url(self, url):
        pass

    @staticmethod
    def cook_from_html(html, parser):
        raw_soup = BeautifulSoup(html, parser)
        return raw_soup

    def cook(self, url=None, html=None, parser=None):
        if parser is None:
            parser = "html.parser"
        if html:
            return self.cook_from_html(html, parser)
        if url:
            plate = self.get_content(url)
            if plate.content:
                plate.soup = self.cook_from_html(plate.content, 'html5lib')
                return plate
        return
