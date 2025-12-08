import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from args import Ica, item_category_arguments as ica


class Plate:
    """
    Class designed to store the data relative to its url until complete parsing.
    The Provider extracts the soup and some metadata and tries to update the item's name and category.
    The Parser class takes the soup, validates the plate, builds the data_dict and check for completion.
    The completed data_dicts are saved in a csv: utils/fixtures/<target>/<edition>/<category>_completed.csv
    """
    def __init__(self, url=None, title=None, content=None, soup=None):
        self.url = url
        self.title = title
        self.content = content
        self.soup = soup
        self.name = "unknown"
        self.category = "default"
        self.item_links = {}
        self.data_dict = {}
        self.validated = False
        self.completed = False


class Provider:
    """
    The job of the provider is to turn an url into a plate containing a beautiful soup that will be given to the Parser
    """
    parsers = ["html.parser", "html5lib"]
    parser = parsers[1]

    def __init__(self, target, edition, parser=None):
        self.target = target
        self.edition = edition
        if parser in self.parsers:
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
        options.add_argument("--detach")
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
    def extract_plate_name_and_category(plate):
        if plate.title and isinstance(plate.title, str):
            title_parts = plate.title.split('-')
            if len(title_parts) > 0:
                plate.name = title_parts[0].lower().strip()
            if len(title_parts) > 1:
                plate.category = title_parts[1].lower().strip()

    @staticmethod
    def cook_from_html(html, parser):
        raw_soup = BeautifulSoup(html, parser)
        return raw_soup

    def cook(self, url, parser='auto'):
        if parser in self.parsers:
            parser = parser
        else:
            parser = self.parser
        if url:
            plate = self.get_content(url)
            if plate.content:
                plate.soup = self.cook_from_html(plate.content, parser)
                self.extract_plate_name_and_category(plate)
                return plate
        return

    @staticmethod
    def request_contents(url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        print(f"Received status code {response.status_code} for url {url}")
        return None

    def cook_from_url(self, url):
        pass
