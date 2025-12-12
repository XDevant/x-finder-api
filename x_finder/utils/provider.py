import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from args import Ica


class Provider:
    """
    The job of the provider is to turn an url into a plate containing a beautiful soup that will be given to the Parser
    """
    parsers = ["html.parser", "html5lib"]
    parser = parsers[1]

    def __init__(self, target, edition, parser=None):
        self.target = target
        self.edition = edition
        self.ica = None
        if parser in self.parsers:
            self.parser = parser
        self.options = self.get_driver_options()
        self.driver = None

    def get(self, argument, category="default", keys=False):
        return Ica.get(self.ica, argument, category)

    @staticmethod
    def get_driver_options():
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--detach")
        return options

    def setup(self, url):
        driver = webdriver.Chrome(options=self.options)
        driver.get(url)
        self.driver = driver

    def teardown(self):
        self.driver.quit()

    def get_content(self, plate, keep_alive=False):
        url = plate.url
        if url:
            if not url.startswith("http:"):
                url = self.get("base_url") + plate.url
            self.setup(url)
            title = self.driver.title
            plate.title = title
            content = self.driver.page_source
            plate.content = content
            if not keep_alive:
                self.teardown()
        else:
            print("No url provided.")

    @staticmethod
    def extract_plate_name_and_category(plate):
        if plate.title and isinstance(plate.title, str):
            title_parts = plate.title.split('-')
            if len(title_parts) > 0:
                plate.name = title_parts[0].lower().strip()
            if len(title_parts) > 1:
                category_parts = title_parts[1].split('(')
                plate.category = category_parts[0].lower().strip()

    @staticmethod
    def cook_from_html(html, parser):
        raw_soup = BeautifulSoup(html, parser)
        return raw_soup

    def cook(self, plate, parser='auto', keep_alive=False):
        if parser in self.parsers:
            parser = parser
        else:
            parser = self.parser
        if plate.url:
            self.get_content(plate, keep_alive=keep_alive)
            if plate.content:
                plate.soup = self.cook_from_html(plate.content, parser)
                self.extract_plate_name_and_category(plate)

    @staticmethod
    def request_contents(url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        print(f"Received status code {response.status_code} for url {url}")
        return None

    def cook_from_url(self, url):
        pass
