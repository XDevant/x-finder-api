import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from args import Ica
from helpers import Plate
from typing import Iterable
from time import sleep


class Provider:
    """
    The job of the provider is to turn an url into a plate containing a beautiful soup that will be given to the Parser
    """
    parsers = ["html.parser", "html5lib"]
    parser = parsers[1]

    def __init__(self, target: str, edition: str, parser: str | None = None):
        self.target = target
        self.edition = edition
        self.ica: dict[str, dict[str, str]] | None = None
        if parser in self.parsers:
            self.parser = parser
        self.options: webdriver.ChromeOptions = self.get_driver_options()
        self.driver: webdriver.Chrome | None = None

    def get(self, argument: str, category: str = "default", keys: bool = False) -> str | Iterable[str]:
        return Ica.get(self.ica, argument, category, keys)

    @staticmethod
    def get_driver_options() -> webdriver.ChromeOptions:
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--detach")
        return options

    def setup(self, url: str) -> None:
        driver = webdriver.Chrome(options=self.options)
        self.driver = driver
        driver.get(url)

    def teardown(self) -> None:
        self.driver.quit()

    def get_content(self, plate: Plate, keep_alive: bool = False) -> None:
        url = plate.url
        if url:
            if not url.startswith("https://"):
                url = self.get("base_url") + plate.url
            if not self.driver:
                self.setup(url)
            else:
                self.driver.get(url)
            title = self.driver.title
            plate.title = title
            if "Group=" in url:
                sleep(3)
                host = self.driver.find_element(By.TAG_NAME, "nethys-search")
                root = host.shadow_root
                shadow_content = root.find_element(By.ID, "results")
                table = shadow_content.find_element(By.TAG_NAME, "table")
                links = table.find_elements(By.TAG_NAME, "a")
                plate.item_links["sources"] = [{"name": link.text, "url": link.get_attribute("href")} for link in links]
                print(plate.item_links)
            else:
                content = self.driver.page_source
                plate.content = content
        else:
            print("No url provided.")
        if not keep_alive:
            self.teardown()

    @staticmethod
    def extract_plate_name_and_category(plate: Plate) -> None:
        if plate.title:
            title_parts = plate.title.split('-')
            if len(title_parts) > 0:
                plate.name = title_parts[0].lower().strip().replace(' ', '_')
            if len(title_parts) > 1:
                category_parts = title_parts[1].split('(')
                plate.category = category_parts[0].lower().strip()

    @staticmethod
    def cook_from_html(html, parser: str) -> BeautifulSoup:
        raw_soup = BeautifulSoup(html, parser)
        return raw_soup

    def cook(self, plate: Plate, parser: str = 'auto', keep_alive: bool = False):
        if parser in self.parsers:
            parser = parser
        else:
            parser = self.parser
        if plate.url:
            self.get_content(plate, keep_alive=keep_alive)
            if plate.content:
                plate.status = "content"
                plate.soup = self.cook_from_html(plate.content, parser)
                self.extract_plate_name_and_category(plate)
            if plate.soup:
                plate.status = "soup"

    @staticmethod
    def request_contents(url: str) -> None | bytes:
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        print(f"Received status code {response.status_code} for url {url}")
        return None

    def cook_from_url(self, url: str):
        pass
