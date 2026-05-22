import requests
import selenium.common
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from helpers.helpers import Plate
from typing import Any
from pandas import DataFrame
from x_finder.utils.mixins.ica import IcaMixin


class Provider(IcaMixin):
    """
    The job of the provider is to turn an url into a plate containing a beautiful soup that will be given to the Parser
    """
    parsers = ["html.parser", "html5lib"]
    parser = parsers[1]

    def __init__(self, target: str, edition: str, parser: str | None = None):
        self.target = target
        self.edition = edition
        self.ica: dict[str, dict[str, str | list[str]]] = {}
        if parser in self.parsers:
            self.parser = parser
        self.options: webdriver.ChromeOptions = self.get_driver_options()
        self.driver: webdriver.Chrome | None = None
        self.cookies: list[dict[str, Any]] | None = None

    @staticmethod
    def get_driver_options() -> webdriver.ChromeOptions:
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        return options

    def setup(self, url: str) -> None:
        driver = webdriver.Chrome(options=self.options)
        self.driver = driver
        driver.get(url)

    def teardown(self) -> None:
        if self.driver is not None:
            self.driver.quit()
            self.driver = None

    def accept_cookies(self) -> None:
        if self.driver is None:
            return
        wait = WebDriverWait(self.driver, timeout=4)
        try:
            alert = wait.until(lambda d: d.switch_to.alert)
            alert.accept()
        except selenium.common.NoAlertPresentException:
            div = self.driver.find_element(By.CLASS_NAME, "fc-consent-root")
            try:
                button = div.find_element(By.CLASS_NAME, "fc-cta-consent")
                button.click()
            except selenium.common.exceptions.NoSuchElementException:
                pass

    def extract_cookies(self) -> list[dict]:
        if self.driver:
            cookies = self.driver.get_cookies()
            return cookies
        return []

    def store_cookies(self, cookies: list[dict] | None) -> None:
        if cookies:
            home = self.sica("base_url")
            domain = home.split('//')[-1].strip(' /')
            self.cookies = cookies
            print(*cookies)

    def load_cookies(self) -> None:
        if self.driver and self.cookies:
            for cookie in self.cookies:
                self.driver.add_cookie(cookie)

    def say_hello(self):
        home = self.sica("base_url")
        if home and self.driver is not None:
            self.get_page(home)
            self.accept_cookies()
            cookies = self.extract_cookies()
            self.store_cookies(cookies)
            title = self.driver.title
            self.teardown()
            return title
        return ""

    @staticmethod
    def check_shadow_dom(url: str) -> bool:
        if "Group=" in url:
            return True
        return False

    def get_shadow_dom_links(self, plate: Plate) -> None:
        if self.driver is not None:
            host = self.driver.find_element(By.TAG_NAME, self.sica("host_tag"))
            root = host.shadow_root
            shadow_content = root.find_element(By.ID, self.sica("search_id"))
            table = shadow_content.find_element(By.TAG_NAME, self.sica("search_tag"))
            raw_links = table.find_elements(By.TAG_NAME, "a")
            links = [{"name": link.text, "url": link.get_attribute("href")} for link in raw_links]
            if links:
                plate.item_links = DataFrame.from_records(links)
                table_html = table.get_attribute("outerHTML")
                plate.content = table_html

    def get_page_content(self, plate: Plate) -> None:
        if self.driver is not None:
            content = self.driver.page_source
            plate.content = content

    def get_content(self, plate: Plate, keep_alive: bool = False) -> None:
        url = plate.url
        if url:
            self.get_page(url)
            if self.driver is not None:
                title = self.driver.title
                plate.title = title
            if keep_alive and not self.cookies:
                self.accept_cookies()
                self.extract_cookies()
                self.store_cookies(self.extract_cookies())
            if self.check_shadow_dom(url):
                self.get_shadow_dom_links(plate)
            else:
                self.get_page_content(plate)
        else:
            print("No url provided.")
        if not keep_alive:
            self.teardown()

    def get_page(self, url: str):
        if not url.startswith("https://"):
            url = self.sica("base_url") + url
        if not self.driver:
            self.setup(url)
        else:
            self.driver.get(url)

    @staticmethod
    def extract_plate_name_and_category(plate: Plate) -> None:
        if plate.title:
            title_parts = plate.title.split('-')
            if plate.name == "unknown" and len(title_parts) > 0:
                plate.name = title_parts[0].lower().strip('().,;: ').replace(' ', '_')
            if plate.category == "default" and len(title_parts) > 1:
                category_parts = title_parts[1].split('(')
                plate.category = category_parts[0].lower().strip('):,;. ').replace(' ', '_')

    def cook_from_html(self, html: str | bytes, parser: str = "") -> BeautifulSoup:
        if parser in self.parsers:
            parser = parser
        elif self.parser is not None:
            parser = self.parser
        raw_soup = BeautifulSoup(html, parser)
        return raw_soup

    def cook(self, plate: Plate, parser: str = 'auto', keep_alive: bool = False):
        if plate.url:
            self.get_content(plate, keep_alive=keep_alive)
            self.extract_plate_name_and_category(plate)
            if plate.content:
                plate.soup = self.cook_from_html(plate.content, parser)

    @staticmethod
    def request_contents(url: str) -> None | bytes:
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        print(f"Received status code {response.status_code} for url {url}")
        return None
