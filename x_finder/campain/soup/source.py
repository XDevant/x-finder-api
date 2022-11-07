from bs4 import BeautifulSoup
import requests


class SoupKitchen:
    base_url = "https://2e.aonprd.com/"

    def __init__(self, url):
        self.content = requests.get(self.base_url + url).content
        self.raw_soup = BeautifulSoup(self.content, 'html.parser')
        self.soup = self.raw_soup.prettify()
        self.nav_links = []

    def extract_nav_links(self):
        main = self.raw_soup.find(id="main")
        nav_links = main.find_all('a')
        self.nav_links = [link['href'] for link in nav_links]


source_url = "https://2e.aonprd.com/Sources.aspx"
