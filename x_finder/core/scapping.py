from soup.scrapp import SoupKitchen


source_url = "https://2e.aonprd.com/Sources.aspx"


class SourceSoup(SoupKitchen):
    def run(self):
        pass


if __name__ == "__main__":
    bowl = SoupKitchen("Sources.aspx")
    bowl.extract_nav_links()
    links = bowl.nav_links
    sub_bowl = SoupKitchen(links['Rulebooks'])
    sub_bowl.load_table(text_complement_columns=["Latest Errata"],
                        url_complement_columns=["Product Page", "Latest Errata"],
                        tail_start='<br/>')
