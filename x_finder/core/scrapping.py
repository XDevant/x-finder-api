from utils.scrapp import SoupKitchen


source_url = "https://2e.aonprd.com/Sources.aspx"


class SourceSoup(SoupKitchen):
    """
    A soup kitchen specific for sources extraction that creates a csv/df matching the model.
    """
    def norm_df(self, df):
        return df

    @staticmethod
    def format_df(df):
        df["release_date"] = [SoupKitchen.translate_date(date) for date in df["release_date"]]
        df["errata_date"] = df.apply(
            lambda r: SoupKitchen.translate_date(
                str(r["latest_errata"]).split(' - ')[-1]
                ) if r["latest_errata"] else None,
            axis=1)
        df["errata_version"] = df.apply(
            lambda r: str(r["latest_errata"]).split(' - ')[0].strip() if r["latest_errata"] else "-",
            axis=1)
        df = df[["name",
                 "group",
                 "category",
                 "release_date",
                 "errata_date",
                 "errata_version",
                 "nethys_url",
                 "product_page_url",
                 "latest_errata_url"]]
        df.rename(columns={'product_page_url': 'paizo_url', 'latest_errata_url': 'errata_url'},
                  inplace=True)
        return df

    @staticmethod
    def cook():
        bowl = SourceSoup("Sources.aspx",
                          app="core",
                          text_complement_columns=["Latest Errata"],
                          url_complement_columns=["Product Page", "Latest Errata"],
                          tail_start='<br>')
        return bowl


if __name__ == "__main__":
    # SourceSoup.cook()
    pass
