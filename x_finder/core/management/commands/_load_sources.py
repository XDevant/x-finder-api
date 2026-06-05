from django.core.management.base import BaseCommand
from django.conf import settings
from x_finder.core.models import Source
from x_finder.core.scrapping import SourceSoup


class Command(BaseCommand):
    help = 'build and load source df in db, save df in csv'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        df = SourceSoup.load_fixture("core", "Sources")
        df = SourceSoup.format_df(df)
        list_of_sources = df.to_dict('record')
        for source in list_of_sources:
            Source.objects.create(**source)
        pathfile = f"{settings.BASE_DIR}\\core\\fixtures\\Sources.csv"
        SourceSoup.df_to_csv(pathfile, df)
