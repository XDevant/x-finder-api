from django.core.management.base import BaseCommand
from django.conf import settings
from core.models import Source
from core.scrapping import SourceSoup


class Command(BaseCommand):
    help = 'build and load source df in db'

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
