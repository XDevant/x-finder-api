from django.core.management.base import BaseCommand
from x_finder.core import models
from x_finder.core.scrapping import SourceSoup


class Command(BaseCommand):
    help = 'build and load source df in db, save df in csv'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        df = SourceSoup.load(name="traits", app="core", directory="Core Rulebook", suffix="finalized")
        model_list = [model for model in dir(models) if model[0].isupper()]
        print(model_list)
        list_of_items = df.to_dict('record')
        for item in list_of_items:
            models.Trait.objects.create(**item)
