from django.core.management.base import BaseCommand
from django.conf import settings
from x_finder.remaster import models
from x_finder.core.fixtures.loader import LoadFixture



class Command(BaseCommand):
    help = 'build and load source df in db, save df in csv'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        model_list = [model for model in dir(models) if model[0].isupper()]
        print(model_list)
        for model_name in model_list:
            fixture = LoadFixture(model_name, app="remaster")
            model = getattr(models, model_name)
            count: int = 0
            missed: list = []
            for row in fixture.records:
                try:
                    model.objects.create(**row)
                    count += 1
                except Exception as e:
                    print(e)
                    missed.append(row)
            print(f"inserted: {count}", f"missed: ", *missed)

