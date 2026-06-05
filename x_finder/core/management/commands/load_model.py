from django.core.management.base import BaseCommand
from remaster import models
from core.fixtures.loader import FixtureLoader



class Command(BaseCommand):
    help = 'load all model items from sqlite db, save them in the app db'

    def add_arguments(self, parser):
        parser.add_argument('model')

    def handle(self, *args, **options):
        if options['model']:
            model_list = [options['model']]
        else:
            model_list = [model for model in dir(models) if model[0].isupper()]

        for model_name in model_list:
            fixture = FixtureLoader(model_name, app="remaster")
            model = getattr(models, model_name.title())
            foreign_dict = {}
            for f_model_name in fixture.fks:
                foreign_dict[f_model_name] = getattr(models, f_model_name.title())
            many_dict = {}
            for t_model_tuple in fixture.tts:
                many_dict[t_model_tuple[0]] = (getattr(models, t_model_tuple[0].title()), t_model_tuple[-1], )
            count: int = 0
            missed: list = []
            for row in fixture.records:
                for key, value in foreign_dict.items():
                    fk = value.objects.get(name=row["source"])
                    row[key] = fk
                try:
                    new_obj = model.objects.create(**row)
                    count += 1
                except Exception as e:
                    print(e)
                    missed.append(row)
                else:
                    for key, value in many_dict.items():
                        names = row[key]
                        other_model = value[0]
                        condition = value[1]
                        names = names.split(condition)
                        for name in names:
                            other_fk = other_model.get(name=name)
                            getattr(new_obj, key).set(other_fk, 'choice'= condition == 'or')
            self.stdout.write(
                self.style.SUCCESS(str(count))
            )
            self.stdout.write(
                str(missed)
            )


