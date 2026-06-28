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
                if f_model_name.endswith("_spell"):
                    f_model = "Spell"
                else:
                    f_model = f_model_name.title()
                foreign_dict[f_model_name] = getattr(models, f_model)
            many_dict = {}
            for t_model_dict in fixture.tts:
                name = str(t_model_dict["name"])
                many_dict[name] = {"model": getattr(models, name.strip("_")[:-1].title()),
                                   "condition": t_model_dict["condition"],
                                   "defaults": t_model_dict["defaults"]}
            count: int = 0
            missed: list = []
            for row in fixture.records:
                for key, value in foreign_dict.items():
                    try:
                        fk = value.objects.get(name=row[key])
                        row[key] = fk
                    except Exception as e:
                        self.stdout.write(str(e))
                        self.stdout.write(str(row[key]))
                        missed.append(row)
                        continue
                names_dict: dict[str, list[str] | str] = {key : row.pop(key) for key in many_dict.keys()}
                for key, value in many_dict.items():
                    names =str(names_dict[key])
                    condition = value["condition"]
                    if "and" in condition:
                        condition = ", "
                    names = names.split(condition)
                    names_dict[key] = [name.strip() for name in names if name.strip()]
                try:
                    new_obj = model.objects.create(**row)
                    count += 1
                except Exception as e:
                    self.stdout.write(str(e))
                    missed.append(row)
                else:
                    for key, value in many_dict.items():
                        other_model = value["model"]
                        condition = value["condition"]
                        defaults = value["defaults"]
                        through_defaults = {default.strip(): row.pop(default.strip()) for default in defaults if default.strip()}
                        if condition == ' or ':
                            through_defaults['choice'] = True
                        elif "language" in key:
                            through_defaults['choice'] = False
                        for name in names_dict[key]:
                            try:
                                other_fk = other_model.objects.get(name=name.strip("_"))
                            except Exception as e:
                                self.stdout.write(str(e))
                                self.stdout.write(str(name))
                            else:
                                getattr(new_obj, key.strip("_")).add(other_fk, through_defaults=through_defaults)
                                self.stdout.write(str(other_fk))
                                self.stdout.write(str(key))
                                self.stdout.write(str(through_defaults))
            self.stdout.write(
                self.style.SUCCESS(str(count))
            )
            self.stdout.write(
                str(missed)
            )
