from django.core.management.base import BaseCommand
from django.core.management import call_command
from psycopg2.errors import ObjectInUse, OperationalError
import os
import environ
from pathlib import Path
from utils.prettyprints import PRR
from utils.utils import run_sql
from authentication.models import User, UserManager


BASE_DIR = Path(__file__).resolve().parent.parent

environ.Env.read_env(os.path.join(BASE_DIR, '.env'))


class Command(BaseCommand):
    help = 'create db, drops db if exists'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        name = "X-Finder"
        sql_1 = f'DROP DATABASE IF EXISTS "{name}"'
        sql_2 = f'CREATE DATABASE "{name}"'
        try:
            run_sql(sql_1)
            run_sql(sql_2)
            print(f"Database {PRR.colorize(name, True)} successfully created")
        except ObjectInUse:
            db = PRR.colorize(name, False)
            print(f"DB {db} already in use, make sure to quit pgAdmin")
        except OperationalError:
            db = PRR.colorize(name, False)
            print(f"Unable to connect to {db}, credentials are in config.py.")
        except Exception:
            db = PRR.colorize(name, False)
            print(f"Unable to create database {db}")

        call_command("makemigrations")
        call_command("migrate")

        User.objects.create_superuser(os.environ.get("SUPER_NAME"),
                                      email=None,
                                      password=os.environ.get("SUPER_PASSWORD"))
