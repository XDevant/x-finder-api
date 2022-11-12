import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from django.conf import settings


def run_sql(sql):
    c = psycopg2.connect(database=settings.DATABASES['default']['USER'],
                         user=settings.DATABASES['default']['USER'],
                         password=settings.DATABASES['default']['PASSWORD'])
    c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = c.cursor()
    cur.execute(sql)
    c.close()
