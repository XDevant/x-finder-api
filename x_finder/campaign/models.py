from django.db import models
from core.models import Source


class Deity(models.Model):
    name = models.CharField(max_length=25)
    source = models.ForeignKey(to=Source, on_delete=models.CASCADE)
    source_page = models.IntegerField


class Campaign(models.Model):
    name = models.CharField(max_length=25)


class Pantheon(models.Model):
    name = models.CharField(max_length=25)
