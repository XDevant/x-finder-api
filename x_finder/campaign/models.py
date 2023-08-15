from django.db import models
from core.models import Source


class Deity(models.Model):
    name = models.CharField(max_length=25)
    source = models.ForeignKey(to=Source, on_delete=models.CASCADE)
    source_page = models.IntegerField


class Campaign(models.Model):
    name = models.CharField(max_length=25)


class Pantheon(models.Model):
    """A pantheon is a collection of gods relative to a culture.
    A campaign can have several, like Romans and Greek god for example."""
    name = models.CharField(max_length=25)
