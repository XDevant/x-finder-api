from django.db import models
from core.models import Source


class Deity(models.Model):
    name = models.CharField(max_length=25)


class Campaign(models.Model):
    name = models.CharField(max_length=25)
