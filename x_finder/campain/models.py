from django.db import models


class Deity(models.Model):
    name = models.CharField(max_length=25)


class Campaign(models.Model):
    name = models.CharField(max_length=25)
