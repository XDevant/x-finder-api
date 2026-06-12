from django.db.models import  Model, TextChoices, CharField, URLField, TextField
from django.contrib.admin import display

class Base(Model):
    name = CharField(max_length=25, unique=True)
    nethys_url = URLField(default="https://2e.aonprd.com")
    description = TextField(default="", blank=True)
    links = CharField(blank=True, default="")


    class Meta:
        abstract = True

    def __str__(self):
        return self.name

    @property
    @display(description="description")
    def sum_description(self):
        if len(self.description) > 175:
            return self.description[:175] + "..."
        return self.description


class Attribute(TextChoices):
    STR = 'Strength'
    DEX = 'Dexterity'
    CON = 'Constitution'
    INT = 'Intelligence'
    WIS = 'Wisdom'
    CHA = 'Charisma'


class Proficiency(TextChoices):
    U = "Untrained"
    T = "Trained"
    E = "Expert"
    M = "Master"
    L = "Legendary"


class Size(TextChoices):
    T = "Tiny"
    S = "Small"
    M = "Medium"
    L = "Large"
    H = "Huge"
    G = "Gargantuan"
