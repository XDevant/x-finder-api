from django.db.models import  ( Model, 
                               TextChoices,
                                CharField,
                                URLField,
                                TextField,
                                ManyToManyField,
                                SmallIntegerField,
                                ForeignKey,
                                CASCADE,
                                UniqueConstraint )
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.admin import display



class Base(Model):
    name = CharField(unique=True)
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


class SourceMixin(Model):
    sources = ManyToManyField(to='Source',
                              related_name='%(class)s_sources',
                              through='SourceThrough' + '%(class)s'.title(),
                              blank=True)

    class Meta:
        abstract = True


class TraitMixin(Model):
    traits = ManyToManyField(to='Trait',
                             related_name='%(class)s_traits',
                             through='TraitThrough' + '%(class)s'.title(),
                             blank=True)

    class Meta:
        abstract = True


class SourceThrough(Model):
    source = ForeignKey(to='Source', on_delete=CASCADE, related_name='%(class)s_sources')
    source_page = SmallIntegerField(default=0)
    item = ForeignKey(to='%(class)s'.split("Through")[-1], on_delete=CASCADE, related_name='%(class)s_items')

    class Meta:
        abstract = True
        constraints = [
            UniqueConstraint(
                fields=["source", "item"], name="unique_%(class)s"
            )
        ]

    def __str__(self):
        return f"{self.source.name} p.{self.source_page}"


class TraitThrough(Model):
    item = ForeignKey(to='%(class)s'.split("Through")[-1], on_delete=CASCADE, related_name='%(class)s_traits')
    trait = ForeignKey(to='Trait', on_delete=CASCADE, related_name='%(class)s_traits')
    trait_value = CharField(max_length=55, blank=True, default="")

    class Meta:
        abstract = True
        constraints = [
            UniqueConstraint(
                fields=["trait", "item"], name="unique_%(class)s"
            )
        ]

    def __str__(self):
        return f"{self.trait.name} {self.trait_value}"


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

class ArmorCategory(TextChoices):
    U = "Unarmored"
    L = "Light"
    M = "Medium"
    H = "Heavy"

class WeaponCategory(TextChoices):
    U = "Unarmed"
    S = "Simple"
    M = "Martial"
    A = "Advanced"

class WeaponType(TextChoices):
    M = "Melee"
    R = "Ranged"
