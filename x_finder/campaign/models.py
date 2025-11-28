from django.db import models
from ..core.models import Source


class Deity(models.Model):
    name = models.CharField(max_length=25)
    source = models.ForeignKey(to=Source, on_delete=models.CASCADE)
    source_page = models.IntegerField


class Campaign(models.Model):
    name = models.CharField(max_length=25)


class Language(models.Model):
    class Rarity(models.TextChoices):
        COMMON = 'Common'
        UNCOMMON = 'Uncommon'
        RARE = 'Rare'
        SECRET = 'Secret'
    name = models.CharField(max_length=25)
    rarity = models.CharField(max_length=10, choices=Rarity.choices)
    source = models.ForeignKey(
        to=Source,
        on_delete=models.PROTECT,
        related_name='language_source'
    )


class Pantheon(models.Model):
    """A pantheon is a collection of gods relative to a culture.
    A campaign can have several, like Romans and Greek gods for example."""
    name = models.CharField(max_length=25)


class Faction(models.Model):
    class Type(models.TextChoices):
        PRIVATE = 'Private Company'
        EXECUTIVE = 'Governmental Organisation'
        PUBLIC = 'Non Governmental Organisation'
        SECRET = 'Secret Organisation'
    name = models.CharField(max_length=25)
    home = models.CharField(max_length=25)
    type = models.CharField(max_length=10, choices=Type.choices)
    purpose = models.TextField(max_length=60)
    activities = models.TextField(max_length=200)


class Npc(models.Model):
    name = models.CharField(max_length=25)
    level = models.IntegerField
    rounds = models.IntegerField
    threshold = models.IntegerField
    home = models.CharField(max_length=25)
    faction = source = models.ForeignKey(
        to=Faction,
        on_delete=models.PROTECT,
        related_name='npc_faction'
    )
    masquerade = models.ForeignKey(
        to=Faction,
        on_delete=models.PROTECT,
        related_name='npc_fake_faction'
    )
    Appearance = models.TextField(max_length=60)
    Personality = models.TextField(max_length=60)
    Background = models.TextField(max_length=100)
    Taboo = models.TextField(max_length=60)
    Resistance = models.TextField(max_length=60)
    Weakness = models.TextField(max_length=60)
    Perception = models.IntegerField
