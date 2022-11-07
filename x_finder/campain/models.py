from django.db import models


class Source(models.Model):
    class Group(models.TextChoices):
        CORE = 'Rulebooks'
        ADV = 'Adventure'
        PATH = 'Adventure Path'
        SOCIETY = 'Pathfinder Society'
        BLOG = 'Blog'
        LOST_OMENS = 'Lost Omens'
        CASUS = 'Casus Belli'
        CUSTOM = 'Custom Source'
    title = models.CharField(max_length=25)
    subtitle = models.CharField(max_length=25)
    group = models.CharField(max_length=25, choices=Group.choices)
    paizo_url = models.URLField()
    release_date = models.DateField()
    updated = models.DateField()
    update_id = models.CharField(max_length=5)
    errata_url = models.URLField()



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



class Domain(models.Model):
    name = models.CharField(max_length=25)


class Deity(models.Model):
    name = models.CharField(max_length=25)