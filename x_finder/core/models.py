from django.db import models


class Source(models.Model):
    class Group(models.TextChoices):
        CORE = 'Rulebooks'
        COMICS = 'Comics'
        ADV = 'Adventures'
        PATH = 'Adventure Paths'
        SOCIETY = 'Society'
        BLOG = 'Blog Posts'
        LOST_OMENS = 'Lost Omens'
        CASUS = 'Casus Belli'
        CUSTOM = 'Custom Source'
    name = models.CharField(max_length=80)
    group = models.CharField(max_length=50)
    category = models.CharField(max_length=25, choices=Group.choices)
    release_date = models.DateField(blank=True, null=True)
    errata_date = models.DateField(blank=True, null=True)
    errata_version = models.CharField(max_length=10)
    nethys_url = models.URLField()
    paizo_url = models.URLField()
    errata_url = models.URLField(blank=True, null=True)


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


class Trait(models.Model):
    name = models.CharField(max_length=25)
    subtype = models.CharField(max_length=25)
    nethys_url = models.URLField()
    source = models.ForeignKey(
        to=Source,
        on_delete=models.PROTECT,
        related_name='trait_source'
    )
    source_page = models.IntegerField()
    description = models.TextField()


class Domain(models.Model):
    name = models.CharField(max_length=25)

