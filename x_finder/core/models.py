from django.db import models


class Proficiency(models.TextChoices):
    Untrained = 0
    Trained = 2
    Expert = 4
    Master = 6
    Legendary = 8


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


class Trait(models.Model):
    name = models.CharField(max_length=25, unique=True)
    subtype = models.CharField(max_length=25)
    nethys_url = models.URLField()
    source = models.ForeignKey(
        to=Source,
        on_delete=models.PROTECT,
        related_name='trait_source'
    )
    source_page = models.PositiveSmallIntegerField()
    description = models.TextField()
    description_links = models.TextField()


class Skills(models.Model):
    name = models.CharField(max_length=25, unique=True)
    nethys_url = models.URLField()
    source = models.ForeignKey(
        to=Source,
        on_delete=models.PROTECT,
        related_name='skill_source'
    )
    source_page = models.PositiveSmallIntegerField()


class Feature(models.Model):
    name = models.CharField(max_length=25, unique=True)
    description = models.TextField()
    description_links = models.TextField()


class CharClass(models.Model):
    name = models.CharField(max_length=25, unique=True)
    nethys_url = models.URLField()
    source = models.ForeignKey(
        to=Source,
        on_delete=models.PROTECT,
        related_name='class_source'
    )
    source_page = models.PositiveSmallIntegerField()
    key_attribute = models.PositiveSmallIntegerField()
    hit_points = models.PositiveSmallIntegerField()
    perception = models.PositiveSmallIntegerField(choices=Proficiency)
    fortitude = models.PositiveSmallIntegerField(choices=Proficiency)
    reflex = models.PositiveSmallIntegerField(choices=Proficiency)
    will = models.PositiveSmallIntegerField(choices=Proficiency)
    unarmed = models.PositiveSmallIntegerField(choices=Proficiency)
    simple = models.PositiveSmallIntegerField(choices=Proficiency)
    martial = models.PositiveSmallIntegerField(choices=Proficiency)
    advanced = models.PositiveSmallIntegerField(choices=Proficiency)
    unarmored = models.PositiveSmallIntegerField(choices=Proficiency)
    light = models.PositiveSmallIntegerField(choices=Proficiency)
    medium = models.PositiveSmallIntegerField(choices=Proficiency)
    heavy = models.PositiveSmallIntegerField(choices=Proficiency)
    spells = models.PositiveSmallIntegerField(choices=Proficiency)
    class_dc = models.PositiveSmallIntegerField(choices=Proficiency)
    description = models.TextField()
    during_combat_encounters = models.TextField()
    during_social_encounters = models.TextField()
    while_exploring = models.TextField()
    in_downtime = models.TextField()
    you_might = models.TextField()
    others_probably = models.TextField()
    free_skills = models.PositiveSmallIntegerField()
    skill_trainings = models.ManyToManyField(Skills, through='ClassSkills')
    features = models.ManyToManyField(Feature, through='ClassFeature')


class ClassFeature(models.Model):
    char_class = models.ForeignKey(
        to=CharClass,
        on_delete=models.CASCADE,
        related_name='feature_class'
    )
    feature = models.ForeignKey(
        to=Feature,
        on_delete=models.CASCADE,
        related_name='class_feature'
    )
    level = models.PositiveSmallIntegerField()


class SubClass(models.Model):
    name = models.CharField(max_length=25, unique=True)
    nethys_url = models.URLField()
    source = models.ForeignKey(
        to=Source,
        on_delete=models.PROTECT,
        related_name='subclass_source'
    )
    source_page = models.PositiveSmallIntegerField()
    alternate_key_attribute = models.PositiveSmallIntegerField()


class Domain(models.Model):
    name = models.CharField(max_length=25)
