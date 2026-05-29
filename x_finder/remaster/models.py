from django.db.models import (ForeignKey, PROTECT, CASCADE, Model, TextChoices, CharField, DateField, URLField, TextField,
                              PositiveSmallIntegerField, ManyToManyField)



class Attribute(TextChoices):
    STR = 'Strength'
    DEX = 'Dexterity'
    CON = 'Constitution'
    INT = 'Intelligence'
    WIS = 'Wisdom'
    CHA = 'Charisma'


class Proficiency(TextChoices):
    Untrained = 0
    Trained = 2
    Expert = 4
    Master = 6
    Legendary = 8

class Source(Model):
    name = CharField(max_length=80)
    group = CharField(max_length=50)
    category = CharField(max_length=25, default='Custom Source')
    release_date = DateField()
    errata_date = DateField(blank=True, null=True)
    latest_errata = CharField(max_length=10, null=True)
    nethys_url = URLField()
    paizo_url = URLField()
    errata_url = URLField(blank=True, null=True)


class Trait(Model):
    name = CharField(max_length=25, unique=True)
    subtype = CharField(max_length=25)
    nethys_url = URLField()
    description = TextField(blank=True)
    description_links = TextField()
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='trait_source'
    )

class Ancestrie(Model):
    name = CharField(max_length=25, unique=True)
    nethys_url = URLField()
    description = TextField(blank=True)


class Archetype(Model):
    name = CharField(max_length=25, unique=True)
    nethys_url = URLField()
    description = TextField(blank=True)


class Background(Model):
    name = CharField(max_length=25, unique=True)
    nethys_url = URLField()
    description = TextField(blank=True)


class Heritage(Model):
    name = CharField(max_length=25, unique=True)
    nethys_url = URLField()
    description = TextField(blank=True)


class Skill(Model):
    name = CharField(max_length=25, unique=True)
    nethys_url = URLField()
    attribute = CharField(max_length=12, choices=Attribute.choices)
    description = TextField(blank=True)
    description_links = TextField(blank=True)
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='skill_source'
    )

class Feature(Model):
    name = CharField(max_length=25, unique=True)
    level = PositiveSmallIntegerField(default=0)
    description = TextField(blank=True)
    description_links = TextField(blank=True)


class Classe(Model):
    name = CharField(max_length=25, unique=True)
    nethys_url = URLField()
    key_attribute = PositiveSmallIntegerField()
    hit_points = PositiveSmallIntegerField()
    perception = PositiveSmallIntegerField(choices=Proficiency)
    fortitude = PositiveSmallIntegerField(choices=Proficiency)
    reflex = PositiveSmallIntegerField(choices=Proficiency)
    will = PositiveSmallIntegerField(choices=Proficiency)
    unarmed = PositiveSmallIntegerField(choices=Proficiency)
    simple = PositiveSmallIntegerField(choices=Proficiency)
    martial = PositiveSmallIntegerField(choices=Proficiency)
    advanced = PositiveSmallIntegerField(choices=Proficiency)
    unarmored = PositiveSmallIntegerField(choices=Proficiency)
    light = PositiveSmallIntegerField(choices=Proficiency)
    medium = PositiveSmallIntegerField(choices=Proficiency)
    heavy = PositiveSmallIntegerField(choices=Proficiency)
    spells = PositiveSmallIntegerField(choices=Proficiency)
    class_dc = PositiveSmallIntegerField(choices=Proficiency)
    description = TextField(blank=True)
    during_combat_encounters = TextField()
    during_social_encounters = TextField()
    while_exploring = TextField()
    in_downtime = TextField()
    you_might = TextField()
    others_probably = TextField()
    free_skills = PositiveSmallIntegerField()
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='class_source'
    )
    skill_trainings = ManyToManyField(Skill, through='ClassSkill')
    features = ManyToManyField(Feature, through='ClassFeature')


class ClassSkill(Model):
    char_class = ForeignKey(
        to=Classe,
        on_delete=CASCADE,
        related_name='skill_class'
    )
    skill = ForeignKey(
        to=Skill,
        on_delete=CASCADE,
        related_name='class_skill')

class ClassFeature(Model):
    level = PositiveSmallIntegerField()
    char_class = ForeignKey(
        to=Classe,
        on_delete=CASCADE,
        related_name='feature_class'
    )
    feature = ForeignKey(
        to=Feature,
        on_delete=CASCADE,
        related_name='class_feature'
    )


class Action(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Spell(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Ritual(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Domain(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Deitie(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Condition(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Curse(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Disease(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Hazard(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)



class Poison(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Creature(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class Equipment(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)



class SubClass(Model):
    name = CharField(max_length=25, unique=True)
    nethys_url = URLField()
    alternate_key_attribute = PositiveSmallIntegerField()
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='subclass_source'
    )


class Familiar(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)


class AnimalCompanion(Model):
    name = CharField(max_length=25)
    description = TextField(blank=True)

