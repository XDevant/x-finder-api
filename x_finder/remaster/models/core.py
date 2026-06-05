from remaster.models.base import Base, Attribute, Proficiency
from django.db.models import (PROTECT, ForeignKey, CharField, URLField, DateField, TextField, PositiveSmallIntegerField,
                              ManyToManyField)
from django.contrib.admin import display


class Source(Base):
    group = CharField(max_length=50, default='Custom Source')
    category = CharField(max_length=25, default='-')
    release_date = DateField()
    errata_date = CharField(max_length=15, default="-", null=True)
    errata_version = CharField(max_length=25, default="-", blank=True)
    paizo_url = URLField()



class Trait(Base):
    subtype = CharField(max_length=25)
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='trait_source'
    )

    def __str__(self):
        return self.name + " - " + self.subtype


class Feature(Base):
    level = PositiveSmallIntegerField(default=0)


class Skill(Base):
    attribute = CharField(max_length=12, choices=Attribute)
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='skill_source'
    )


class Action(Base):
    pass


class Ancestrie(Base):
    pass


class AnimalCompanion(Base):
    pass


class Archetype(Base):
    pass


class Background(Base):
    pass


class Classe(Base):
    key_attribute = CharField(max_length=12, choices=Attribute)
    alt_key_attribute = CharField(max_length=12, choices=Attribute, default="", blank=True)
    hit_points = PositiveSmallIntegerField()
    perception = CharField(max_length=10, choices=Proficiency)
    fortitude = CharField(max_length=10, choices=Proficiency)
    reflex = CharField(max_length=10, choices=Proficiency)
    will = CharField(max_length=10, choices=Proficiency)
    unarmed = CharField(max_length=10, choices=Proficiency)
    simple = CharField(max_length=10, choices=Proficiency)
    martial = CharField(max_length=10, choices=Proficiency)
    advanced = CharField(max_length=10, choices=Proficiency)
    favored = CharField(max_length=10, choices=Proficiency, default="Untrained")
    unarmored = CharField(max_length=10, choices=Proficiency)
    light = CharField(max_length=10, choices=Proficiency)
    medium = CharField(max_length=10, choices=Proficiency)
    heavy = CharField(max_length=10, choices=Proficiency)
    spells = CharField(max_length=10, choices=Proficiency)
    class_dc = CharField(max_length=10, choices=Proficiency)
    free_skills = PositiveSmallIntegerField()
    during_combat_encounters = TextField()
    during_social_encounters = TextField()
    while_exploring = TextField()
    in_downtime = TextField()
    you_might = TextField()
    others_probably = TextField()
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='source_classes'
    )
    skills = ManyToManyField(Skill, through='ClassSkill')
    features = ManyToManyField(Feature, through='ClassFeature')

    @property
    @display(description="others probably")
    def sum_others_probably(self):
        if len(self.others_probably) > 75:
            return self.others_probably[:75] + "..."
        return self.others_probably

    @property
    @display(description="you might")
    def sum_you_might(self):
        if len(self.you_might) > 75:
            return self.you_might[:75] + "..."
        return self.you_might

    @property
    @display(description="in downtime")
    def sum_in_downtime(self):
        if len(self.in_downtime) > 75:
            return self.in_downtime[:75] + "..."
        return self.in_downtime

    @property
    @display(description="while exploring")
    def sum_while_exploring(self):
        if len(self.while_exploring) > 75:
            return self.while_exploring[:75] + "..."
        return self.while_exploring

    @property
    @display(description="social encounters")
    def sum_during_social_encounters(self):
        if len(self.during_social_encounters) > 75:
            return self.during_social_encounters[:75] + "..."
        return self.during_social_encounters

    @property
    @display(description="combat encounters")
    def sum_during_combat_encounters(self):
        if len(self.during_combat_encounters) > 75:
            return self.during_combat_encounters[:75] + "..."
        return self.during_combat_encounters


class Condition(Base):
    pass


class Creature(Base):
    pass

class Curse(Base):
    pass

class Disease(Base):
    pass

class Domain(Base):
    pass

class Equipment(Base):
    pass

class Familiar(Base):
    pass

class Hazard(Base):
    pass

class Heritage(Base):
    pass

class Poison(Base):
    pass

class Ritual(Base):
    rank = PositiveSmallIntegerField(default=1)

class Spell(Base):
    rank = PositiveSmallIntegerField(default=1)
    spell_type = CharField(blank=True, default="")
    cast = CharField(blank=True, default="")
    trigger = CharField(blank=True, default="")
    range = CharField(blank=True, default="")
    area = CharField(blank=True, default="")
    defense = CharField(blank=True, default="")
    targets = CharField(blank=True, default="")
    duration = CharField(blank=True, default="")
    requirements = CharField(blank=True, default="")
    critical_success = CharField(blank=True, default="")
    success = CharField(blank=True, default="")
    failure = CharField(blank=True, default="")
    critical_failure = CharField(blank=True, default="")
    heightened = CharField(blank=True, default="")


class SubClass(Base):
    alternate_key_attribute = PositiveSmallIntegerField()
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='subclass_source'
    )
