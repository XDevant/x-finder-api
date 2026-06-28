from remaster.models.base import Base, Attribute, Proficiency, Size
from django.db.models import (PROTECT, CASCADE, ForeignKey, CharField, URLField, DateField, TextField, PositiveSmallIntegerField,
                              ManyToManyField, BooleanField)
from django.contrib.admin import display


class Source(Base):
    group = CharField(max_length=50, default='Custom Source')
    category = CharField(max_length=25, default='-')
    release_date = DateField()
    errata_date = CharField(max_length=15, default="-", null=True)
    errata_version = CharField(max_length=25, default="-", blank=True)
    paizo_url = URLField()


class Trait(Base):
    subtype = CharField(max_length=25, default='-')
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='trait_source'
    )

    def __str__(self):
        return self.name + " - " + self.subtype


class Feature(Base):
    subtype = CharField(max_length=25, default='-')

    def __str__(self):
        return self.name + " - " + self.subtype


class Language(Base):
    subtype = CharField(max_length=25, default='-')
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='language_source',
        null=True,
        default=None
    )
    source_page = PositiveSmallIntegerField(default=0)

    def __str__(self):
        return self.name + " - " + self.subtype


class Action(Base):
    subtype = CharField(max_length=25, default='-')
    action = CharField(max_length=15, default="-")
    critical_success = CharField(max_length=50, default="-")
    success = CharField(max_length=50, default="-")
    failure = CharField(max_length=50, default="-")
    critical_failure = CharField(max_length=50, default="-")
    traits = ManyToManyField(Trait, through='ActionTrait')


class SkillFeat(Base):
    level = PositiveSmallIntegerField(default=0)
    required_proficiency = CharField(
        max_length=15,
        choices=Proficiency,
        default='Untrained'
    )


class Skill(Base):
    attribute = CharField(max_length=12, choices=Attribute)
    actions = ManyToManyField(Action, through='SkillAction', related_name='skill_actions')
    feats = ManyToManyField(SkillFeat, through='SkillThroughFeat', related_name='skill_feats')
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='skill_source'
    )

    def __str__(self):
        return f"{self.name} - {self.attribute}"


class AncestryFeat(Base):
    """Model for all ancestry feats, either tied to an ancestry or a versatile héritage, who each have their own TT"""
    level = PositiveSmallIntegerField(choices={1: 1, 5: 5, 9: 9, 13: 13, 17: 17}, default=1)
    source = ForeignKey(to=Source, on_delete=PROTECT, related_name='ancestry_feat_source')
    source_page = PositiveSmallIntegerField(default=0)
    traits = ManyToManyField(Trait, through='AncestryFeatTrait')

    def __str__(self):
        return f"{self.name} - {self.level}"


class Ancestrie(Base):
    hit_points = PositiveSmallIntegerField(default=2)
    size = CharField(max_length=12, choices=Size, default='Medium')
    speed = PositiveSmallIntegerField(default=5)
    attribute_flaw = CharField(max_length=12, choices=Attribute, default="", blank=True)
    attribute_bonus_1 = CharField(max_length=12, choices=Attribute, default="", blank=True)
    attribute_bonus_2 = CharField(max_length=12, choices=Attribute, default="", blank=True)
    free_bonus = PositiveSmallIntegerField(default=1)
    augmented_sense = CharField(default="Normal vision")
    special = CharField(default="-")
    free_languages = PositiveSmallIntegerField(default=0)
    you_might = TextField(default="-")
    others_probably = TextField(default="-")
    physical_description = TextField(default="-")
    society = TextField(default="-")
    beliefs = CharField(default="-")
    common_names= CharField(default="-")
    traits = ManyToManyField(Trait, through='AncestryTrait')
    feats = ManyToManyField(AncestryFeat, through='AncestryThroughFeat', related_name='ancestry_feats')
    languages = ManyToManyField(Language, through='AncestryLanguage', related_name='ancestry_languages', related_query_name='ancestrylanguages')
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='ancestry_source',
        null=True,
        default=None
    )
    source_page = PositiveSmallIntegerField(default=0)

    @property
    def spoken_languages(self):
        return self.languages.filter(ancestry_language_languages__choice=False).filter(ancestry_language_languages__ancestry=self.pk)

    @property
    def starting_languages(self):
        return self.languages.filter(ancestry_language_languages__choice=True).filter(ancestry_language_languages__ancestry=self.pk)

    @property
    def ancestry_feats(self):
        return self.feats.filter(ancestry_feat_feats__ancestry=self.pk)

    @property
    def heritages(self):
        return Heritage.objects.filter(ancestrie=self.pk)


class AnimalCompanion(Base):
    pass


class Archetype(Base):
    pass


class Background(Base):
    boost_choice_1 = CharField(max_length=12, choices=Attribute, default="", blank=True)
    boost_choice_2 = CharField(max_length=12, choices=Attribute, default="", blank=True)
    free_boosts = PositiveSmallIntegerField(default=0)
    skill_training = ForeignKey(
        to=Skill,
        on_delete=PROTECT,
        related_name='skill_training_background',
        null=True,
        default=None
    )
    lore = CharField(default="Lore: ?")
    bonus_skill_feat = ForeignKey(
        to=Feature,
        on_delete=PROTECT,
        related_name='bonus_feat_background',
        null = True,
        default=None
    )
    rarity = ForeignKey(
        to=Trait,
        on_delete=PROTECT,
        related_name='trait_background',
        null = True,
        default = None
    )
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='background_source',
        null=True,
        default=None
    )
    source_page = PositiveSmallIntegerField(default=0)


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
    free_skills = PositiveSmallIntegerField(default=0)
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
    skill_choices = ManyToManyField(Skill, through='ClassSkill')
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

class Equipment(Base):
    pass

class Familiar(Base):
    pass

class Hazard(Base):
    pass


class Heritage(Feature):
    ancestrie = ForeignKey(
        to=Ancestrie,
        on_delete=CASCADE,
        related_name='heritage_ancestry',
        default=None,
        null=True
    )
    source = ForeignKey(
        to=Source,
        on_delete=CASCADE,
        related_name='heritage_source'
    )
    source_page = PositiveSmallIntegerField(default=0)
    traits = ManyToManyField(Trait, through='HeritageTrait', related_name='heritage_traits')


class VersatileHeritage(Feature):
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='versatile_heritage_source'
    )
    feats = ManyToManyField(AncestryFeat,
                            through='VersatileHeritageFeat',
                            related_name='versatile_heritage_feats')


class Poison(Base):
    pass

class Ritual(Base):
    rank = PositiveSmallIntegerField(default=1)

class Spell(Base):
    rank = PositiveSmallIntegerField(default=1)
    arcane = BooleanField(default=False)
    divine = BooleanField(default=False)
    occult = BooleanField(default=False)
    primal = BooleanField(default=False)
    spell_type = CharField(blank=True, default="")
    action = CharField(blank=True, default="")
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
    traits = ManyToManyField(Trait, through='SpellTrait', related_name='spell_traits')
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='spell_source',
        default=None,
        null=True
    )
    source_page = PositiveSmallIntegerField(default=0)

    @property
    def traditions(self):
        answer = []
        for tradition in ["arcane", "divine", "occult", "primal"]:
            if self.__getattribute__(tradition):
                answer.append(tradition)
        return answer


class Domain(Base):
    domain_spell = ForeignKey(to=Spell,
                              on_delete=PROTECT,
                              related_name='domain_spell',
                              default=None,
                              null=True)
    advanced_domain_spell = ForeignKey(to=Spell,
                                       on_delete=PROTECT,
                                       related_name='advanced_domain_spell',
                                       default=None,
                                       null=True)
    source = ForeignKey(to=Source, on_delete=PROTECT, default=None, null=True, related_name="domain_source")
    source_page = PositiveSmallIntegerField(default=0)


class SubClass(Base):
    classe = ForeignKey(
        to=Classe,
        on_delete=PROTECT,
        related_name='classe',
        null=True,
        default=None
    )
    alternate_key_attribute = PositiveSmallIntegerField()
    source_page = PositiveSmallIntegerField()
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='subclass_source'
    )
    features = ManyToManyField(Feature,
                               through='SubClassFeature')
