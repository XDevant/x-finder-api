from django.db.models import (Model, ForeignKey, BooleanField, PositiveSmallIntegerField, CASCADE, UniqueConstraint,
                             CharField)
from remaster.models.core import (Trait, Feature, Action, Ancestrie, Classe, SubClass, Skill, VersatileHeritage,
                                 Language, Heritage, AncestryFeat, SkillFeat)
from remaster.models.base import Proficiency
from remaster.models.magic import Spell



class SkillAction(Model):
    skill = ForeignKey(
        to=Skill,
        on_delete=CASCADE,
        related_name='skill_action_skills'
    )
    action = ForeignKey(
        to=Action,
        on_delete=CASCADE,
        related_name='skill_action_actions'
    )
    proficiency = CharField(
        max_length=15,
        choices=Proficiency,
        default='Untrained'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["skill", "action"], name="unique_skill_action"
            )
        ]

class SkillThroughFeat(Model):
    skill = ForeignKey(
        to=Skill,
        on_delete=CASCADE,
        related_name='skill_feat_skills'
    )
    feat = ForeignKey(
        to=SkillFeat,
        on_delete=CASCADE,
        related_name='skill_feat_feats'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["skill", "feat"], name="unique_skill_feat"
            )
        ]


class ActionTrait(Model):
    action = ForeignKey(
        to=Action,
        on_delete=CASCADE,
        related_name='trait_actions',
        null=True,
        default=None
    )
    trait = ForeignKey(
        to=Trait,
        on_delete=CASCADE,
        related_name='action_traits',
        null=True,
        default=None
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["action", "trait"], name="unique_action_trait"
            )
        ]

class ClassSkill(Model):
    choice = BooleanField(default=False)
    char_class = ForeignKey(
        to=Classe,
        on_delete=CASCADE,
        related_name='skill_classes'
    )
    skill = ForeignKey(
        to=Skill,
        on_delete=CASCADE,
        related_name='class_skills')

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["char_class", "skill"], name="unique_char_class_skill"
            )
        ]

    def __str__(self):
        add = ""
        if self.choice:
            add = "choice"
        return f"{self.char_class.name} skill {add}: {self.skill.name}"


class ClassFeature(Model):
    level = PositiveSmallIntegerField(default=0)
    char_class = ForeignKey(
        to=Classe,
        on_delete=CASCADE,
        related_name='feature_classes'
    )
    feature = ForeignKey(
        to=Feature,
        on_delete=CASCADE,
        related_name='class_features'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["char_class", "feature"], name="unique_class_feature"
            )
        ]

    def __str__(self):
        return f"{self.char_class.name} - {self.feature.name} ({self.level})"


class SubClassFeature(Model):
    level = PositiveSmallIntegerField(default=0)
    subclass = ForeignKey(
        to=SubClass,
        on_delete=CASCADE,
        related_name='feature_subclasses'
    )
    feature = ForeignKey(
        to=Feature,
        on_delete=CASCADE,
        related_name='subclass_features'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["subclass", "feature"], name="unique_subclass_feature"
            )
        ]

    def __str__(self):
        return f"{self.subclass.name} - {self.feature.name} ({self.level})"


class SpellTrait(Model):
    spell = ForeignKey(
        to=Spell,
        on_delete=CASCADE,
        related_name='spell_trait_spells'
    )
    trait = ForeignKey(
        to=Trait,
        on_delete=CASCADE,
        related_name='spell_trait_traits'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["spell", "trait"], name="unique_spell_trait"
            )
        ]

class AncestryTrait(Model):
    ancestry = ForeignKey(
        to=Ancestrie,
        on_delete=CASCADE,
        related_name='trait_ancestries'
    )
    trait = ForeignKey(
        to=Trait,
        on_delete=CASCADE,
        related_name='ancestry_traits'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["ancestry", "trait"], name="unique_ancestry_trait"
            )
        ]


class HeritageTrait(Model):
    heritage = ForeignKey(
        to=Heritage,
        on_delete=CASCADE,
        related_name='heritage_trait_heritages'
    )
    trait = ForeignKey(
        to=Trait,
        on_delete=CASCADE,
        related_name='heritage_trait_traits'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["heritage", "trait"], name="unique_heritage_trait"
            )
        ]


class AncestryLanguage(Model):
    choice = BooleanField(default=False)
    ancestry = ForeignKey(
        to=Ancestrie,
        on_delete=CASCADE,
        related_name='ancestry_language_ancestries'
    )
    language = ForeignKey(
        to=Language,
        on_delete=CASCADE,
        related_name='ancestry_language_languages'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["ancestry", "language"], name="unique_ancestry_language"
            )
        ]

    def __str__(self):
        return f"{self.ancestry.name} - {self.language.name} ({'Spoken' if self.choice else 'Can Learn'})"


class AncestryThroughFeat(Model):
    ancestry = ForeignKey(
        to=Ancestrie,
        on_delete=CASCADE,
        related_name='ancestry_feat_ancestries'
    )
    feat = ForeignKey(
        to=AncestryFeat,
        on_delete=CASCADE,
        related_name='ancestry_feat_feats'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["ancestry", "feat"], name="unique_ancestry_feat"
            )
        ]

    def __str__(self):
        return f"{self.feat.name} - {self.ancestry.name}"


class VersatileHeritageFeat(Model):
    heritage = ForeignKey(to=VersatileHeritage,
                          on_delete=CASCADE,
                          related_name='versatile_heritage_feat_herigate')
    feat = ForeignKey(to=AncestryFeat,
                      on_delete=CASCADE,
                      related_name='versatile_heritage_feat_feats',
                      default=None,
                      null=True)

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["heritage", "feat"], name="unique_versatile_heritage_feat"
            )
        ]

    def __str__(self):
        return f"{self.feat.name if self.feat else 'None'} - {self.heritage.name}"


class AncestryFeatTrait(Model):
    feat = ForeignKey(to=AncestryFeat, on_delete=CASCADE, related_name='ancestry_feat_trait_feats')
    trait = ForeignKey(to=Trait, on_delete=CASCADE, related_name='ancestry_feat_trait_traits')

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["feat", "trait"], name="unique_ancestry_feat_trait"
            )
        ]

    def __str__(self):
        return f"{self.feat.name} - {self.trait.name}"
