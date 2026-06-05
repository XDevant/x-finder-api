from rest_framework.serializers import HyperlinkedModelSerializer, StringRelatedField, CharField
from .models import (Source, Trait, Feature, Classe, Skill, Ancestrie, Background, Heritage, Archetype, Domain, Deitie,
                     Spell, Ritual, Creature, Condition, Curse, Disease, Hazard, Poison, Action, Equipment, 
                     AnimalCompanion, Familiar)

# Source - https://stackoverflow.com/a/51831098
# Posted by zeynel
# Retrieved 2026-06-02, License - CC BY-SA 4.0
class CustomCharField(CharField):

    def __init__(self, repr_length, **kwargs):
        self.repr_length = repr_length
        super(CustomCharField, self).__init__(**kwargs)

    def to_representation(self, value):
        return super(CustomCharField, self).to_representation(value)[:self.repr_length]


class SourceListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Source
        fields = ["pk", "name", "category", "group", "errata_version", "release_date", "errata_date", "paizo_url"]


class SourceDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Source
        fields = ["pk", "name", "category", "group", "errata_version", "release_date", "errata_date", "nethys_url", "paizo_url"]


class SourceSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = SourceListSerializer
    detail = SourceDetailSerializer


class TraitListSerializer(HyperlinkedModelSerializer):
    source = StringRelatedField()

    class Meta:
        model = Trait
        fields = ["pk", "name", "subtype", "description", "source", "source_page"]


class TraitDetailSerializer(HyperlinkedModelSerializer):
    source = SourceDetailSerializer()

    class Meta:
        model = Trait
        fields = ["pk", "name", "subtype", "description", "source", "source_page"]


class TraitSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = TraitListSerializer
    detail = TraitDetailSerializer


class FeatureListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Feature
        fields = ["name", "description"]


class FeatureDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Feature
        fields = ["name", "description"]


class FeatureSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = FeatureListSerializer
    detail = FeatureDetailSerializer


class ClasseListSerializer(HyperlinkedModelSerializer):
    source = StringRelatedField()

    class Meta:
        model = Classe
        fields = ["name", "nethys_url", "key_attribute", "alt_key_attribute", "hit_points", "perception",
                  "fortitude", "reflex", "will", "skill", "free_skills", "unarmed", "simple", "martial", "advanced",
                  "favored", "unarmored", "light", "medium", "heavy", "spells", "class_dc", "source", "source_page"]


class ClasseDetailSerializer(HyperlinkedModelSerializer):
    source = SourceDetailSerializer()
    sum_description = CharField()

    class Meta:
        model = Classe
        fields = ["name", "nethys_url", "key_attribute", "alternate_key_attribute", "hit_points", "perception",
                  "fortitude", "reflex", "will", "skill", "free_skills", "unarmed", "simple", "martial", "advanced",
                  "favored", "unarmored", "light", "medium", "heavy", "spells", "class_dc", "during_combat_encounters",
                  "during_social_encounters", "while_exploring", "in_downtime", "you_might", "others_probably",
                  "sum_description", "source", "source_page"]


class ClasseSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = ClasseListSerializer
    detail = ClasseDetailSerializer

class AncestrieListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Ancestrie
        fields = ["name", "description"]


class AncestrieDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Ancestrie
        fields = ["name", "description"]


class AncestrieSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = AncestrieListSerializer
    detail = AncestrieDetailSerializer

class BackgroundListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Background
        fields = ["name", "description"]


class BackgroundDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Background
        fields = ["name", "description"]


class BackgroundSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = BackgroundListSerializer
    detail = BackgroundDetailSerializer


class HeritageListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Heritage
        fields = ["name", "description"]


class HeritageDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Heritage
        fields = ["name", "description"]


class HeritageSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = HeritageListSerializer
    detail = HeritageDetailSerializer


class SkillListSerializer(HyperlinkedModelSerializer):
    source = StringRelatedField()

    class Meta:
        model = Skill
        fields = ["name", "attribute", "description", "nethys_url", "source"]


class SkillDetailSerializer(HyperlinkedModelSerializer):
    source = SourceDetailSerializer()

    class Meta:
        model = Skill
        fields = ["name", "attribute", "description", "nethys_url", "source"]


class SkillSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = SkillListSerializer
    detail = SkillDetailSerializer


class ArchetypeListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Archetype
        fields = ["name", "description"]


class ArchetypeDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Archetype
        fields = ["name", "description"]


class ArchetypeSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = ArchetypeListSerializer
    detail = ArchetypeDetailSerializer


class DomainListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Domain
        fields = ["name", "description"]


class DomainDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Domain
        fields = ["name", "description"]


class DomainSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = DomainListSerializer
    detail = DomainDetailSerializer


class DeitieListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Deitie
        fields = ["name", "description"]


class DeitieDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Deitie
        fields = ["name", "description"]


class DeitieSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = DeitieListSerializer
    detail = DeitieDetailSerializer


class SpellListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Spell
        fields = ["name", "description"]


class SpellDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Spell
        fields = ["name", "description"]


class SpellSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = SpellListSerializer
    detail = SpellDetailSerializer


class RitualListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Ritual
        fields = ["name", "description"]


class RitualDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Ritual
        fields = ["name", "description"]


class RitualSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = RitualListSerializer
    detail = RitualDetailSerializer


class CreatureListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Creature
        fields = ["name", "description"]


class CreatureDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Creature
        fields = ["name", "description"]


class CreatureSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = CreatureListSerializer
    detail = CreatureDetailSerializer


class ConditionListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Condition
        fields = ["name", "description"]


class ConditionDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Condition
        fields = ["name", "description"]


class ConditionSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = ConditionListSerializer
    detail = ConditionDetailSerializer


class CurseListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Curse
        fields = ["name", "description"]


class CurseDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Curse
        fields = ["name", "description"]


class CurseSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = CurseListSerializer
    detail = CurseDetailSerializer


class DiseaseListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Disease
        fields = ["name", "description"]


class DiseaseDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Disease
        fields = ["name", "description"]


class DiseaseSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = DiseaseListSerializer
    detail = DiseaseDetailSerializer


class HazardListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Hazard
        fields = ["name", "description"]


class HazardDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Hazard
        fields = ["name", "description"]


class HazardSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = HazardListSerializer
    detail = HazardDetailSerializer


class PoisonListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Poison
        fields = ["name", "description"]


class PoisonDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Poison
        fields = ["name", "description"]


class PoisonSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = PoisonListSerializer
    detail = PoisonDetailSerializer


class ActionListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Action
        fields = ["name", "description"]


class ActionDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Action
        fields = ["name", "description"]


class ActionSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = ActionListSerializer
    detail = ActionDetailSerializer


class EquipmentListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Equipment
        fields = ["name", "description"]


class EquipmentDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Equipment
        fields = ["name", "description"]


class EquipmentSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = EquipmentListSerializer
    detail = EquipmentDetailSerializer


class AnimalCompanionListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = AnimalCompanion
        fields = ["name", "description"]


class AnimalCompanionDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = AnimalCompanion
        fields = ["name", "description"]


class AnimalCompanionSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = AnimalCompanionListSerializer
    detail = AnimalCompanionDetailSerializer


class FamiliarListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Familiar
        fields = ["name", "description"]


class FamiliarDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Familiar
        fields = ["name", "description"]


class FamiliarSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = FamiliarListSerializer
    detail = FamiliarDetailSerializer
