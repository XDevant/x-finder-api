from rest_framework.serializers import (HyperlinkedModelSerializer,
                                        StringRelatedField,
                                        SlugRelatedField,
                                        HyperlinkedRelatedField,
                                        CharField)
from .models import (Source, Trait, Feature, Classe, Skill, Ancestrie, Background, VersatileHeritage, Archetype, Domain,
                     Deitie, Spell, Ritual, Creature, Condition, Curse, Disease, Hazard, Poison, Action, Equipment,
                     AnimalCompanion, Familiar, Language, SkillAction, ActionTrait, ClassSkill, ClassFeature, SubClass,
                     AncestryFeat, AncestryLanguage, Heritage, VersatileHeritageFeat, )



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
        fields = ["pk", "name", "category", "group", "errata_version", "release_date", "nethys_url", "paizo_url"]


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
        fields = ["pk", "name", "subtype", "nethys_url", "description", "source", "source_page"]


class TraitDetailSerializer(HyperlinkedModelSerializer):
    source = SourceDetailSerializer()

    class Meta:
        model = Trait
        fields = ["pk", "name", "subtype", "nethys_url", "description", "source", "source_page"]


class TraitSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = TraitListSerializer
    detail = TraitDetailSerializer


class FeatureListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Feature
        fields = ["pk", "name", "nethys_url", "description"]


class FeatureDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Feature
        fields = ["pk", "name", "nethys_url", "description"]


class FeatureSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = FeatureListSerializer
    detail = FeatureDetailSerializer


class LanguageListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Language
        fields = ["pk", "name", "subtype", "nethys_url", "description"]


class LanguageDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Language
        fields = ["pk", "name", "subtype", "nethys_url", "description"]


class LanguageSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = LanguageListSerializer
    detail = LanguageDetailSerializer


class ClasseListSerializer(HyperlinkedModelSerializer):
    source = StringRelatedField()
    skill_choices = StringRelatedField(many=True)

    class Meta:
        model = Classe
        fields = ["pk", "name", "nethys_url", "key_attribute", "alt_key_attribute", "hit_points", "perception",
                  "fortitude", "reflex", "will", "skill_choices", "free_skills", "unarmed", "simple", "martial", "advanced",
                  "favored", "unarmored", "light", "medium", "heavy", "spells", "class_dc", "source", "source_page"]


class ClasseDetailSerializer(HyperlinkedModelSerializer):
    source = SourceDetailSerializer()
    skill_choices = StringRelatedField(many=True)
    features = StringRelatedField(many=True)

    class Meta:
        model = Classe
        fields = ["pk", "name", "nethys_url", "key_attribute", "alt_key_attribute", "hit_points", "perception",
                  "fortitude", "reflex", "will", "skill_choices", "free_skills", "unarmed", "simple", "martial", "advanced",
                  "favored", "unarmored", "light", "medium", "heavy", "spells", "class_dc", "features", "during_combat_encounters",
                  "during_social_encounters", "while_exploring", "in_downtime", "you_might", "others_probably",
                  "description", "source", "source_page"]


class ClasseSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = ClasseListSerializer
    detail = ClasseDetailSerializer


class SubClassListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = SubClass
        fields = ["pk", "name", "nethys_url", "description"]


class SubClassDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = SubClass
        fields = ["pk", "name", "nethys_url", "description"]


class SubclassSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = SubClassListSerializer
    detail = SubClassDetailSerializer


class ClassFeatureListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = ClassFeature
        fields = ["pk", "name", "nethys_url", "description"]


class ClassFeatureDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = ClassFeature
        fields = ["pk", "name", "nethys_url", "description"]


class ClassFeatureSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = ClassFeatureListSerializer
    detail = ClassFeatureDetailSerializer


class AncestrieListSerializer(HyperlinkedModelSerializer):
    source = StringRelatedField()

    class Meta:
        model = Ancestrie
        fields = ["pk", "name", "nethys_url", "hit_points", "size", "speed", "attribute_flaw",
                  "attribute_bonus_1", "attribute_bonus_2", "free_bonus", "free_languages", "augmented_sense",
                  "special", "description", "source", "source_page"]


class AncestrieDetailSerializer(HyperlinkedModelSerializer):
    source = StringRelatedField()
    spoken_languages = SlugRelatedField(many=True,
                                        slug_field='name',
                                        read_only=True)
    starting_languages = SlugRelatedField(many=True,
                                        slug_field='name',
                                        read_only=True)
    traits = SlugRelatedField(many=True,
                             slug_field='name',
                             read_only=True)
    heritages = SlugRelatedField(many=True,
                                 slug_field='name',
                                 read_only=True)
    ancestry_feats = SlugRelatedField(many=True,
                            slug_field='name',
                            read_only=True)

    class Meta:
        model = Ancestrie
        fields = ["pk", "name", "nethys_url", "traits", "hit_points", "size", "speed", "attribute_flaw", "attribute_bonus_1",
                  "attribute_bonus_2", "free_bonus", "free_languages", "augmented_sense", "special", "heritages", "ancestry_feats",
                  "spoken_languages", "starting_languages", "you_might", "others_probably", "physical_description", "common_names",
                  "society", "beliefs", "description", "source", "source_page"]
        depth = 1


class AncestrieSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = AncestrieListSerializer
    detail = AncestrieDetailSerializer


class HeritageListSerializer(HyperlinkedModelSerializer):
    ancestrie = SlugRelatedField(slug_field="name", read_only=True)

    class Meta:
        model = Heritage
        fields = ["pk", "name", "nethys_url", "ancestrie", "description"]


class HeritageDetailSerializer(HyperlinkedModelSerializer):
    source = StringRelatedField()
    traits = StringRelatedField()
    ancestrie = SlugRelatedField(slug_field="name", read_only=True)

    class Meta:
        model = Heritage
        fields = ["pk", "name","nethys_url", "traits", "ancestrie", "description", "source", "source_page"]


class HeritageSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = HeritageListSerializer
    detail = HeritageDetailSerializer


class AncestryFeatListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = AncestryFeat
        fields = ["pk", "name", "nethys_url", "description"]


class AncestryFeatDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = AncestryFeat
        fields = ["pk", "name", "nethys_url", "description"]


class AncestryFeatSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = AncestryFeatListSerializer
    detail = AncestryFeatDetailSerializer


class BackgroundListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Background
        fields = ["pk", "name", "nethys_url", "description"]


class BackgroundDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Background
        fields = ["pk", "name", "nethys_url", "description"]


class BackgroundSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = BackgroundListSerializer
    detail = BackgroundDetailSerializer


class VersatileHeritageListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = VersatileHeritage
        fields = ["pk", "name", "nethys_url", "description"]


class VersatileHeritageDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = VersatileHeritage
        fields = ["pk", "name", "nethys_url", "description"]


class VersatileHeritageSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = VersatileHeritageListSerializer
    detail = VersatileHeritageDetailSerializer


class VersatileHeritageFeatListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = VersatileHeritageFeat
        fields = ["pk", "name", "nethys_url", "description"]


class VersatileHeritageFeatDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = VersatileHeritageFeat
        fields = ["pk", "name", "nethys_url", "description"]


class VersatileHeritageFeatSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = VersatileHeritageFeatListSerializer
    detail = VersatileHeritageFeatDetailSerializer


class SkillListSerializer(HyperlinkedModelSerializer):
    source = StringRelatedField()

    class Meta:
        model = Skill
        fields = ["pk", "name", "nethys_url", "attribute", "description", "nethys_url", "source"]


class SkillDetailSerializer(HyperlinkedModelSerializer):
    source = SourceDetailSerializer()

    class Meta:
        model = Skill
        fields = ["pk", "name", "nethys_url", "attribute", "description", "nethys_url", "source"]


class SkillSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = SkillListSerializer
    detail = SkillDetailSerializer


class SkillActionListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = SkillAction
        fields = ["pk", "name", "nethys_url", "description"]


class SkillActionDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = SkillAction
        fields = ["pk", "name", "nethys_url", "description"]


class SkillActionSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = SkillActionListSerializer
    detail = SkillActionDetailSerializer


class ArchetypeListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Archetype
        fields = ["pk", "name", "nethys_url", "description"]


class ArchetypeDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Archetype
        fields = ["pk", "name", "nethys_url", "description"]


class ArchetypeSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = ArchetypeListSerializer
    detail = ArchetypeDetailSerializer


class DomainListSerializer(HyperlinkedModelSerializer):
    domain_spell = HyperlinkedRelatedField(view_name="spell-detail", lookup_field="name", read_only=True)
    advanced_domain_spell = HyperlinkedRelatedField(view_name="spell-detail", lookup_field="name", read_only=True)

    class Meta:
        model = Domain
        fields = ["pk", "name", "nethys_url", "domain_spell", "advanced_domain_spell", "description"]


class DomainDetailSerializer(HyperlinkedModelSerializer):
    domain_spell = StringRelatedField()
    advanced_domain_spell = StringRelatedField()

    class Meta:
        model = Domain
        fields = ["pk", "name", "nethys_url", "domain_spell", "advanced_domain_spell", "description"]


class DomainSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = DomainListSerializer
    detail = DomainDetailSerializer


class DeitieListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Deitie
        fields = ["pk", "name", "nethys_url", "description"]


class DeitieDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Deitie
        fields = ["pk", "name", "nethys_url", "description"]


class DeitieSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = DeitieListSerializer
    detail = DeitieDetailSerializer


class SpellListSerializer(HyperlinkedModelSerializer):
    traits = StringRelatedField(many=True)
    source = StringRelatedField()
    traditions = StringRelatedField(many=True)

    class Meta:
        model = Spell
        fields = ["pk", "name", "rank", "nethys_url", "traits", "traditions", "spell_type", "cast", "range", "area", "defense", "targets", "description", "source"]


class SpellDetailSerializer(HyperlinkedModelSerializer):
    traits = StringRelatedField(many=True)
    source = StringRelatedField()
    traditions = StringRelatedField(many=True)

    class Meta:
        model = Spell
        fields = ["pk", "name", "rank", "traits", "traditions", "spell_type", "cast", "trigger", "range", "area", "defense", "targets", "duration", "requirements", "critical_success", "success", "failure", "critical_failure", "heightened", "description", "nethys_url", "source", "source_page"]


class SpellSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = SpellListSerializer
    detail = SpellDetailSerializer


class RitualListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Ritual
        fields = ["pk", "name", "nethys_url", "description"]


class RitualDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Ritual
        fields = ["pk", "name", "nethys_url", "description"]


class RitualSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = RitualListSerializer
    detail = RitualDetailSerializer


class CreatureListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Creature
        fields = ["pk", "name", "nethys_url", "description"]


class CreatureDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Creature
        fields = ["pk", "name", "nethys_url", "description"]


class CreatureSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = CreatureListSerializer
    detail = CreatureDetailSerializer


class ConditionListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Condition
        fields = ["pk", "name", "nethys_url", "description"]


class ConditionDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Condition
        fields = ["pk", "name", "nethys_url", "description"]


class ConditionSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = ConditionListSerializer
    detail = ConditionDetailSerializer


class CurseListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Curse
        fields = ["pk", "name", "nethys_url", "description"]


class CurseDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Curse
        fields = ["pk", "name", "nethys_url", "description"]


class CurseSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = CurseListSerializer
    detail = CurseDetailSerializer


class DiseaseListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Disease
        fields = ["pk", "name", "nethys_url", "description"]


class DiseaseDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Disease
        fields = ["pk", "name", "nethys_url", "description"]


class DiseaseSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = DiseaseListSerializer
    detail = DiseaseDetailSerializer


class HazardListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Hazard
        fields = ["pk", "name", "nethys_url", "description"]


class HazardDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Hazard
        fields = ["pk", "name", "nethys_url", "description"]


class HazardSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = HazardListSerializer
    detail = HazardDetailSerializer


class PoisonListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Poison
        fields = ["pk", "name", "nethys_url", "description"]


class PoisonDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Poison
        fields = ["pk", "name", "nethys_url", "description"]


class PoisonSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = PoisonListSerializer
    detail = PoisonDetailSerializer


class ActionListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Action
        fields = ["pk", "name", "nethys_url", "description"]


class ActionDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Action
        fields = ["pk", "name", "nethys_url", "description"]


class ActionSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = ActionListSerializer
    detail = ActionDetailSerializer


class EquipmentListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Equipment
        fields = ["pk", "name", "nethys_url", "description"]


class EquipmentDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Equipment
        fields = ["pk", "name", "nethys_url", "description"]


class EquipmentSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = EquipmentListSerializer
    detail = EquipmentDetailSerializer


class AnimalCompanionListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = AnimalCompanion
        fields = ["pk", "name", "nethys_url", "description"]


class AnimalCompanionDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = AnimalCompanion
        fields = ["pk", "name", "nethys_url", "description"]


class AnimalCompanionSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = AnimalCompanionListSerializer
    detail = AnimalCompanionDetailSerializer


class FamiliarListSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Familiar
        fields = ["pk", "name", "nethys_url", "description"]


class FamiliarDetailSerializer(HyperlinkedModelSerializer):
    class Meta:
        model = Familiar
        fields = ["pk", "name", "nethys_url", "description"]


class FamiliarSerializerSelector:
    """Import container for the view, and it's get_serializer method"""
    list = FamiliarListSerializer
    detail = FamiliarDetailSerializer
