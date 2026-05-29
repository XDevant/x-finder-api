from .models import (Source, Trait, Classe, Feature, Skill, Ancestrie, Background, Archetype, Heritage, Domain, Deitie,
                     Spell, Ritual, Creature, Condition, Curse, Disease, Hazard, Poison, Action, Equipment, 
                     AnimalCompanion, Familiar)
from rest_framework.viewsets import ModelViewSet, ReadOnlyModelViewSet
from .serializer import (SourceSerializerSelector, TraitSerializerSelector, FeatureSerializerSelector,
                         ClasseSerializerSelector, SkillSerializerSelector, DomainSerializerSelector,
                         AncestrieSerializerSelector, BackgroundSerializerSelector, HeritageSerializerSelector,
                         ArchetypeSerializerSelector, DeitieSerializerSelector, SpellSerializerSelector, 
                         RitualSerializerSelector, CreatureSerializerSelector, ConditionSerializerSelector, 
                         CurseSerializerSelector, DiseaseSerializerSelector, HazardSerializerSelector, 
                         PoisonSerializerSelector, ActionSerializerSelector, EquipmentSerializerSelector, 
                         AnimalCompanionSerializerSelector, FamiliarSerializerSelector)
from .mixins import MultipleSerializerMixin



class ActionViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Action.objects.all()
    serializer_class = ActionSerializerSelector


class AncestrieViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Ancestrie.objects.all()
    serializer_class = AncestrieSerializerSelector


class AnimalCompanionViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = AnimalCompanion.objects.all()
    serializer_class = AnimalCompanionSerializerSelector


class ArchetypeViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Archetype.objects.all()
    serializer_class = ArchetypeSerializerSelector


class BackgroundViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Background.objects.all()
    serializer_class = BackgroundSerializerSelector


class ClasseViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Classe.objects.all()
    serializer_class = ClasseSerializerSelector


class ConditionViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Condition.objects.all()
    serializer_class = ConditionSerializerSelector


class CreatureViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Creature.objects.all()
    serializer_class = CreatureSerializerSelector


class CurseViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Curse.objects.all()
    serializer_class = CurseSerializerSelector


class DeitieViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Deitie.objects.all()
    serializer_class = DeitieSerializerSelector


class DiseaseViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Disease.objects.all()
    serializer_class = DiseaseSerializerSelector


class DomainViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Domain.objects.all()
    serializer_class = DomainSerializerSelector


class EquipmentViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Equipment.objects.all()
    serializer_class = EquipmentSerializerSelector


class FamiliarViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Familiar.objects.all()
    serializer_class = FamiliarSerializerSelector


class FeatureViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Feature.objects.all()
    serializer_class = FeatureSerializerSelector


class HazardViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Hazard.objects.all()
    serializer_class = HazardSerializerSelector


class HeritageViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Heritage.objects.all()
    serializer_class = HeritageSerializerSelector


class PoisonViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Poison.objects.all()
    serializer_class = PoisonSerializerSelector


class SkillViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Skill.objects.all()
    serializer_class = SkillSerializerSelector


class SourceViewSet(MultipleSerializerMixin, ReadOnlyModelViewSet):
    queryset = Source.objects.all()
    serializer_class = SourceSerializerSelector.list
    multi_serializer_class = SourceSerializerSelector
    filterset_fields = {'name': ['exact', 'icontains'],
                        'category': ['exact', 'icontains']}


class SpellViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Spell.objects.all()
    serializer_class = SpellSerializerSelector


class RitualViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Ritual.objects.all()
    serializer_class = RitualSerializerSelector


class TraitViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Trait.objects.all()
    serializer_class = TraitSerializerSelector
