from .models import (Source, Trait, Classe, Feature, Skill, Ancestrie, Background, Archetype, VersatileHeritage, Domain, Deitie,
                     Spell, Ritual, Creature, Condition, Curse, Disease, Hazard, Poison, Action, Equipment, Heritage,
                     AnimalCompanion, Familiar, Language, AncestryFeat)
from rest_framework.viewsets import ModelViewSet, ReadOnlyModelViewSet
from .serializer import (SourceSerializerSelector, TraitSerializerSelector, FeatureSerializerSelector,
                         ClasseSerializerSelector, SkillSerializerSelector, DomainSerializerSelector,
                         AncestrieSerializerSelector, BackgroundSerializerSelector, VersatileHeritageSerializerSelector,
                         ArchetypeSerializerSelector, DeitieSerializerSelector, SpellSerializerSelector, 
                         RitualSerializerSelector, CreatureSerializerSelector, ConditionSerializerSelector, 
                         CurseSerializerSelector, DiseaseSerializerSelector, HazardSerializerSelector, 
                         PoisonSerializerSelector, ActionSerializerSelector, EquipmentSerializerSelector, 
                         AnimalCompanionSerializerSelector, FamiliarSerializerSelector, HeritageSerializerSelector,
                         AncestryFeatSerializerSelector, SubclassSerializerSelector, ClassFeatureSerializerSelector,
                         SkillActionSerializerSelector, LanguageSerializerSelector, )
from .mixins import MultipleSerializerMixin



class ActionViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Action.objects.all()
    serializer_class = ActionSerializerSelector.list
    multi_serializer_class = ActionSerializerSelector


class AncestrieViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Ancestrie.objects.all()
    serializer_class = AncestrieSerializerSelector.list
    multi_serializer_class = AncestrieSerializerSelector
    lookup_field = 'name'


class AncestryFeatViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = AncestryFeat.objects.prefetch_related('ancestry')
    serializer_class = AncestryFeatSerializerSelector.list
    multi_serializer_class = AncestryFeatSerializerSelector
    lookup_field = 'name'

    def get_queryset(self):
        ancestry_pk = self.kwargs.get('ancestrie_name')  # Extract author ID from URL
        if ancestry_pk:
            return AncestryFeat.objects.filter(ancestry__name=ancestry_pk)
        return super().get_queryset()


class AnimalCompanionViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = AnimalCompanion.objects.all()
    serializer_class = AnimalCompanionSerializerSelector.list
    multi_serializer_class = AnimalCompanionSerializerSelector


class ArchetypeViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Archetype.objects.all()
    serializer_class = ArchetypeSerializerSelector.list
    multi_serializer_class = ArchetypeSerializerSelector


class BackgroundViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Background.objects.all()
    serializer_class = BackgroundSerializerSelector.list
    multi_serializer_class = BackgroundSerializerSelector


class ClasseViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Classe.objects.all()
    serializer_class = ClasseSerializerSelector.list
    multi_serializer_class = ClasseSerializerSelector


class ConditionViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Condition.objects.all()
    serializer_class = ConditionSerializerSelector.list
    multi_serializer_class = ConditionSerializerSelector


class CreatureViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Creature.objects.all()
    serializer_class = CreatureSerializerSelector.list
    multi_serializer_class = CreatureSerializerSelector


class CurseViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Curse.objects.all()
    serializer_class = CurseSerializerSelector.list
    multi_serializer_class = CurseSerializerSelector


class DeitieViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Deitie.objects.all()
    serializer_class = DeitieSerializerSelector.list
    multi_serializer_class = DeitieSerializerSelector


class DiseaseViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Disease.objects.all()
    serializer_class = DiseaseSerializerSelector.list
    multi_serializer_class = DiseaseSerializerSelector


class DomainViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Domain.objects.all()
    serializer_class = DomainSerializerSelector.list
    multi_serializer_class = DomainSerializerSelector


class EquipmentViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Equipment.objects.all()
    serializer_class = EquipmentSerializerSelector.list
    multi_serializer_class = EquipmentSerializerSelector


class FamiliarViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Familiar.objects.all()
    serializer_class = FamiliarSerializerSelector.list
    multi_serializer_class = FamiliarSerializerSelector


class FeatureViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Feature.objects.all()
    serializer_class = FeatureSerializerSelector.list
    multi_serializer_class = FeatureSerializerSelector


class HazardViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Hazard.objects.all()
    serializer_class = HazardSerializerSelector.list
    multi_serializer_class = HazardSerializerSelector


class HeritageViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Heritage.objects.prefetch_related("ancestrie")
    serializer_class = HeritageSerializerSelector.list
    multi_serializer_class = HeritageSerializerSelector
    lookup_field = 'name'

    def get_queryset(self):
        ancestry_pk = self.kwargs.get('ancestrie_name')  # Extract ancestry ID from URL
        qs = super().get_queryset()
        if ancestry_pk:
            return qs.filter(ancestrie__name=ancestry_pk.title())
        return qs


class LanguageViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Language.objects.all()
    serializer_class = LanguageSerializerSelector.list
    multi_serializer_class = LanguageSerializerSelector


class PoisonViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Poison.objects.all()
    serializer_class = PoisonSerializerSelector.list
    multi_serializer_class = PoisonSerializerSelector


class SkillViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Skill.objects.all()
    serializer_class = SkillSerializerSelector.list
    multi_serializer_class = SkillSerializerSelector


class SourceViewSet(MultipleSerializerMixin, ReadOnlyModelViewSet):
    queryset = Source.objects.all()
    serializer_class = SourceSerializerSelector.list
    multi_serializer_class = SourceSerializerSelector
    filterset_fields = {'name': ['exact', 'icontains'],
                        'category': ['exact', 'icontains']}


class SpellViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Spell.objects.all()
    serializer_class = SpellSerializerSelector.list
    multi_serializer_class = SpellSerializerSelector


class RitualViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Ritual.objects.all()
    serializer_class = RitualSerializerSelector.list
    multi_serializer_class = RitualSerializerSelector


class TraitViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = Trait.objects.all()
    serializer_class = TraitSerializerSelector.list
    multi_serializer_class = TraitSerializerSelector


class VersatileHeritageViewSet(MultipleSerializerMixin, ModelViewSet):
    queryset = VersatileHeritage.objects.all()
    serializer_class = VersatileHeritageSerializerSelector.list
    multi_serializer_class = VersatileHeritageSerializerSelector
