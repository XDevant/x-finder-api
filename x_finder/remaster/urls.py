from django.urls import path, include
from rest_framework_nested.routers import DefaultRouter, NestedSimpleRouter

from .views import (SourceViewSet, TraitViewSet, FeatureViewSet, ClasseViewSet, AncestrieViewSet, ArchetypeViewSet,
                    BackgroundViewSet, VersatileHeritageViewSet, SkillViewSet, DomainViewSet, DeitieViewSet, CurseViewSet,
                    AnimalCompanionViewSet, DiseaseViewSet, ActionViewSet, FamiliarViewSet, HazardViewSet, SpellViewSet,
                    RitualViewSet, EquipmentViewSet, PoisonViewSet, ConditionViewSet, CreatureViewSet, HeritageViewSet,
                    LanguageViewSet)



router = DefaultRouter()
router.register(r"ancestry", AncestrieViewSet, basename="ancestry")
router.register(r"background", BackgroundViewSet, basename="background")
router.register(r"versatile-heritage", VersatileHeritageViewSet, basename="versatile-heritage")
router.register(r"class", ClasseViewSet, basename="classe")
router.register(r"equipment", EquipmentViewSet, basename="equipment")
router.register(r"skill", SkillViewSet, basename="skill")
router.register(r"creature", CreatureViewSet, basename="creature")
router.register(r"deity", DeitieViewSet, basename="deity")
router.register(r"domain", DomainViewSet, basename="domain")
router.register(r"spell", SpellViewSet, basename="spell")
router.register(r"ritual", RitualViewSet, basename="rituals")
router.register(r"animal-companions", AnimalCompanionViewSet, basename="animal_companions")
router.register(r"familiar", FamiliarViewSet, basename="familiar")
router.register(r"archetype", ArchetypeViewSet, basename="archetype")
router.register(r"action", ActionViewSet, basename="action")
router.register(r"condition", ConditionViewSet, basename="condition")
router.register(r"feature", FeatureViewSet, basename="feature")
router.register(r"curse", CurseViewSet, basename="curse")
router.register(r"disease", DiseaseViewSet, basename="disease")
router.register(r"hazard", HazardViewSet, basename="hazard")
router.register(r"language", LanguageViewSet, basename="language")
router.register(r"poison", PoisonViewSet, basename="poison")
router.register(r"source", SourceViewSet, basename="source")
router.register(r"trait", TraitViewSet, basename="trait")

heritage_router = NestedSimpleRouter(router, r'ancestry', lookup='ancestrie')
heritage_router.register(r'heritage', HeritageViewSet, basename='ancestry-heritage')


urlpatterns = [
    path("", include(router.urls), name="Remaster"),
    path('', include(heritage_router.urls)),
]


