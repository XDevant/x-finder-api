from django.urls import path, include
from rest_framework_nested.routers import DefaultRouter

from .views import (SourceViewSet, TraitViewSet, FeatureViewSet, ClasseViewSet, AncestrieViewSet, ArchetypeViewSet,
                    BackgroundViewSet, HeritageViewSet, SkillViewSet, DomainViewSet, DeitieViewSet, CurseViewSet, 
                    AnimalCompanionViewSet, DiseaseViewSet, ActionViewSet, FamiliarViewSet, HazardViewSet, SpellViewSet,
                    RitualViewSet, EquipmentViewSet, PoisonViewSet, ConditionViewSet, CreatureViewSet)



router = DefaultRouter()
router.register(r"Ancestries", AncestrieViewSet, basename="Ancestries")
router.register(r"Backgrounds", BackgroundViewSet, basename="Backgrounds")
router.register(r"Heritages", HeritageViewSet, basename="Heritages")
router.register(r"Classes", ClasseViewSet, basename="Classes")
router.register(r"Equipments", EquipmentViewSet, basename="Equipments")
router.register(r"Skills", SkillViewSet, basename="Skills")
router.register(r"Creatures", CreatureViewSet, basename="Creatures")
router.register(r"Deities", DeitieViewSet, basename="Deities")
router.register(r"Domains", DomainViewSet, basename="Domains")
router.register(r"Spells", SpellViewSet, basename="Spells")
router.register(r"Rituals", RitualViewSet, basename="Rituals")
router.register(r"AnimalCompanions", AnimalCompanionViewSet, basename="AnimalCompanions")
router.register(r"Familiars", FamiliarViewSet, basename="Familiars")
router.register(r"Archetypes", ArchetypeViewSet, basename="Archetypes")
router.register(r"Actions", ActionViewSet, basename="Actions")
router.register(r"Conditions", ConditionViewSet, basename="Conditions")
router.register(r"Features", FeatureViewSet, basename="Features")
router.register(r"Curses", CurseViewSet, basename="Curses")
router.register(r"Diseases", DiseaseViewSet, basename="Diseases")
router.register(r"Hazards", HazardViewSet, basename="Hazards")
router.register(r"Poisons", PoisonViewSet, basename="Poisons")
router.register(r"Sources", SourceViewSet, basename="Sources")
router.register(r"Traits", TraitViewSet, basename="Traits")


urlpatterns = [
    path(r"", include(router.urls), name="Remaster"),
]


