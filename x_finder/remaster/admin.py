from django.contrib import admin
from .models import (Source, Trait, Feature, Classe, Skill, Ancestrie, Archetype, Background, Heritage, Domain, Deitie,
                     Spell, Ritual, Creature, Condition, Curse, Disease, Hazard, Poison, Action, Equipment, Familiar,
                     AnimalCompanion)


class SourceAdmin(admin.ModelAdmin):
    list_display = ('name', 'group', 'category', 'release_date', 'nethys_url',
                    'paizo_url', 'errata_date', 'latest_errata', 'errata_url',)
    ordering = ('category', 'name',)

class TraitAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', "source", "source_page",)
    ordering = ('name',)

class FeatureAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class ClasseAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class SkillAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class AncestrieAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class ArchetypeAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class BackgroundAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class HeritageAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class DeitieAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class DomainAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class SpellAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class RitualAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class CreatureAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class ConditionAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class CurseAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class DiseaseAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class HazardAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class PoisonAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class ActionAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class EquipmentAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class FamiliarAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


class AnimalCompanionAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    ordering = ('name',)


admin.site.register(Ancestrie, AncestrieAdmin)
admin.site.register(Archetype, ArchetypeAdmin)
admin.site.register(Background, BackgroundAdmin)
admin.site.register(Classe, ClasseAdmin)
admin.site.register(Heritage, HeritageAdmin)
admin.site.register(Skill, SkillAdmin)
admin.site.register(Deitie, DeitieAdmin)
admin.site.register(Domain, DomainAdmin)
admin.site.register(Spell, SpellAdmin)
admin.site.register(Ritual, RitualAdmin)
admin.site.register(Equipment, EquipmentAdmin)
admin.site.register(Creature, CreatureAdmin)
admin.site.register(AnimalCompanion, AnimalCompanionAdmin)
admin.site.register(Familiar, FamiliarAdmin)
admin.site.register(Action, ActionAdmin)
admin.site.register(Condition, ConditionAdmin)
admin.site.register(Curse, CurseAdmin)
admin.site.register(Disease, DiseaseAdmin)
admin.site.register(Hazard, HazardAdmin)
admin.site.register(Poison, PoisonAdmin)
admin.site.register(Feature, FeatureAdmin)
admin.site.register(Source, SourceAdmin)
admin.site.register(Trait, TraitAdmin)
