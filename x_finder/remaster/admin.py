from django.contrib import admin
from .models import (Source, Trait, Feature, Classe, Skill, Ancestrie, Archetype, Background, Heritage, Domain, Deitie,
                     Spell, Ritual, Creature, Condition, Curse, Disease, Hazard, Poison, Action, Equipment, Familiar,
                     AnimalCompanion)


class XfinderAdmin(admin.ModelAdmin):
    actions = ["update_selection"]

    def update_from_db(self, model_name: str, app: str):
        pass

    @admin.action(description="Update selection from db")
    def update_selection(self, request, queryset):
        pass


@admin.register(Source)
class SourceAdmin(XfinderAdmin):
    list_display = ('name', 'group', 'category', 'release_date', 'nethys_url',
                    'paizo_url', 'errata_date', 'errata_version',)
    ordering = ('group', 'name',)

@admin.register(Trait)
class TraitAdmin(XfinderAdmin):
    list_display = ('name', "subtype", "sum_description", "source", "source_page",)
    ordering = ('subtype', 'name',)


class FeatureAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class SkillAdmin(admin.ModelAdmin):
    list_display = ('name', 'attribute', "source", "source_page",)
    ordering = ('name',)


class ClasseAdmin(admin.ModelAdmin):
    list_display = ("name", "nethys_url", "key_attribute", "alt_key_attribute", "hit_points", "perception",
                    "fortitude", "reflex", "will", "free_skills", "unarmed", "simple", "martial", "advanced", "favored",
                    "unarmored", "light", "medium", "heavy", "spells", "class_dc", "sum_during_combat_encounters",
                    "sum_during_social_encounters", "sum_while_exploring", "sum_in_downtime", "sum_you_might",
                    "sum_others_probably", "sum_description", "source", "source_page",)
    ordering = ('name',)


class AncestrieAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class ArchetypeAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class BackgroundAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class HeritageAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class DeitieAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class DomainAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class SpellAdmin(admin.ModelAdmin):
    list_display = ('name', )
    ordering = ('name',)


class RitualAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class CreatureAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class ConditionAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class CurseAdmin(admin.ModelAdmin):
    list_display = ('name', )
    ordering = ('name',)


class DiseaseAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class HazardAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class PoisonAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class ActionAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class EquipmentAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class FamiliarAdmin(admin.ModelAdmin):
    list_display = ('name',)
    ordering = ('name',)


class AnimalCompanionAdmin(admin.ModelAdmin):
    list_display = ('name',)
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
