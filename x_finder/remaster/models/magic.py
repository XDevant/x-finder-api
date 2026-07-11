from django.db.models import (CharField, BooleanField, PositiveSmallIntegerField, ForeignKey, PROTECT)
from remaster.models.base import Base, TraitMixin, TraitThrough
from remaster.models.core import Source



class Ritual(Base, TraitMixin):
    rank = PositiveSmallIntegerField(default=1)
    source = ForeignKey(
        to=Source,
        on_delete=PROTECT,
        related_name='ritual_source',
        default=None,
        null=True
    )


class TraitThroughRitual(TraitThrough):
    pass


class Spell(Base, TraitMixin):
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


class TraitThroughSpell(TraitThrough):
    pass


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
