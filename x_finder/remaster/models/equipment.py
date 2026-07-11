from django.db.models import (
    CharField, BooleanField, PositiveSmallIntegerField, ManyToManyField, ForeignKey, PROTECT, CASCADE
)
from remaster.models.base import Base, TraitMixin, SourceMixin, TraitThrough, SourceThrough
from remaster.models.core import Action
from remaster.models.magic import Spell



class Equipment(Base, TraitMixin, SourceMixin):
    bulk = PositiveSmallIntegerField(default=0)
    price = PositiveSmallIntegerField(default=0)
    level = PositiveSmallIntegerField(default=0)
    hardness = PositiveSmallIntegerField(default=0)
    hit_points = PositiveSmallIntegerField(default=0)
    hands = PositiveSmallIntegerField(default=0)
    usage = CharField(max_length=25, default="-")


class TraitThroughEquipment(TraitThrough):
    pass


class SourceThroughEquipment(SourceThrough):
    pass


class ArmorGroup(Base):
    pass


class WeaponGroup(Base):
    pass


class Armor(Equipment):
    group = ForeignKey(
        to=ArmorGroup,
        on_delete=PROTECT,
        related_name='armor_group',
        default=None,
        null=True
    )
    category = CharField(max_length=25, default="-")
    armor_bonus = PositiveSmallIntegerField(default=0)
    max_dex_bonus = PositiveSmallIntegerField(default=0)
    check_penalty = PositiveSmallIntegerField(default=0)
    speed_penalty = PositiveSmallIntegerField(default=0)
    minimum_strength_modifier = PositiveSmallIntegerField(default=0)


class Shield(Equipment):
    circumstance_bonus = PositiveSmallIntegerField(default=0)
    cover_bonus = PositiveSmallIntegerField(default=0)
    speed_penalty = PositiveSmallIntegerField(default=0)


class Staff(Equipment):
    spells = ManyToManyField(Spell, through='StaffSpell', related_name='staff_spells')


class Rune(Equipment):
    item_type = CharField(max_length=25, default="-")
    fundamental = BooleanField(default=False)


class Wand(Equipment):
    spell = ForeignKey(to=Spell,
                       on_delete=PROTECT,
                       related_name='wand_spell',
                       default=None,
                       null=True)
    action = ForeignKey(to=Action,
                        on_delete=PROTECT,
                        related_name='wand_action',
                        default=None,
                        null=True)


class Weapon(Equipment):
    group = ForeignKey(
        to=WeaponGroup,
        on_delete=PROTECT,
        related_name='weapon_group',
        default=None,
        null=True
    )
    type = CharField(max_length=25, default="-")
    category = CharField(max_length=25, default="-")
    damage_dice = PositiveSmallIntegerField(default=3)
    damage_type = CharField(max_length=3)
    range = PositiveSmallIntegerField(default=0)
    reload = PositiveSmallIntegerField(default=0)
