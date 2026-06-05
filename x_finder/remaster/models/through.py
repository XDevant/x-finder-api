from django.db.models import Model, ForeignKey, BooleanField, PositiveSmallIntegerField, CASCADE, UniqueConstraint

from remaster.models.base import Base
from remaster.models.core import Classe, Skill, Feature


class ClassSkill(Model):
    choice = BooleanField(default=False)
    char_class = ForeignKey(
        to=Classe,
        on_delete=CASCADE,
        related_name='skill_classes'
    )
    skill = ForeignKey(
        to=Skill,
        on_delete=CASCADE,
        related_name='class_skills')

    def __str__(self):
        add = ""
        if self.choice:
            add = "choice"
        return f"{self.char_class.name} skill {add}: {self.skill.name}"


class ClassFeature(Model):
    level = PositiveSmallIntegerField(default=0)
    char_class = ForeignKey(
        to=Classe,
        on_delete=CASCADE,
        related_name='feature_classes'
    )
    feature = ForeignKey(
        to=Feature,
        on_delete=CASCADE,
        related_name='class_features'
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["char_class", "feature"], name="unique_class_feature"
            )
        ]

    def __str__(self):
        return f"{self.char_class.name} - {self.feature.name} ({self.level})"
