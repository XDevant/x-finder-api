from django.contrib.auth import models


class UserManager(models.UserManager):
    use_in_migrations = True


class User(models.User):
    objects = UserManager()
