from django.shortcuts import render
from authentication.models import User
from rest_framework import viewsets
from authentication.serializer import UserSerializer


class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer
