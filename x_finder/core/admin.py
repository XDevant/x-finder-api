from django.contrib import admin
from .models import Source


class SourceAdmin(admin.ModelAdmin):
    list_display = ('name', 'group', 'category', 'release_date', 'nethys_url',
                    'paizo_url', 'errata_date', 'errata_version', 'errata_url')
    ordering = ('category', 'name')


admin.site.register(Source, SourceAdmin)
