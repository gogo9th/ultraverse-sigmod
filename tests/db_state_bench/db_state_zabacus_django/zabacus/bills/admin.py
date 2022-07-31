from django.contrib import admin
from zabacus.bills.models import Bill, BillItem, Involvement, ItemWeightAssignment

# Register your models here.

admin.site.register(Bill)
admin.site.register(BillItem)
admin.site.register(Involvement)
admin.site.register(ItemWeightAssignment)
