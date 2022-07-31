from django.db import models
from django.conf import settings

from enum import Enum

# Create your models here.


class BillStatus(Enum):
    OPN = 'open'
    STL = 'settled'
    DIS = 'dispute'


class Bill(models.Model):
    name = models.CharField(max_length=255)
    desc = models.TextField(default='No description.')
    date = models.DateTimeField()
    edited = models.DateTimeField()
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name='created',
        on_delete=models.CASCADE,
    )
    people = models.ManyToManyField(
        settings.AUTH_USER_MODEL,
        related_name='involved',
        through='Involvement',
        through_fields=('bill', 'user'),
    )
    status = models.CharField(
        max_length=32,
        choices=[(tag.name, tag.value) for tag in BillStatus],
    )

    def __str__(self):
        return self.name


class BillItem(models.Model):
    name = models.CharField(max_length=255)
    desc = models.TextField()
    date = models.DateTimeField()
    edited = models.DateTimeField()
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name='created_items',
        on_delete=models.CASCADE,
    )
    paid_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name='paid_items',
        on_delete=models.CASCADE,
    )
    bill = models.ForeignKey(
        Bill,
        related_name='items',
        on_delete=models.CASCADE
    )
    total = models.FloatField()

    def __str__(self):
        return self.name


class Involvement(models.Model):
    bill = models.ForeignKey(Bill, on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)


class ItemWeightAssignment(models.Model):
    item = models.ForeignKey(BillItem, related_name='assignments', on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    amount = models.FloatField()
