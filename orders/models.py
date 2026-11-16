from django.db import models

class Order(models.Model):
    STATUS_CHOICES = [
        ('PENDING', 'Pending Dispatch'),
        ('DISPATCHED', 'Dispatched'),
    ]

    customer = models.CharField(max_length=100)
    product = models.CharField(max_length=100)
    quantity = models.IntegerField(default=1)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Order #{self.id} - {self.customer} [{self.status}]"
