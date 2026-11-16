import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import Order
from .producer import publish_order_created

@csrf_exempt
def order_list_create(request):
    if request.method == 'POST':
        try:
            body = json.loads(request.body)
            order = Order.objects.create(
                customer=body['customer'],
                product=body['product'],
                quantity=body.get('quantity', 1),
            )

            # Despacha o evento para a fila do Kafka sem travar o fluxo
            publish_order_created(order)

            return JsonResponse({
                'message': 'Order received and queued for dispatch.',
                'order': {
                    'id': order.id,
                    'customer': order.customer,
                    'product': order.product,
                    'quantity': order.quantity,
                    'status': order.status,
                }
            }, status=201)

        except (KeyError, json.JSONDecodeError):
            return JsonResponse({'error': 'Invalid payload. Send customer and product.'}, status=400)

    orders = list(Order.objects.values('id', 'customer', 'product', 'quantity', 'status', 'created_at'))
    return JsonResponse({'orders': orders})
