import json
import uuid
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import Order
from .producer import publish_order_event

@csrf_exempt
def order_list_create(request):
    if request.method == 'POST':  
        try:
            body = json.loads(request.body)
            payload = {
                'uuid': str(uuid.uuid4()),
                'customer': body['customer'],
                'product': body['product'],
                'quantity': body.get('quantity', 1),
            }
            
            # Despacha o evento para a fila do Kafka sem travar o fluxo
            publish_order_event(payload)

            return JsonResponse({
                'message': 'Order accepted for async processing.',
                'tracking_uuid': payload['uuid'],
            }, status=202)

        except (KeyError, json.JSONDecodeError):
            return JsonResponse({'error': 'Invalid payload. Send customer and product.'}, status=400)

    orders = list(Order.objects.values('id', 'customer', 'product', 'quantity', 'status', 'created_at'))
    return JsonResponse({'orders': orders})
