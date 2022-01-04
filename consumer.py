import pika
from time import sleep


def slow_print(body,ch,m):
    sleep(1)
    print(body)
    ch.basic_ack(delivery_tag=m.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(
    exchange='priority_exchange',
    exchange_type='direct'
)

queue = channel.queue_declare(queue='priority_queue', arguments={"x-max-priority": 10})
channel.queue_bind(
        exchange='priority_exchange',
        queue=queue.method.queue,
        routing_key='priority_queue',
    )

channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue='priority_queue',
    auto_ack=False,
    on_message_callback= lambda ch,m,p,body:slow_print(body,ch,m)
)
print('waiting')
channel.start_consuming()