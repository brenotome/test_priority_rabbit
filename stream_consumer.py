import pika
from time import sleep


def slow_print(body,ch,m):
    sleep(1)
    print(body)
    ch.basic_ack(delivery_tag=m.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(
    exchange='stream_exchange',
    exchange_type='direct'
)

queue = channel.queue_declare(queue='stream_queue',durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"})
channel.queue_bind(
        exchange='stream_exchange',
        queue=queue.method.queue,
        routing_key='stream_queue',
    )

channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    arguments={'x-stream-offset':'next'},
    # arguments={'x-stream-offset':'first'},#toggle to consume from beggining
    queue='stream_queue',
    auto_ack=False,
    on_message_callback= lambda ch,m,p,body:slow_print(body,ch,m)
)
print('waiting')
channel.start_consuming()