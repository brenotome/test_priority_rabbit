import pika
import random

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(
    exchange='stream_exchange',
    exchange_type='direct'
)

for i in range(100):
    priority = random.choice(range(10))
    channel.basic_publish(
        properties=pika.BasicProperties(priority=priority),
        exchange='stream_exchange',
        routing_key='stream_queue',
        body=f'msg:{i}; priority:{priority}'
    )

connection.close()