import pika
import random

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(
    exchange='priority_exchange',
    exchange_type='direct'
)

for i in range(100):
    priority = random.choice(range(10))
    channel.basic_publish(
        properties=pika.BasicProperties(priority=priority),
        exchange='priority_exchange',
        routing_key='priority_queue',
        body=f'msg:{i}; priority:{priority}'
    )

connection.close()