import json
import pika


print('Starting Savant client...')


connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='Frames', durable=True)
file_msg={"path": "ex1.jpeg"}
channel.basic_publish(exchange='', routing_key='Frames', body=json.dumps(file_msg))