import json
import pika
import os

print('Starting Savant client...')


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='Frames', durable=True)

for file in os.listdir('/home/ia/user3/IA/Frames'):
    file_msg={"path": file}
    channel.basic_publish(exchange='', routing_key='Frames', body=json.dumps(file_msg))