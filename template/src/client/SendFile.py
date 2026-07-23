import json
import pika
import os

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='Frames', durable=True)
files = os.listdir("/home/ia/user3/IA/Frames/")
files = sorted(files, key=lambda f: int(f[5:-4])) # Sorting the frames in the video order for tracking
for file in files:
    file_msg = {"path": file}
    channel.basic_publish(exchange='', routing_key='Frames', body=json.dumps(file_msg))

