# Util script to collect face vector results from queue

import json
import os
import pika

print('Starting face vector consumer...')

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='Results', durable=True)

base_path = '/etc/Frames'

def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    frame_metadata = json.loads(body.decode())
    path = os.path.join(base_path, frame_metadata["path"])
    faces = frame_metadata["faces"]
    print(f"Send: {path}\nFaces: {faces}")

channel.basic_consume(
    queue='Results',
    on_message_callback=callback,
    auto_ack=True
)

channel.start_consuming()


