# Util script to collect input frames paths from queue

import json
import os
import pika

print('Starting frames consumer...')

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='Frames', durable=True)

base_path = '/etc/Frames'

def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    frame_metadata = json.loads(body.decode())
    path = os.path.join(base_path, frame_metadata["path"])
    print(f"Send: {path}")

channel.basic_consume(
    queue='Frames',
    on_message_callback=callback,
    auto_ack=True
)
channel.start_consuming()