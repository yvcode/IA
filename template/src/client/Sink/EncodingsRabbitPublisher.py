import json

import pika


class EncodingsRabbitPublisher:
    def __init__(self, rabbit_host, queue_name):
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))
        self.channel = connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def publish(self, file_path, frame_index, encodings):
        msg = {"file_path": file_path, "frame_index": frame_index, "encodings": encodings}
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=json.dumps(msg))
