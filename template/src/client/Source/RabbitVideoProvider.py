import json

import pika


class RabbitVideoProvider:
    def __init__(self, rabbit_host, queue_name, video_callback):
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.frame_counter = 0
        self.video_callback = video_callback
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=self._rabbit_callback,
            auto_ack=True
        )

    def provide_loop(self):
        self.channel.start_consuming()

    def _rabbit_callback(self, ch, method, properties, body):
        print(f" [x] Received {body.decode()}")
        msg = json.loads(body.decode())
        path = msg["path"]

        self.video_callback(path)
