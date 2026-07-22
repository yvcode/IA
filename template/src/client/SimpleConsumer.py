import json
import os
import time
import pika
import threading

import cv2
import numpy as np
from savant_rs import telemetry
from savant_rs.telemetry import (
    ContextPropagationFormat,
    Protocol,
    TelemetryConfiguration,
    TracerConfiguration,
)
from savant_rs.primitives import Attribute, AttributeValue

from savant.api.builder import build_bbox
from savant.client import JaegerLogProvider, JpegSource, SinkBuilder, SourceBuilder

print('Starting Savant client...')


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='Frames', durable=True)




# Initialize Jaeger tracer to send metrics and logs to Jaeger.
# Note: the Jaeger tracer also should be configured in the module.
telemetry_config = TelemetryConfiguration(
    context_propagation_format=ContextPropagationFormat.W3C,
    tracer=TracerConfiguration(
        service_name='savant-client',
        protocol=Protocol.Grpc,
        endpoint='http://jaeger:4317',
        # tls=ClientTlsConfig(
        #     ca='/path/to/ca.crt',
        #     identity=Identity(
        #         certificate='/path/to/client.crt',
        #         key='/path/to/client.key',
        #     ),
        # ),
        # timeout=5000,  # milliseconds
    ),
)
telemetry.init(telemetry_config)
# or
# use x509 provider config file
# (take a look at samples/telemetry/otlp/x509_provider_config.json)
# telemetry.init_from_file('/path/to/x509_provider_config.json')


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

consumer = threading.Thread(target=channel.start_consuming, args=())
consumer.start()
while True:
    pass

consumer.join()

