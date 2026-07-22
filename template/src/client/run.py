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


connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
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

module_hostname = os.environ.get('MODULE_HOSTNAME', 'localhost')
jaeger_endpoint = 'http://jaeger:16686'
healthcheck_url = f'http://{module_hostname}:8888/status'
source_id = 'test-source'
shutdown_auth = 'shutdown'
parent_dir = os.path.dirname(os.path.dirname(__file__))
result_img_path = os.path.join('/etc/Frames', 'result_img.jpeg')
base_path = '/etc/Frames'
frame_counter = 0
frame_metadata_cache = {}

# Build the source
source = (
    SourceBuilder()
    .with_log_provider(JaegerLogProvider(jaeger_endpoint))
    .with_socket('pub+connect:ipc:///tmp/zmq-sockets/input-video.ipc')
    # Note: healthcheck port should be configured in the module.
    .with_module_health_check_url(healthcheck_url)
    .build()
)

sink = (
    SinkBuilder()
    .with_socket('sub+connect:ipc:///tmp/zmq-sockets/output-video.ipc')
    .with_source_id(source_id)
    .with_idle_timeout(60)
    .with_log_provider(JaegerLogProvider(jaeger_endpoint))
    # Note: healthcheck port should be configured in the module.
    .with_module_health_check_url(healthcheck_url)
    .build()
)


def callback(ch, method, properties, body):
    global frame_counter
    print(f" [x] Received {body.decode()}")
    frame_metadata = json.loads(body.decode())
    path = os.path.join(base_path, frame_metadata["path"])

    frame_source = JpegSource(source_id, path, pts=frame_counter)
    frame_metadata_cache[frame_counter] = path
    frame_counter +=1
    source(frame_source, send_eos=False)

channel.basic_consume(
    queue='Frames',
    on_message_callback=callback,
    auto_ack=True
)

consumer = threading.Thread(target=channel.start_consuming, args=())
consumer.start()
for result in sink:
    print(f'Sink result trace_id {result.trace_id}')
    print(dir(result))
    if result.eos:
        # second message is the EOS
        print('EOS')
        # Optionally send a shutdown message to the module
        # source.send_shutdown(source_id, shutdown_auth)
        break
    original_path= frame_metadata_cache[result.frame_meta.pts]

    img = np.frombuffer(result.frame_content, dtype=np.uint8)
    img = cv2.imdecode(img, cv2.IMREAD_COLOR)

    # save the result image
    # the image will anything that the module has drawn on top of the input image
    print(f'Saving result image to {result_img_path}')
    cv2.imwrite(original_path+"_bboxed.jpeg", img)

    # print the processing logs from the module
    print('Logs from the module:')
    result.logs().pretty_print()

consumer.join()
# Shutdown the Jaeger tracer
telemetry.shutdown()
print('Done.')
