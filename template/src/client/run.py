import json
import os
import time
import pika
import threading
import traceback
import cv2
import numpy as np
from savant_rs import telemetry
from savant_rs.match_query import MatchQuery
from savant_rs.telemetry import (
    ContextPropagationFormat,
    Protocol,
    TelemetryConfiguration,
    TracerConfiguration,
)
from savant_rs.primitives import Attribute, AttributeValue

from savant.api.builder import build_bbox
from savant.client import JaegerLogProvider, JpegSource, SinkBuilder, SourceBuilder

MODEL_NAME = "adaface_ir50_webface4m_90fb74c"

print('Starting Savant client...')


connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()
pub_channel = connection.channel()

channel.queue_declare(queue='Frames', durable=True)
pub_channel.queue_declare(queue='Results', durable=True)



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
pool_limiter = threading.Semaphore(1)
active_delivery_tag = None
# Build the source
source = (
    SourceBuilder()
    .with_log_provider(JaegerLogProvider(jaeger_endpoint))
    .with_socket('pub+connect:ipc:///tmp/zmq-sockets/input-video.ipc')
    # Note: healthcheck port should be configured in the module.
    .with_module_health_check_url(healthcheck_url)
    .with_module_health_check_timeout(800)
    .build()
)

sink = (
    SinkBuilder()
    .with_socket('sub+connect:ipc:///tmp/zmq-sockets/output-video.ipc')
    .with_source_id(source_id)
    .with_idle_timeout(800)
    .with_log_provider(JaegerLogProvider(jaeger_endpoint))
    # Note: healthcheck port should be configured in the module.
    .with_module_health_check_url(healthcheck_url)
    .build()
)


def callback(ch, method, properties, body):
    global frame_counter
    print(f" [x] Received {body.decode()}")
    pool_limiter.acquire()
    frame_metadata = json.loads(body.decode())
    path = os.path.join(base_path, frame_metadata["path"])
    try:
        frame_source = JpegSource(source_id, path, pts=frame_counter)
        frame_metadata_cache[frame_counter] = path
        frame_counter +=1
        source(frame_source, send_eos=False)
    except Exception:
        print(traceback.format_exc())
channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue='Frames',
    on_message_callback=callback,
    auto_ack=True
)

consumer = threading.Thread(target=channel.start_consuming, args=())
#time.sleep(60)
print("Start consuming")
consumer.start()
for result in sink:
    try:
        pool_limiter.release()
        print(f'Sink result trace_id {result.trace_id}')
        if result.eos:
            # second message is the EOS
            print('EOS')
            # Optionally send a shutdown message to the module
            # source.send_shutdown(source_id, shutdown_auth)
            continue
        original_path= frame_metadata_cache[result.frame_meta.pts]
        frame_metadata_cache.pop(result.frame_meta.pts, None)
        faces = []
        #print(f"result dir: {dir(result)}")
        #print(f"result frame meta dir: {dir(result.frame_meta)}")
        for obj in result.frame_meta.get_all_objects():
            if obj.label=="frame":
                continue
            #print(f"obj dir {obj.label}: {dir(obj)}")
            #print(f"attrs: {obj.attributes}")
            print(f"Obj label: {obj.label}")
            if obj.label == "face":
                print("FACE FOUND")
                faces.append(obj.get_attr_meta(MODEL_NAME, 'feature').value)
        result = {"faces": faces, "path": original_path}
        pub_channel.basic_publish(exchange='', routing_key='Results', body=json.dumps(result))
    except Exception:
        print(traceback.format_exc())

consumer.join()
# Shutdown the Jaeger tracer
telemetry.shutdown()
print('Done.')
