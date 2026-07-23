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

from template.src.client.run import jaeger_endpoint, source_id

MODEL_NAME = "adaface_ir50_webface4m_90fb74c"

print('Starting Savant client...')

class RabbitVideoProvider:
    def __init__(self, rabbit_host, queue_name, video_callback):
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.frame_counter = 0
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.rabbit_callback,
            auto_ack=True
        )
        self.video_callback = video_callback

    def provide_loop(self):
        self.channel.start_consuming()

    def _rabbit_callback(self, ch, method, properties, body):
        print(f" [x] Received {body.decode()}")
        pool_limiter.acquire()
        frame_metadata = json.loads(body.decode())
        path = frame_metadata["path"]
        video = cv2.VideoCapture(path)
        self.video_callback(video)

class FrameExtractor:
    def extract(self, video):
        success, frame = video.read()
        while success:
            yield frame
            success, frame = video.read()

class SourceMapper:
    def __init__(self, source_id):
        self.source_id = source_id
        self.counter = 0

    def map(self, frame):
        success, buffer = cv2.imencode('.jpg', frame)
        if not success:
            # Log error
            return

        jpeg_bytes = buffer.tobytes()
        frame_source = JpegSource(
            source_id=self.source_id,
            file_path=jpeg_bytes,
            pts=self.counter
        )
        return frame_source

class SavantPublisher:
    def __init__(self, savant_uri, jaeger_endpoint):
        self.savant_source =  (
            SourceBuilder()
            .with_log_provider(JaegerLogProvider(jaeger_endpoint))
            .with_socket(savant_uri)
            .build()
        )

    def publish(self, frame_source):
        self.savant_source(frame_source, send_eos=False)


class SourceManager:
    def __init__(self):
        self.video_provider = RabbitVideoProvider("rabbitmq", "Frames", self.process)
        self.provider =  threading.Thread(target=self.video_provider.provide_loop, args=())
        self.provider.start()

        self.frame_extractor = FrameExtractor()
        self.source_mapper = SourceMapper("vid-source")
        self.savant_publisher = SavantPublisher('pub+connect:ipc:///tmp/zmq-sockets/input-video.ipc', 'http://jaeger:16686')

    def process(self, video):
        for frame in self.frame_extractor.extract(video):
            frame_source = self.source_mapper.map(frame)
            self.savant_publisher.publish(frame_source)

class ResultProvider:
    def __init__(self, savant_uri, source_id, jaeger_endpoint, callback):
        self.provider = (
            SinkBuilder()
            .with_socket(savant_uri)
            .with_source_id(source_id)
            .with_idle_timeout(800)
            .with_log_provider(JaegerLogProvider(jaeger_endpoint))
            .build()
        )
        self.callback = callback
    def provide_loop(self):
        for result in self.provider:
            self.callback(result)


class SinkManager: # Get repository shared with InputManager to sync parallel. share metadata on files using their pts.
    def __init__(self):
        self.result_provider = ResultProvider('sub+connect:ipc:///tmp/zmq-sockets/output-video.ipc',
                                             'vid-source',
                                             'http://jaeger:16686',

                                              )


jaeger_provider = JaegerLogProvider(jaeger_endpoint)
RabbitToSavant(rabbit_host="rabbitmq", queue_name="Frames", source_id="test-source", jaeger_provider=jaeger_provider)

frame_counter = 0
frame_metadata_cache = {}
pool_limiter = threading.Semaphore(1)






def callback():



consumer = threading.Thread(target=channel.start_consuming, args=())
print("Start consuming")
consumer.start()
for result in sink:
    try:
        pool_limiter.release()
        print(f'Sink result trace_id {result.trace_id}')
        if result.eos:
            continue
        original_path= frame_metadata_cache[result.frame_meta.pts]
        frame_metadata_cache.pop(result.frame_meta.pts, None)
        faces = []
        for obj in result.frame_meta.get_all_objects():
            if obj.label=="frame":
                continue
            if obj.label == "face":
                attr = obj.get_attribute(MODEL_NAME, "feature")
                if attr is not None:
                    feature_vector = attr.values[0].as_floats()
                    faces.append(feature_vector)
        result = {"faces": faces, "path": original_path}
        pub_channel.basic_publish(exchange='', routing_key='Results', body=json.dumps(result))
    except Exception:
        print(traceback.format_exc())

consumer.join()
# Shutdown the Jaeger tracer
telemetry.shutdown()
print('Done.')
