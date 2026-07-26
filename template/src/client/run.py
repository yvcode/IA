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
import uuid
from savant.api.builder import build_bbox
from savant.client import JaegerLogProvider, JpegSource, SinkBuilder, SourceBuilder
from dataclasses import dataclass
from template.src.client.run import jaeger_endpoint, source_id

MODEL_NAME = "adaface_ir50_webface4m_90fb74c"

print('Starting new Savant client...!')


@dataclass
class FrameContext:
    frame_index: int
    original_file_path: str


class MetadataRepository:
    def __init__(self):
        self.repository = {}
        self.counter = 0

    def save(self, frame, index, original_file_path):
        """guid = str(uuid.uuid4())
        frame.set_attribute("frame_repository", "guid", guid)
        context = FrameContext(original_file_path, index)
        self.repository[guid] = context"""
        frame.pts = self.counter
        self.repository[self.counter] = original_file_path
        self.counter += 1


    def get(self, frame):
        #guid = frame.get_attribute("frame_repository", "guid")
        #return self.repository[guid]
        return self.repository[frame.pts]


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

class FrameExtractor:
    def extract(self, video_path):
        video = cv2.VideoCapture(video_path)
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
    def __init__(self, savant_uri, jaeger_provider):
        self.savant_source =  (
            SourceBuilder()
            .with_log_provider(jaeger_provider)
            .with_socket(savant_uri)
            .build()
        )

    def publish(self, frame_source):
        self.savant_source(frame_source, send_eos=False)


class SourceManager:
    def __init__(self, metadata_repository, jaeger_provider):
        self.metadata_repository = metadata_repository
        self.video_provider = RabbitVideoProvider("rabbitmq", "Frames", self.process)
        self.provider =  threading.Thread(target=self.video_provider.provide_loop, args=())
        self.provider.start()

        self.frame_extractor = FrameExtractor()
        self.source_mapper = SourceMapper("vid-source")
        self.savant_publisher = SavantPublisher('pub+connect:ipc:///tmp/zmq-sockets/input-video.ipc',
                                            jaeger_provider)

    def process(self, path):
        for index, frame in enumerate(self.frame_extractor.extract(path)):
            frame_source = self.source_mapper.map(frame)
            self.metadata_repository.save(frame_source, index, path)
            self.savant_publisher.publish(frame_source)

class ResultProvider:
    def __init__(self, savant_uri, source_id, jaeger_provider, callback):
        self.provider = (
            SinkBuilder()
            .with_socket(savant_uri)
            .with_source_id(source_id)
            .with_idle_timeout(800)
            .with_log_provider(jaeger_provider)
            .build()
        )
        self.callback = callback
    def provide_loop(self):
        for result in self.provider:
            self.callback(result)

class EosResultFilter:
    def is_valid(self, result):
        if result.eos:
            return False
        return True

class FaceObjectFilter:
    def is_valid(self, result_obj):
        return result_obj.label == "face"

class FaceEncoder:
    def __init__(self, model_name):
        self.model_name = model_name
    def encode(self, obj):
        attr = obj.get_attribute(self.model_name, "feature")
        if attr is not None:
            return attr.values[0].as_floats()
        return None

class EncodingsRabbitPublisher:
    def __init__(self, rabbit_host, queue_name, video_callback):
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))
        self.channel = connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def publish(self, file_path, frame_index, encodings):
        msg = {"file_path": file_path, "frame_index": frame_index, "encodings": encodings}
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=json.dumps(msg))



class SinkManager: # Get repository shared with InputManager to sync parallel. share metadata on files using their pts.
    def __init__(self, metadata_repository, jaeger_provider):
        self.metadata_repository = metadata_repository
        self.result_provider = ResultProvider('sub+connect:ipc:///tmp/zmq-sockets/output-video.ipc',
                                             'vid-source',
                                             jaeger_provider,
                                              self.process
                                              )
        self.provider = threading.Thread(target=self.result_provider.provide_loop, args=())
        self.provider.start()
        self.result_filter = EosResultFilter()
        self.object_filter = FaceObjectFilter()
        self.encoder = FaceEncoder(MODEL_NAME)
        self.publisher = EncodingsRabbitPublisher("rabbitmq", "Results", self.process)

    def process(self, result):
        if not self.result_filter.is_valid(result):
            return
        path = self.metadata_repository.get(result.frame_meta) # should be replaced
        encodings = []
        for obj in result.frame_meta.get_all_objects():
            if not self.object_filter.is_valid(obj):
                continue

            encodings.append(self.encoder.encode(obj))
        self.publisher.publish(path, 666, encodings) # 666 to be replaced with the context



JaegerLogProvider(jaeger_endpoint)

jaeger_provider = JaegerLogProvider(jaeger_endpoint)
metadata_repository = MetadataRepository()
SinkManager = SinkManager(metadata_repository, jaeger_provider)

SourceManager = SourceManager(metadata_repository, jaeger_provider)




