import threading

from Source.RabbitVideoProvider import RabbitVideoProvider
from MetadataRepository import FrameContext


class SourceManager:
    def __init__(self, metadata_repository, jaeger_provider, rate_limiter, frame_extractor, savant_publisher, source_mapper):
        self.metadata_repository = metadata_repository
        self.rate_limiter = rate_limiter
        self.frame_extractor = frame_extractor
        self.source_mapper = source_mapper
        self.savant_publisher = savant_publisher

        self.video_provider = RabbitVideoProvider("rabbitmq", "Videos", self.process)
        self.provider =  threading.Thread(target=self.video_provider.provide_loop, args=())
        self.provider.start()


    def process(self, path):
        for index, frame in enumerate(self.frame_extractor.extract(path)):
            self.rate_limiter.acquire()
            frame_source = self.source_mapper.map(frame)
            frame_context = FrameContext(index, path)
            frame_source = self.metadata_repository.save(frame_source, frame_context)
            self.savant_publisher.publish(frame_source)
