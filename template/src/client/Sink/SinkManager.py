import threading

from Sink.ResultProvider import ResultProvider


class SinkManager: # Get repository shared with InputManager to sync parallel. share metadata on files using their pts.
    def __init__(self, metadata_repository, jaeger_provider, rate_limiter, face_encoder, result_filter, object_filter, publisher):
        self.metadata_repository = metadata_repository
        self.rate_limiter = rate_limiter
        self.result_filter = result_filter
        self.object_filter = object_filter
        self.encoder = face_encoder
        self.publisher = publisher

        self.result_provider = ResultProvider('sub+connect:ipc:///tmp/zmq-sockets/output-video.ipc',
                                             'vid-source',
                                              jaeger_provider,
                                              self.process
                                              )
        self.provider = threading.Thread(target=self.result_provider.provide_loop, args=())
        self.provider.start()

    def process(self, result):
        self.rate_limiter.release()
        if not self.result_filter.is_valid(result):
            return
        frame_context = self.metadata_repository.get(result)
        encodings = []
        for obj in result.frame_meta.get_all_objects():
            if not self.object_filter.is_valid(obj):
                continue

            encodings.append(self.encoder.encode(obj))
        self.publisher.publish(frame_context.original_file_path, frame_context.frame_index, encodings)
