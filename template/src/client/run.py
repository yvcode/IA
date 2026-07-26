import time
from savant.client import JaegerLogProvider

from Sink.EncodingsRabbitPublisher import EncodingsRabbitPublisher
from Sink.EosResultFilter import EosResultFilter
from Sink.FaceEncoder import FaceEncoder
from Sink.FaceObjectFilter import FaceObjectFilter
from Source.FrameExtractor import FrameExtractor
from MetadataRepository import MetadataRepository
from RateLimiter import RateLimiter
from Source.SavantPublisher import SavantPublisher
from Sink.SinkManager import SinkManager
from Source.SourceManager import SourceManager
from Source.SourceMapper import SourceMapper

MODEL_NAME = "adaface_ir50_webface4m_90fb74c"

time.sleep(10) # Wait for dependencies to initiate

print('Starting new Savant client...!')

jaeger_endpoint = "http://jaeger:16686"
JaegerLogProvider(jaeger_endpoint)

jaeger_provider = JaegerLogProvider(jaeger_endpoint)
rate_limiter = RateLimiter(1)
metadata_repository = MetadataRepository()
face_encoder = FaceEncoder(MODEL_NAME)
result_filter = EosResultFilter()
object_filter = FaceObjectFilter()
publisher = EncodingsRabbitPublisher("rabbitmq", "Results")

frame_extractor = FrameExtractor()
savant_publisher = SavantPublisher('pub+connect:ipc:///tmp/zmq-sockets/input-video.ipc', jaeger_provider)
source_mapper = SourceMapper("vid-source")

SinkManager = SinkManager(metadata_repository, jaeger_provider, rate_limiter, face_encoder, result_filter, object_filter, publisher)
SourceManager = SourceManager(metadata_repository, jaeger_provider, rate_limiter, frame_extractor, savant_publisher, source_mapper)