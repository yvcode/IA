import uuid
from dataclasses import dataclass

from savant_rs.primitives import Attribute, AttributeValue, VideoFrameUpdate


@dataclass
class FrameContext:
    frame_index: int
    original_file_path: str

class MetadataRepository:
    def __init__(self):
        self.repository = {}
        self.counter = 0

    def save(self, source, frame_context):
        guid = str(uuid.uuid4())
        guid_update = VideoFrameUpdate()
        guid_update.add_frame_attribute(
            Attribute(
                namespace="frame_repository",
                name="guid",
                values=[AttributeValue.string(guid)],
            )
        )
        source = source.with_update(guid_update)
        self.repository[guid] = frame_context
        return source


    def get(self, result):
        guid = result.frame_meta.get_attribute("frame_repository", "guid").values[0].as_string()
        return self.repository[guid]
