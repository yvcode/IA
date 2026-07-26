import io
import cv2
from savant.client import JpegSource


class SourceMapper:
    def __init__(self, source_id):
        self.source_id = source_id

    def map(self, frame):
        success, buffer = cv2.imencode('.jpg', frame)
        if not success:
            return

        jpeg_bytes = buffer.tobytes()
        frame_source = JpegSource(
            source_id=self.source_id,
            file=io.BytesIO(jpeg_bytes)
        )
        return frame_source
