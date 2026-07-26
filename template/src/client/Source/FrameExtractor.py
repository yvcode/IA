import cv2


class FrameExtractor:
    def extract(self, video_path):
        video = cv2.VideoCapture(video_path)
        success, frame = video.read()
        while success:
            yield frame
            success, frame = video.read()
