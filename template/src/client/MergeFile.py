import os
import cv2

base_path = "/home/ia/user3/IA/NonBlur/"

frames = os.listdir(base_path)
first_frame = cv2.imread(os.path.join(base_path, frames[0]))
height, width, channels = first_frame.shape
writer = cv2.VideoWriter_fourcc(*'mp4v')
out = cv2.VideoWriter(r'/home/ia/user3/IA/out_sorted_full.mp4', writer, 30, (width, height))
sorted_frames = sorted(frames, key=lambda f: int(f[5:-16]))

for frame in sorted_frames:
        frame_path = cv2.imread(os.path.join(base_path, frame))
        out.write(frame_path)

out.release()
print("Finished")
