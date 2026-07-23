import json
import pika
import os
import cv2


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='Frames', durable=True)


vidcap = cv2.VideoCapture(r'/home/ia/user3/IA/video.mp4')
success,image = vidcap.read()
count = 0
while success:
    filename=f"frame{count}.jpg"
    path = os.path.join("/home/ia/user3/IA/Frames/", filename)
    #cv2.imwrite(path, image)
    success,image = vidcap.read()
    file_msg = {"path": filename}
    channel.basic_publish(exchange='', routing_key='Frames', body=json.dumps(file_msg))
    count += 1
