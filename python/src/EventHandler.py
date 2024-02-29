import json

from numpy import asarray
from watchdog.events import FileSystemEventHandler, FileSystemEvent
import time
import os

from zmq import Socket

from Constants import PRODUCER_STATS_INTERVAL_SECONDS
from Message import Message, MessageEncoder

from PIL import Image
from utils import classify_image


def get_event_classification(image_path: str) -> int:
    img = Image.open(image_path)
    numpy_data_array = asarray(img)
    return classify_image(numpy_data_array)


class EventHandler(FileSystemEventHandler):
    def __init__(self, publisher_socket: Socket):
        self.__publisher_socket = publisher_socket
        self.__msgs_published = 0
        self.__last_report_time = time.time()
        self.__message_set = set()

    def on_created(self, event: FileSystemEvent) -> None:
        self.handle_event(event.src_path)

    def on_modified(self, event: FileSystemEvent) -> None:
        self.handle_event(event.src_path)

    def handle_event(self, image_path: str) -> None:
        if image_path.endswith(".jpg") and os.path.isfile(image_path):
            # 1. Classify event
            classification = get_event_classification(image_path)
            # 2. Publish results
            json_string = json.dumps(Message(image_path, classification), cls=MessageEncoder)
            self.__publisher_socket.send_string(json_string)
            self.__message_set.add(json_string)
            self.__msgs_published += 1
            # 3. Delete file to avoid duplicate event handling.
            os.remove(image_path)

    def get_published_messages(self) -> set:
        return set.copy(self.__message_set)

    def print_stats(self):
        # Publish stats every one second
        current_time = time.time()
        if current_time - self.__last_report_time >= PRODUCER_STATS_INTERVAL_SECONDS:
            print(f"Throughput: {self.__msgs_published} images / second")
            self.__msgs_published = 0
            self.__last_report_time = current_time
