from threading import Thread

from zmq import Socket

from Constants import WAIT_TIME_MS
from EventHandler import EventHandler


class BackgroundThread(Thread):
    def __init__(self, publisher_socket: Socket, late_subscriber_socket: Socket, event_handler: EventHandler):
        self.__publisher_socket = publisher_socket
        self.__late_subscriber_socket = late_subscriber_socket
        self.__event_handler = event_handler

    def run(self) -> None:
        while True:
            event = self.__late_subscriber_socket.poll(timeout=WAIT_TIME_MS)
            if event == 0:
                # timeout reached before any events were queued
                pass
            else:
                # Receive subscription message from a late subscriber
                self.__late_subscriber_socket.recv()
                message_set = self.__event_handler.get_published_messages()
                # Check if there are any messages in the queue
                for message in message_set:
                    self.__publisher_socket.send_string(message)
            self.__event_handler.print_stats()
