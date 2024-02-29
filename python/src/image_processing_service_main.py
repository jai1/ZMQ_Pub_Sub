import argparse
import zmq
import os

from watchdog.observers import Observer
from zmq import Socket

from BackgroundThread import BackgroundThread
from Constants import PUB_SUB_TCP_PORT, REQ_REP_TCP_PORT
from EventHandler import EventHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_folder", type=str, required=True)
    return parser.parse_args()


def initialize_zmq() -> tuple[Socket, Socket]:
    # Using Pub Sub pattern
    context = zmq.Context()
    publisher_socket = context.socket(zmq.PUB)
    publisher_socket.bind(f"tcp://*:{PUB_SUB_TCP_PORT}")

    # Using Reply/Request pattern
    late_subscriber_socket = context.socket(zmq.XREP)
    late_subscriber_socket.bind(f"tcp://*:{REQ_REP_TCP_PORT}")
    return publisher_socket, late_subscriber_socket


def run(watch_dir: str) -> None:
    # 1. Initialize zmq for PUB / SUB and REQ / REPLY
    publisher_socket, late_subscriber_socket = initialize_zmq()

    # 2. Create a watch
    event_handler = EventHandler(publisher_socket)
    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=False)  # Set recursive to True to monitor subdirectories
    observer.start()

    # 3. Touch all the previously created file before the watch was set.
    for filename in os.listdir(watch_dir):
        complete_filename = os.path.join(watch_dir, filename)
        if os.path.isfile(complete_filename) and complete_filename.endswith(".jpg"):
            os.utime(complete_filename)

    # 4. Handle late subscribers and printing stats
    thread = BackgroundThread(publisher_socket, late_subscriber_socket, event_handler)
    thread.run()


def main() -> None:
    args = parse_args()
    run(args.log_folder)


if __name__ == "__main__":
    main()
