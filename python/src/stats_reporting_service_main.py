import json
import time
from collections import Counter
import zmq
from zmq import Socket

from Constants import PUB_SUB_TCP_PORT, REQ_REP_TCP_PORT, WAIT_TIME_MS, CONSUMER_STATS_INTERVAL_SECONDS
from Message import Message


def initialize_zmq() -> Socket:
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(f"tcp://localhost:{PUB_SUB_TCP_PORT}")

    late_subscriber_socket = context.socket(zmq.REQ)
    late_subscriber_socket.connect(f"tcp://localhost:{REQ_REP_TCP_PORT}")
    late_subscriber_socket.send_string("New Subscriber connect")

    return socket


def process(socket: Socket) -> None:
    # For ignoring duplicate items
    message_id_set = set()
    class_counts = Counter()
    last_report_time = time.time()

    while True:
        event = socket.poll(timeout=WAIT_TIME_MS)  # wait 100 milliseconds
        if event == 0:
            # timeout reached before any events were queued
            pass
        else:
            # events queued within our time limit
            received_message = socket.recv()
            message = json.loads(received_message.decode(), object_hook=lambda d: Message(**d))
            if message.message_id not in message_id_set:
                message_id_set.add(message.message_id)
                class_counts[message.classification] += 1

        current_time = time.time()
        if current_time - last_report_time >= CONSUMER_STATS_INTERVAL_SECONDS:
            print(f"In the past 10s:")
            for label, count in class_counts.most_common():
                print(f"class {label} detected {count} times")
            class_counts.clear()  # Reset counter for next reporting period

            last_report_time = current_time


def main() -> None:
    # 1. Initialize zmq
    socket = initialize_zmq()
    # 2. Subscribe to the queue
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    # 3. Read messages and emit class frequency
    process(socket)


if __name__ == "__main__":
    main()