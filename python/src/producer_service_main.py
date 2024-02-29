import argparse
import cv2
import os

from Constants import FRAMES_PER_SECOND
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--video_file", type=str, required=True)
    parser.add_argument("--log_folder", type=str, required=True)
    return parser.parse_args()


def run(video_file: str, log_folder: str) -> None:
    # Create log directory
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # Capture video
    cap = cv2.VideoCapture(os.path.join(video_file))

    # Check if video opened successfully
    if not cap.isOpened():
        print(f"Error opening the video file {video_file}")
        return

    frame_count = 0
    while True:
        # Capture frame-by-frame
        ret, frame = cap.read()

        if not ret:
            break

        # Save frame only if frame rate condition is met
        if frame_count % int(cap.get(cv2.CAP_PROP_FPS) / FRAMES_PER_SECOND) == 0:
            filename = f"frame_{frame_count}.jpg"
            cv2.imwrite(os.path.join(log_folder, filename), frame)

        frame_count += 1

    # Release capture
    cap.release()
    cv2.destroyAllWindows()


def main() -> None:
    args = parse_args()
    run(args.video_file, args.log_folder)
    pass


if __name__ == "__main__":
    main()
