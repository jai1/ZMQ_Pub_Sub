import json
from types import SimpleNamespace as Namespace


class Message():
    def __init__(self, message_id: str, classification: int):
        self.message_id = message_id
        self.classification = classification


class MessageEncoder(json.JSONEncoder):
    def default(self, obj: Message):
        return obj.__dict__

