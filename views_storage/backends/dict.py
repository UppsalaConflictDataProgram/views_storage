
from . import storage_backend


class DictBackend(storage_backend.StorageBackend, dict):

    def store(self, key: str, value: bytes) -> None:
        self[key] = value

    def retrieve(self, key: str) -> bool:
        return self[key]
