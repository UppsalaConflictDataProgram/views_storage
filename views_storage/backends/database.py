
from views_storage.backends import storage_backend

class RestBackend(storage_backend.StorageBackend):
    def store(self, key: str, value: bytes) -> None:
        pass

    def retrieve(self, key: str) -> bytes:
        pass
