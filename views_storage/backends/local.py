
import os
from . import storage_backend

class Local(storage_backend.StorageBackend):

    def store(self, key: str, value: bytes) -> None:
        with open(self._path(key), "wb") as f:
            f.write(bytes)

    def retrieve(self, key: str):
        with open(self._path(key), "rb") as f:
            return f.read()

    def __init__(self, root: str):
        self._root = root

    def _path(self, key: str):
        return os.path.join(self._root, key)

