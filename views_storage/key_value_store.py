from abc import ABC
from typing import Generic, TypeVar
from .serializers import serializer
from .backends import storage_backend

T = TypeVar("T")


class KeyValueStore(ABC, Generic[T]):
    """
    KeyValueStore
    =============

    Abstract class for a key-value store combining a storage backend with a
    serializer-deserializer,

    """

    backend: storage_backend.StorageBackend
    serializer: serializer.Serializer

    def __init__(self):
        pass

    def exists(self, key: str) -> bool:
        return self.backend.exists(key)

    def write(self, key: str, value: T, overwrite: bool = False):
        if self.exists(key) and not overwrite:
            raise FileExistsError("File exists, overwrite is False")

        self.backend.store(key, self.serializer.serialize(value))

    def read(self, key: str) -> T:
        return self.serializer.deserialize(self.backend.retrieve(key))
