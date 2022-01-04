"""
These simple tests are just asserting that the key value store class works as
intended, with two different mock backends.
"""
import unittest
import tempfile
from views_storage.key_value_store import KeyValueStore
from views_storage.backends import local, dictionary
from views_storage.serializers import pickle

class TestKeyValueStore(unittest.TestCase):
    def test_key_value_store_dict(self):
        class TestKvDict(KeyValueStore):
            def __init__(self):
                self.backend = dictionary.DictBackend()
                self.serializer = pickle.Pickle()
                super().__init__()

        kv = TestKvDict()
        kv.write("foo","bar")
        self.assertEqual(kv.read("foo"), "bar")


    def test_key_value_store_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            class TestKvLocal(KeyValueStore):
                def __init__(self):
                    self.backend = local.Local(tmp)
                    self.serializer = pickle.Pickle()
                    super().__init__()

            kv = TestKvLocal()
            kv.write("foo","bar")
            self.assertEqual(kv.read("foo"), "bar")
