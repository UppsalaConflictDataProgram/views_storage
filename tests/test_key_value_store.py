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
        kv = KeyValueStore(backend = dictionary.DictBackend(), serializer = pickle.Pickle())
        kv.write("foo","bar")
        self.assertEqual(kv.read("foo"), "bar")


    def test_key_value_store_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            kv = KeyValueStore(backend = local.Local(tmp), serializer = pickle.Pickle())
            kv.write("foo","bar")
            self.assertEqual(kv.read("foo"), "bar")
