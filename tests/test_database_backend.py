
import unittest
from sqlalchemy import create_engine
from views_storage.backends import sql

class TestDbBackend(unittest.TestCase):
    def setUp(self):
        self.engine = engine = create_engine("sqlite://")
        con = engine.connect()
        con.execute("create table abc (x text not null primary key, y int, z text)")
        self.backend = sql.Sql(self.engine, "abc")

    def test_db_backend(self):
        data = {"y":2, "z": "def"}
        self.backend.store("abc", data)
        self.assertEqual(self.backend.retrieve("abc"), data)

    def test_overwrite(self):
        self.backend.store("abc", {"y":10, "z": "data"})
        self.backend.store("abc", {"y":12, "z": "other data"})
        self.assertEqual(self.backend.retrieve("abc"), {"y":12, "z": "other data"})

    def test_crud_exceptions(self):
        self.assertRaises(ValueError, lambda: self.backend.store("abc", {"def": "ghi"}))
        self.assertRaises(ValueError, lambda: self.backend.store("abc", {"y":"def", "z": 1}))
        self.assertRaises(KeyError, lambda: self.backend.retrieve("nonexistent"))

    def test_bad_instantiation(self):
        self.assertRaises(KeyError, lambda: sql.Sql(self.engine, "nonexistent"))
