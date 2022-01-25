
import unittest
from sqlalchemy import create_engine
from views_storage.backends import sql

class TestDbBackend(unittest.TestCase):
    def test_db_backend(self):
        engine = create_engine("sqlite://")
        con = engine.connect()
        con.execute("create table foo (x text not null primary key, y int, z text)")
        backend = sql.Sql(engine, "foo")
        
        data = {"y":2, "z": "bar"}
        backend.store("foo", data)
        self.assertEqual(backend.retrieve("foo"), data)
