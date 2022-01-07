from typing import Any
import unittest
import time
import os
import docker

from views_storage import key_value_store
from views_storage.backends import sftp
from views_storage.serializers import pickle

client = docker.DockerClient.from_env()
client.images.build(path = "tests/mock_key_db", tag = "views-storage-test-key-db")
client.close()

class TestSftp(unittest.TestCase):
    def setUp(self):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        self.client = docker.DockerClient.from_env()
        self.sftp = self.client.containers.run("atmoz/sftp",
                environment = {
                    "SFTP_USERS": "testuser:pleaselogmein:::upload"
                    },
                volumes = {
                    os.path.join(test_dir,"testcert/test_pub"): {
                        "bind": "/home/testuser/.ssh/keys/id_rsa.pub",
                        "mode": "ro"
                        }
                    },
                ports = {
                    22: 2222
                    },
                remove = True,
                detach = True)

        self.db = self.client.containers.run(
                "views-storage-test-key-db",
                ports = {
                    5432: 2345
                    },
                remove = True,
                detach = True)

        time.sleep(1.5)

    def tearDown(self):
        self.sftp.kill()
        self.db.kill()
        self.client.close()

    def test_null(self):
        store: key_value_store.KeyValueStore[Any] = key_value_store.KeyValueStore(
                backend = sftp.Sftp(
                        host = "0.0.0.0",
                        port = 2222,
                        user = "testuser",
                        key_db_host = "0.0.0.0",
                        key_db_dbname = "keys",
                        key_db_user = "testuser",
                        key_db_password = "pleaselogmein",
                        key_db_port = 2345,
                        key_db_sslmode = "allow",
                        folder = "upload/my/fantastic/data"
                    ),
                serializer = pickle.Pickle()
                )

        store.write("yee", "haw")
        self.assertEqual(store.read("yee"), "haw")

