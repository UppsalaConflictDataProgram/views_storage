import time
import random
import hashlib
import unittest
from azure.storage.blob import BlobServiceClient
import docker
from views_storage.backends import azure

class TestAzureStorage(unittest.TestCase):
    def setUp(self):
        self.client = docker.DockerClient.from_env()

        try:
            self.client.containers.get("views-storage-azurite").kill()
        except:
            pass

        self.azurite = self.client.containers.run("mcr.microsoft.com/azure-storage/azurite",
                ports = {
                    10000:10000
                    },
                remove = True,
                detach = True,
                name = "views-storage-azurite",
                entrypoint = "azurite-blob --blobHost 0.0.0.0")
        time.sleep(1.5)
        self._bs_constring = (
                    "DefaultEndpointsProtocol=http;"
                    "AccountName=devstoreaccount1;"
                    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
                )
        self.bs_client = BlobServiceClient.from_connection_string(self._bs_constring)
        self.bs_client.create_container("test")

    def tearDown(self):
        self.azurite.kill()
        self.client.close()
        self.bs_client.close()

    def test_storage_driver(self):
        azure_bs = azure.AzureBlobStorageBackend(self._bs_constring, "test")

        x = hashlib.sha256(str(random.random()).encode()).hexdigest()
        azure_bs.store("test",x)

        self.assertTrue(azure_bs.exists("test"))
        self.assertIn("test", azure_bs.keys())
        self.assertEqual(azure_bs.retrieve("test").decode(), x)
