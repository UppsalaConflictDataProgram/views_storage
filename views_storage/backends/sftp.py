from stat import S_ISDIR, S_ISREG
import os
from tempfile import NamedTemporaryFile
import paramiko
import psycopg2
import sqlalchemy as sa
from .. import models
from . import storage_backend


class Sftp(storage_backend.StorageBackend):
    def __init__(self, user: str, host: str, port: int, dbname: str, sslmode: str, folder: str = "."):
        self._keystore_connection_string = (
            f"host={host} "
            f"port={port} "
            f"dbname={dbname} "
            f"user={user} "
            f"sslmode={sslmode}"
        )

        self.key = self._fetch_paramiko_key()
        self.connection = self._connect()
        self.connection.chdir(None)
        self._folder = folder

    def store(self, key: str, value: bytes) -> None:
        path = self._path(key)
        with self.connection.open(path, "wb") as f:
            f.write(value)

    def retrieve(self, key: str) -> bytes:
        path = self._path(key)
        with self.connection.open(path, "rb") as f:
            return f.read()

    def exists(self, key: str) -> bool:
        key = self._path(key)
        try:
            _ = self.connection.stat(key)
            return True
        except IOError:
            return False

    def list(self, path: str = ".") -> models.Listing:
        folders = []
        files = []

        for entry in self.connection.listdir_attr(self._path(path)):
            mode = entry.st_mode
            if S_ISDIR(mode):
                folders.append(entry.filename)
            elif S_ISREG(mode):
                files.append(entry.filename)

        return models.Listing(folders=folders, files=files)

    def keys(self):
        return self.list()

    def _db_connect(self):
        return psycopg2.connect(self._keystore_connection_string)

    def _fetch_paramiko_key(self) -> paramiko.Ed25519Key:
        """
        _fetch_paramiko_key
        ===================

        returns:
            paramiko.Ed25519Key

        Connects to a certificate store on "key" server to the writing server using SSL.
        This is currently Janus, but can be migrated to any safe store solution like a vault.
        Getting a private key in this way SHOULD be safe, since we do it based on already critical SSL certs.
        If these are compromised the whole chain is compromised. If you have a better idea, however, do say.
        The user key will be rotated frequently, so is not cached.
        This key points to a very low privileged user that can only write or read from a dedicated store.
        The dedicated store is chrooted to and from the server.
        """
        cert_table = sa.Table(
            "sftp_cert",
            sa.MetaData(),
            sa.Column("sftp_cert", sa.String),
            schema="public")

        query = sa.select([cert_table.c["sftp_cert"]])

        print(str(query))

        with self._db_connect() as con:
            c = con.cursor()
            c.execute(str(query))
            cert,*_ = c.fetchone()

        with NamedTemporaryFile(dir=".", mode="w") as x:
            x.write(cert)  # .encode())
            x.seek(0)
            key = paramiko.Ed25519Key.from_private_key_file(x.name, password=None)
            return key

    def _connect(self) -> paramiko.SFTPClient:
        """
        _connect
        ========

        returns:
            paramiko.SFTPClient

        Initialize a connection and connect to the sftp store.
        The user and key are the dedicated user and key generated above.
        DO NOT use your views user share!
        """
        t = paramiko.Transport(("hermes", 22222))
        t.connect(hostkey=None, pkey=self.key, username="predictions")
        return paramiko.SFTPClient.from_transport(t)

    @staticmethod
    def _file_name_fixer(file_name, extension):
        extension = extension.strip(" .").lower()
        file_name = file_name.strip().lower()
        ext_len = -(len(extension) + 1)
        file_name = (
            file_name
            if file_name[ext_len:] == f".{extension}"
            else file_name + "." + extension
        )
        return file_name

    def _path(self, key):
        return os.path.join(self._folder, key)

    def __del__(self):
        """
        Destruct the key and close the connection if it exists.
        :return: Nothing, it's a destructor.
        """
        self.key = None
        try:
            self.connection.close()
        except TypeError:
            pass
