from typing import Optional
from stat import S_ISDIR, S_ISREG
import os
from tempfile import NamedTemporaryFile
from cryptography import x509

import paramiko
import sqlalchemy as sa

from . import settings


class Operations:
    """
    SFTPOperations
    ==============

    """

    def __init__(self):
        self.username = (
            settings.DB_USER if settings.DB_USER is not None else self._certificate_user
        )
        self._keystore_connection_string = (
            f"host={settings.DB_HOST} "
            f"port={settings.DB_PORT} "
            f"dbname={settings.DB_NAME} "
            f"user={settings.DB_USER} "
            f"sslmode={settings.DB_SSLMODE}"
        )

        self.key = self._fetch_paramiko_key()
        self.connection = self._connect()
        self.connection.chdir(None)

    def file_exists(self, path: str) -> bool:
        """
        file_exists
        ===========

        parameters:
            path (str): The path to a file on the server

        returns:
            bool: Whether file exists or not

        For a given path check if file exists
        """
        try:
            _ = self.connection.stat(path)
            return True
        except IOError:
            return False

    def mkdir(self, path: str) -> None:
        """
        mkdir
        =====

        parameters:
            path (str): Path to folder to create

        returns:
            None

        Makes the specified directory on the remote server.
        """
        path = self._path_fixer(path)

        try:
            self.connection.chdir(path)
        except IOError:
            self.connection.mkdir(path)
            self.connection.chdir(path)

        self.connection.chdir(None)

    def ls(self, path: str) -> models.Listing:
        """
        ls
        ==

        parameters:
            path (str): Path to a folder on the server to ls

        returns:
            views_sftp.models.Listing

        Returns a listing for the specified path on the server.
        """
        folders = []
        files = []
        for entry in self.connection.listdir_attr(path):
            mode = entry.st_mode
            if S_ISDIR(mode):
                folders.append(entry.filename)
            elif S_ISREG(mode):
                files.append(entry.filename)

        return models.Listing(folders=folders, files=files)

    @property
    def _certificate_user(self) -> Optional[str]:
        """
        _fetch_views_user
        =================

        Fetch the ViEWS user name and check that a certificate exists.
        Each user authenticates to ViEWS using a username and a ViEWS signed PEM certificate.
        The certificate, which should be installed in .postgres contains the user name as part of the CN field.
        This fetches the user name from the certificate.
        """

        cert_file_path = os.path.expanduser("~/.postgresql/postgresql.crt")
        try:
            assert os.path.exists(cert_file_path)
        except AssertionError:
            return None

        with open(cert_file_path) as f:
            cert = x509.load_pem_x509_certificate(f.read().encode())

        return self._username_from_certificate(cert)

    @staticmethod
    def _username_from_certificate(cert: x509.Certificate):
        common_name = cert.subject.rfc4514_string().split(",")
        try:
            # Extract the content of the CN field from the x509 formatted string.
            views_user_name = [
                i.split("=")[1] for i in common_name if i.split("=")[0] == "CN"
            ][0]
        except IndexError:
            raise ConnectionError(
                "Something is wrong with the ViEWS Certificate. Contact ViEWS to obtain authentication!"
            )
        return views_user_name

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

        cert_store_engine = sa.create_engine(self._keystore_connection_string)
        cert_table = sa.Table(
            "sftp_cert",
            sa.MetaData(),
            schema="public",
            autoload=True,
            autoload_with=cert_store_engine,
        )

        query = sa.select([cert_table.c["sftp_cert"]])
        with cert_store_engine.connect() as conn:
            cert = conn.execute(query).fetchone()[0]

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

    @staticmethod
    def _path_fixer(path):
        store_path = path.rstrip("/ ") + "/"
        return store_path

    @staticmethod
    def _path_maker(file_name, path, extension):
        return Operations._path_fixer(path) + Operations._file_name_fixer(
            file_name, extension
        )

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
