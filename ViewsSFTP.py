from cryptography import x509
import os
import pandas as pd
import sqlalchemy as sa
from tempfile import NamedTemporaryFile
import paramiko
from stat import S_ISDIR, S_ISREG

paramiko.util.log_to_file("paramiko.log")

class ViewsSFTP:
    def __init__(self, df=pd.DataFrame(), key_store='janus'):
        self.df = df.copy()
        self.views_name = self.__fetch_views_user()
        self.key_store = f'postgresql://{self.views_name}@{key_store}:5432/pred3_certs'
        self.key = self.__fetch_paramiko_key()
        self.connection = self.__connect()
        self.connection.chdir(None)

    @staticmethod
    def __fetch_views_user():
        """
        Fetch the ViEWS user name and check that a certificate exists.
        Each user authenticates to ViEWS using a username and a ViEWS signed PEM certificate.
        The certificate, which should be installed in .postgres contains the user name as part of the CN field.
        This fetches the user name from the certificate.
        :return:
        """

        with open(os.path.expanduser('~/.postgresql/postgresql.crt'), 'rb') as f:
            cert = x509.load_pem_x509_certificate(f.read())
        common_name = cert.subject.rfc4514_string().split(',')
        try:
            # Extract the content of the CN field from the x509 formatted string.
            views_user_name = [i.split('=')[1] for i in common_name if i.split('=')[0] == 'CN'][0]
        except IndexError:
            raise ConnectionError(
                "Something is wrong with the ViEWS Certificate. Contact ViEWS to obtain authentication!")
        return views_user_name

    def __fetch_paramiko_key(self):
        """
        Connects to a certificate store on "key" server to the writing server using SSL.
        This is currently Janus, but can be migrated to any safe store solution like a vault.
        Getting a private key in this way SHOULD be safe, since we do it based on already critical SSL certs.
        If these are compromised the whole chain is compromised. If you have a better idea, however, do say.
        The user key will be rotated frequently, so is not cached.
        This key points to a very low privileged user that can only write or read from a dedicated store.
        The dedicated store is chrooted to and from the server.
        :return: a Paramiko private key to connect.
        """

        cert_store_engine = sa.create_engine(self.key_store)
        cert_table = sa.Table('sftp_cert',
                              sa.MetaData(),
                              schema='public',
                              autoload=True,
                              autoload_with=cert_store_engine)

        query = sa.select([cert_table.c['sftp_cert']])
        with cert_store_engine.connect() as conn:
            cert = conn.execute(query).fetchone()[0]

        with NamedTemporaryFile(dir='.', mode='w') as x:
            x.write(cert)  # .encode())
            x.seek(0)
            key = paramiko.Ed25519Key.from_private_key_file(x.name, password=None)
            return key

    def __connect(self):
        """
        Initialize a connection and connect to the sftp store.
        The user and key are the dedicated user and key generated above.
        DO NOT use your views user share!
        :return: a paramiko connection
        """
        t = paramiko.Transport(('hermes', 22222))
        t.connect(
            hostkey=None,
            pkey=self.key,
            username='predictions')
        return paramiko.SFTPClient.from_transport(t)

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
            #Already garbage collected and closed

    def file_exists(self, path):
        """
        For a given path check if file exists
        :param path: A complete, correct path to the resources
        :return: If the file exists on the server.
        """
        try:
            _ = self.connection.stat(path)
            return True
        except IOError:
            return False

    def mkdir(self, path):
        path = self.__path_fixer(path)
        try:
            self.connection.chdir(path)
        except IOError:
            self.connection.mkdir(path)
            self.connection.chdir(path)
        self.connection.chdir(None)



    def ls(self, path):
        folders = []
        files = []
        for entry in self.connection.listdir_attr(path):
            mode = entry.st_mode
            if S_ISDIR(mode):
                folders.append(entry.filename)
            elif S_ISREG(mode):
                files.append(entry.filename)
        return {'folders': folders, 'files': files}

    @staticmethod
    def __file_name_fixer(file_name, extension='csv'):
        extension = extension.strip(' .').lower()
        file_name = file_name.strip().lower()
        ext_len = -(len(extension)+1)
        file_name = file_name if file_name[ext_len:] == f'.{extension}' else file_name+'.'+extension
        return file_name

    @staticmethod
    def __path_fixer(path):
        store_path = path.rstrip('/ ')+'/'
        return store_path

    @staticmethod
    def __path_maker(file_name, path, extension):
        return ViewsSFTP.__path_fixer(path) + ViewsSFTP.__file_name_fixer(file_name,extension)


    def write_csv(self, file_name, path='./data/', overwrite=False):
        """
        Writes the current df to the remote store as a df in the given path.
        :param file_name: The name of the file to write.
        :param path: The remote path to save to.
        :param overwrite: If True, will overwrite any file it encounters
        :return: Nothing on success, exception on failure
        """

        store_path = self.__path_maker(file_name, path, 'csv')
        #print(store_path)

        if self.file_exists(store_path) and not overwrite:
            raise FileExistsError('File exists on server, set fail_on_exists to false.')

        with self.connection.open(store_path, "w") as f:
            f.write(self.df.to_csv(index=False))

    def write_parquet(self, file_name, path='./data/', overwrite=False):
        store_path = self.__path_maker(file_name, path, 'parquet')
        #print(store_path)

        if self.file_exists(store_path) and not overwrite:
            raise FileExistsError('File exists on server, set fail_on_exists to false.')

        with self.connection.open(store_path, "w") as f:
            # Pyarrow is needed for writing to remote file-likes
            f.write(self.df.to_parquet(index=False, engine='pyarrow'))

    def read_csv(self, file_name, path='./data/'):
        """
        Reads a csv from the store. It overwrites the df property in the object (whatever that is) and
        returns itself as a pandas dataframe
        :param file_name: File to read
        :param path: The remote path to read from.
        :return:
        """
        store_path = self.__path_maker(file_name, path, 'csv')
        #print(store_path)
        with self.connection.open(store_path, "r+b") as f:
            self.df = pd.read_csv(f)
            return self.df

    def read_parquet(self, file_name, path='./data/'):
        store_path = self.__path_maker(file_name, path, 'parquet')
        with self.connection.open(store_path, "r+b") as f:
            # Pyarrow is needed for reading from remote file-likes
            self.df = pd.read_parquet(f, engine='pyarrow')
            return self.df

    def delete_file(self, file_name, path='/data/'):
        raise NotImplementedError("""
        Low level deletion is not implemented and will never be for file safety reasons.
        Mark it for deletion using the register. Garbage collection will take care of it.
        """)