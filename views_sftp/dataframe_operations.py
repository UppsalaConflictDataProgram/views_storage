from functools import partial
import enum
import pandas as pd
from . import operations


class Backend(enum.Enum):
    CSV = 1
    PARQUET = 2


class DataframeOperations(operations.Operations):
    """
    SFTPDataframeOperations
    =======================

    Operations for reading and writing data from a remote server over SFTP.

    """

    BACKENDS = {
        Backend.CSV: (pd.read_csv, lambda df: df.to_csv(index=False)),
        Backend.PARQUET: (
            partial(pd.read_parquet, engine="pyarrow"),
            lambda df: df.to_parquet(index=False, engine="pyarrow"),
        ),
    }

    def read_csv(self, file_name: str, path: str = "./data/") -> pd.DataFrame:
        """
        read_csv
        ========

        parameters:
            file_name (str): Name of the file on the server
            path (str): Path to the folder containing the file to read

        returns:
            pandas.DataFrame

        Reads a dataframe from the store.
        """

        return self._read(Backend.CSV, file_name, path)

    def write_csv(
        self,
        dataframe: pd.DataFrame,
        file_name: str,
        path: str = "./data/",
        overwrite: bool = False,
    ) -> None:
        """
        write_csv
        =========

        parameters:
            dataframe (pd.DataFrame): The dataframe to write
            file_name (str): Name of the file on the server
            path (str): Path to the folder containing the file to read
            overwrite (bool): Whether to overwrite the file if it already exists.

        Writes a dataframe to the remote store in the given path.
        """

        return self._write(Backend.CSV, dataframe, file_name, path, overwrite)

    def read_parquet(self, file_name, path="./data/"):
        """
        read_parquet
        ============

        parameters:
            file_name (str): Name of the file on the server
            path (str): Path to the folder containing the file to read

        returns:
            pandas.DataFrame

        Reads a dataframe from the store.
        """

        return self._read(Backend.PARQUET, file_name, path)

    def write_parquet(
        self, dataframe: pd.DataFrame, file_name, path="./data/", overwrite=False
    ):
        """
        write_parquet
        =============

        parameters:
            dataframe (pd.DataFrame): The dataframe to write
            file_name (str): Name of the file on the server
            path (str): Path to the folder containing the file to read
            overwrite (bool): Whether to overwrite the file if it already exists.

        Writes a dataframe to the store in the given path.
        """

        return self._write(Backend.PARQUET, dataframe, file_name, path, overwrite)

    def _serialized(self, backend: Backend):
        return self.BACKENDS[backend][0]

    def _write(
        self,
        backend: Backend,
        dataframe: pd.DataFrame,
        file_name: str,
        path: str,
        overwrite: bool,
    ) -> None:
        """
        _write
        ======

        parameters:
            dataframe (pd.DataFrame): The dataframe to write
            file_name (str): Name of the file on the server
            path (str): Path to the folder containing the file to read
            overwrite (bool): Whether to overwrite the file if it already exists.

        Writes a dataframe to the store in the given path.
        """
        serialize = self._serializer(backend)
        store_path = self._path_maker(file_name, path, "csv")

        if self.file_exists(store_path) and not overwrite:
            raise FileExistsError("File exists on server, set fail_on_exists to false.")

        with self.connection.open(store_path, "w") as f:
            f.write(serialize(dataframe))

    def _read(self, backend: str, file_name: str, path: str = "./data/"):
        """
        _read
        ============

        parameters:
            file_name (str): Name of the file on the server
            path (str): Path to the folder containing the file to read

        returns:
            pandas.DataFrame

        Reads a dataframe from the store.
        """
        read = self._reader(backend)
        store_path = self._path_maker(file_name, path, backend)
        with self.connection.open(store_path, "r+b") as f:
            return read(f)

    def _reader(self, backend: Backend):
        try:
            return self.BACKENDS[backend][0]
        except KeyError:
            raise NotImplementedError(f"Backend {backend} is not supported")

    def _serializer(self, backend: Backend):
        try:
            return self.BACKENDS[backend][1]
        except KeyError:
            raise NotImplementedError(f"Backend {backend} is not supported")
