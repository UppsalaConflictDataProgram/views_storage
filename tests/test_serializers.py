
import unittest
import string
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from views_storage.serializers import csv, parquet, pickle, serializer

class TestSerializers(unittest.TestCase):
    def assert_serializer_identity(self, dataframe, ser: serializer.Serializer):
        assert_frame_equal(dataframe, ser.deserialize(ser.serialize(dataframe)))

    def test_serializers_identity(self):
        df = pd.DataFrame(np.random.rand(10,10), columns = list(string.ascii_lowercase[:10]))
        for ser in csv.Csv, parquet.Parquet, pickle.Pickle:
            self.assert_serializer_identity(df, ser())
