import pandas
import pandas as pd
import numpy as np
from ViewsSFTP import ViewsSFTP

reader = ViewsSFTP()
df = reader.read_csv(file_name='test.csv')

assert df.loc[0].id == 12
assert (np.isnan(df.loc[2]['data']))

df2 = pd.DataFrame({'cat': [10,29],'ef': [9,5.5]})
writer = ViewsSFTP(df2)
writer.write_csv(file_name='test2.csv', overwrite=True)
writer.write_parquet(file_name='test2.parquet', overwrite=True)

df = reader.read_csv(file_name='test2.csv')
assert df.loc[1]['cat'] == 29

reader.mkdir('/data/test_data2')