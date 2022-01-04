from views_storage import SftpDataStorage

storage = SftpDataStorage("janus",dbname = "pred3_certs")
d = storage.read("data/test/test2/test2.parquet")
storage.write("data/test/test2/test2_2.parquet", d, overwrite = True)
print(storage.list("data/test/test2"))

"""
reader = ViewsSFTP()

reader.mkdir('/data/test/test2')

df = reader.read_csv(file_name='read_test.csv', path='./data/test/')

assert df.loc[0].id == 12
assert (np.isnan(df.loc[2]['data']))

df2 = pd.DataFrame({'cat': [10,29],'ef': [9,5.5]})
writer = ViewsSFTP(df2)
writer.write_csv(file_name='test2.csv', path='./data/test/test2', overwrite=True)
writer.write_parquet(file_name='test2.parquet', path='/data/test/test2/', overwrite=True)

df = reader.read_csv(file_name='test2.csv', path='/data/test/test2/')
assert df.loc[1]['cat'] == 29

df = reader.read_parquet(file_name='test2.parquet', path='./data/test/test2/')
assert df.loc[1]['cat'] == 29

test_list = reader.ls(path='./data/test/')
assert 'test2' in test_list['folders']
assert 'read_test.csv' in test_list['files']
"""
