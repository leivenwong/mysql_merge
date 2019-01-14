import pandas as pd
from sqlalchemy import create_engine
import pymysql

#set utc time critical value
utc_critical_value = '13191664500000000'

#read data setting
data_read_user_1 = 'wang_2'
data_read_psd_1 = 'wang_2'
data_read_db_1 = 'wang2_merged'

#write data setting
data_write_user = 'ctp_user'
data_write_psd = 'ctp_password'
data_write_db = 'ctp_merged_mq'

#merge table list
table_level_list = ['5s', '1m', '5m', '15m', '1h', '1d']
table_name_list = ['if', 'ih', 'ic', 'ru', 'rb']


#data merge cycle
for name in table_name_list:
    for level in table_level_list:
        table_name = name + '_' + level
        print("Enter Mysql wang1 " + table_name)
        engine = create_engine('mysql+pymysql://'
                               + data_read_user_1 + ':'
                               + data_read_psd_1 +
                               '@127.0.0.1/' +
                               data_read_db_1 +
                               '?charset=utf8')
        df = pd.read_sql(sql="select * from " + table_name + " where utc > "
                         + utc_critical_value, con=engine)
        print("Out Mysql wang1 " + table_name)
        data_append = df
        data_append = pd.DataFrame(data_append)

        engine = create_engine('mysql+pymysql://'
                               + data_write_user + ':'
                               + data_write_psd +
                               '@127.0.0.1/' +
                               data_write_db +
                               '?charset=utf8')
        data_append.to_sql(table_name, engine, if_exists='append', index=False)

        print('write completed')
