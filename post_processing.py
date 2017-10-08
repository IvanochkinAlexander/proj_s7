import pandas as pd
import os
from collections import deque
from time import gmtime, strftime
from sqlalchemy import create_engine
import sqlite3

def read_files (path):

    """Reads all files in path, drops old data"""

    all_tables = pd.DataFrame()
    print ('concating files:')
    for i in os.listdir('{}'.format(path)):
        # print (i)
        try:
            if i.split('agoda_')[1].split('.')[1] == 'xlsx':
                print (i)
                temp_table = pd.read_excel(path+'/'+i)
                # path+'{}'.format(i)
                temp_table['end_of_month'] = i.split('agoda_')[1].split('.')[0]
                temp_table['date_of_parsing'] =  i.split('_Parsing_')[0]
                all_tables = pd.concat([all_tables, temp_table], axis=0)
                print 'success'
        except:
            pass
    # print all_tables
    all_tables = all_tables.sort_values(['date_of_parsing', 'end_of_month'], ascending=False)
    all_tables['uid'] = all_tables['date'] + '_' + all_tables['city'] + '_' + all_tables['end_of_month']
    all_tables = all_tables.drop_duplicates(subset='uid', keep='first').reset_index(drop=True)
    all_tables = all_tables.sort_values(['city', 'date'], ascending=True).reset_index(drop=True)

    return all_tables

def window(seq, n=5):

    """Sliding windows function"""

    it = iter(seq)
    win = deque((next(it, None) for _ in xrange(n)), maxlen=n)
    yield win
    append = win.append
    for e in it:
        append(e)
        yield win

def count_delta (ts_df):

    """Counts delta between current value and 2 neighbors - in the front of it and in the back"""

    today_time = strftime("%Y-%m-%d_%H-%M-%S", gmtime())

    all_df = pd.DataFrame()
    for city in ts_df['city'].unique():
        print city
        ts_filtered = ts_df[ts_df['city']==city].reset_index(drop=True)
        deltas = []
        for i in window(ts_filtered['date']):
            temp_df = ts_filtered[ts_filtered['date'].isin(i)].reset_index(drop=True)
            target_value = temp_df.iloc[2]
            temp_df = temp_df.reindex(temp_df.index.drop(2))
            delta = (target_value['percent'] -temp_df['percent'].median())/temp_df['percent'].median()
            deltas.append(delta)
        print len(deltas)
        print (ts_filtered.shape[0])
        ts_filtered['delta'] = pd.DataFrame([0,0] + deltas + [0,0])[0].apply(lambda x : round(x,2)).values
        all_df = pd.concat([all_df, ts_filtered], axis=0)

    return all_df

def search_extreme_values (all_df):

    """Sets marker of extreme values and saves file"""

    today_time = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
    all_df['extreme_value'] = ''
    all_df.loc[all_df['delta']>=0.5, 'extreme_value'] = 1
    all_df.loc[all_df['delta']<0.5, 'extreme_value'] = 0
    all_df = all_df.reset_index(drop=True)
    all_df.to_excel('../output/{}_search_extreme_{}_{}.xlsx'.format(today_time,
                                                             all_df['end_of_month'].min(),
                                                             all_df['end_of_month'].max()), index=False)
    print ('finished, file saved')

    return all_df

def make_dashboard_file (all_df):

    """Makes sqlite file for superset dashboard"""

    city_dict = pd.read_excel('../input/city_dict_final.xlsx')
    all_df = pd.merge(all_df, city_dict, on='city', how='left')
    all_df['date'] = pd.to_datetime(all_df['date'])
    all_df['month'] = all_df['date'].dt.month
    today_time = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
    disk_engine = create_engine('sqlite:///../output/superset_file_{}.db'.format(today_time), encoding='utf-8')
    all_df.to_sql('superset_file'.format(today_time), disk_engine)
    print ('sqlite file saved')

    return all_df

ts_df = read_files ('../output/s7')
all_df = count_delta(ts_df)
all_df = search_extreme_values(all_df)
all_df.to_excel('../output/temp_excel.xlsx', index=False)

# all_df = make_dashboard_file(all_df)

### check

# cnx = sqlite3.connect('../superset_file_2017-09-08_16-47-40.db')
# df = pd.read_sql_query("SELECT * FROM superset_file", cnx)
# cnx = sqlite3.connect('../.superset/superset_datafile_6.db')
# df = pd.read_sql_query("SELECT * FROM slices", cnx)
