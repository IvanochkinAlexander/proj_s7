import re
import pandas as pd
import numpy as np
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import re
import datetime
from time import gmtime, strftime
import datetime

### loading phantomhs browser with proxy server
# proxy_server = '--proxy=us-wa.proxymesh.com:31280'
browser = webdriver.PhantomJS(executable_path='/../usr/local/bin/phantomjs')


def split_on_simbol (df, col, smb, name):

    """Splits columns into three parts"""

    new_df = pd.DataFrame(df[col].str.split(smb, 0).tolist())
    new_df_2 = pd.DataFrame(df[col].str.split(smb, 1).tolist())
    df = pd.concat([df.reset_index(drop=True), new_df, new_df_2],axis=1)
    df = df.iloc[:,1:4]
    df = df.rename(columns={0:name+'_0', 1:name+'_1', 2:name+'_2'})
    return df

def split_on_simbol_2_cols (df, col, smb, name):

    """Splits columns into two parts"""

    new_df = pd.DataFrame(df[col].str.split(smb, 0).tolist())
    df = pd.concat([df.reset_index(drop=True), new_df],axis=1)
    df = df.rename(columns={0:name+'_0', 1:name+'_1'})
    return df

def take_digits(col):

    """Finds digits in column"""

    try:
        return map(int,(re.findall('\d', col)))
    except:
        return [3,0]

def post_processing (result_df):

    """Makes final result"""

    splited_df = split_on_simbol(result_df, 0, '_', 'result')
    splited_df['result_2'] = splited_df['result_2'].apply(take_digits)
    splited_df['result_2'] = splited_df['result_2'].apply(lambda x:str(x[0]) + str(x[1]))
    cities_df['id'] = cities_df['id'].astype(int)
    splited_df['result_1']=splited_df['result_1'].astype(int)
    final_df = pd.merge(splited_df, cities_df, left_on='result_1', right_on='id', how='left')
    del final_df['result_1']
    del final_df['id']
    final_df['result_2'] = final_df['result_2'].astype(int)*0.01
    final_df = final_df.rename(columns={'result_2':'percent'})
    final_df = final_df.rename(columns={'result_0':'date'})
    final_df['date'] = final_df['date'].apply(lambda x:str(x)[:10])
    return final_df

def check_and_fill_nulls (final_df, dates, cities_df, value_to_fill):

    """
    If availability is lower than 30 percent script return empty value.
    Function finds it and replaces.
    """

    pivot_df = final_df.reset_index().pivot_table('percent', 'city', 'date', aggfunc='sum').reset_index().fillna(value_to_fill)
    all_ids = []
    for i in cities_df['city'].unique():
        for l in dates:
            all_ids.append(str(l)[:10] + '_' + unicode(i))

    ids_df = pd.DataFrame(all_ids).sort_values(0)
    ids_df = ids_df.rename(columns={0:'uniq_id'})
    final_df['sc'] = final_df['date'].astype(str) + '_' + final_df['city']
    print (ids_df.shape[0])
    final_checked = pd.merge(ids_df, final_df, left_on='uniq_id', right_on='sc', how='left')
    final_checked = final_checked.fillna(value_to_fill)
    final_checked = split_on_simbol_2_cols(final_checked, 'uniq_id', '_', 'final')[['final_0', 'final_1', 'percent']]
    final_checked.columns = ('date', 'city', 'percent')
    print (final_checked.shape[0])
    return (final_checked)

start_months = ['2017-09-01', '2017-10-01', '2017-11-01', '2017-12-01',
          '2018-01-01']

end_months = ['2017-09-30', '2017-10-31', '2017-11-30', '2017-12-31',
          '2018-01-31']

def parse_agoda (dates, cities_ids):

    """Parses availability in certain city on certain date"""

    all_percent = []
    for date in dates:
        print (date)
        for i in cities_ids:
            print (i)
            browser.get('https://www.agoda.com/ru-ru/pages/agoda/default/DestinationSearchResult.aspx?city={}'.format(i) + '&checkIn={}&los=1&rooms=1&adults=2&children=0&cid=-1'.format(str(date)[:10]))
            page = browser.page_source
            soup = BeautifulSoup(page, "lxml")
            time.sleep(3)
            for text_value in soup.find_all('div', class_='availability-score-gauge'):
                all_text = ''
                all_text = unicode(date)
                all_text+='_'
                all_text+=unicode(i)
                all_text+='_'
                all_text+=unicode(text_value.text)
#                 print text_value.text
                all_percent.append(all_text)
    return pd.DataFrame(all_percent)

cities_df = pd.read_excel('../input/cities2.xlsx')
cities_df = cities_df[['id', 'city']][:10]

def final_parse (start_months, end_months):

    for start_month,end_month in zip(start_months, end_months):
        today = datetime.date.today()
        today_time = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
        dates_genetator = pd.date_range(start_month, end_month, freq='D')
        dates=[]
        for date in pd.to_datetime(dates_genetator):
            if str(date) > str(today+datetime.timedelta(days=1)):
                dates.append(date)
        # dates=dates
        result_df = pd.DataFrame()
        result_df = parse_agoda(dates, cities_df['id'])
        if len(result_df) >0:
            final_df = post_processing(result_df)
            print (final_df)
            final_checked= check_and_fill_nulls(final_df, dates, cities_df, 0.3)
            final_checked.to_excel('../output/s7/{}_Parsing_agoda'.format(today_time) + '_{}.xlsx'.format(end_month), index=False)
    return final_checked

final_checked = final_parse (start_months, end_months)
