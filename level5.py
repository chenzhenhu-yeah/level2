import numpy as np
import pandas as pd
import os
import re
import pandas as pd
import datetime

# '主买笔数','主买成交量','大单主买量占比','小单主买量占比',
def js_zb(df, flag_v, l, s):
    r = []

    if len(df) > 0:
        r.append(len(df))
        r.append(df['Volume'].sum())
        df_l = df[df[flag_v]>l]
        r.append(round(df_l['Volume'].sum()/df['Volume'].sum(),2))
        df_s = df[df[flag_v]<=s]
        r.append(round(df_s['Volume'].sum()/df['Volume'].sum(),2))
    else:
        r = ['','','','']

    return r

def join_zb(df, date):
    r = [date]

    # 超大资金的标准为100万，大资金标准为30万
    l = int(30E4/df.at[0,'Price'])
    s = int(3E4/df.at[0,'Price'])

    # 剔除开、收盘集合竞价数据
    df = df[~df.Time.isin(['09:25:00','15:00:00'])]

    df_buy = df[df.Type=='B']
    r += js_zb(df_buy, 'BuyOrderVolume', l, s)

    df_sell = df[df.Type=='S']
    r += js_zb(df_sell, 'SaleOrderVolume', l, s)

    return r

if __name__ == '__main__':
    path =  r'C:\Users\czh\Documents\critical\data\zhubi\201901'
    codes = ['300408','002273','002570','300433','300461','600776','300461','300602','002792']
    codes = ['300693']

    dates = ['2019-01-02','2019-01-03','2019-01-04','2019-01-07','2019-01-09',
             '2019-01-10','2019-01-11','2019-01-14','2019-01-15','2019-01-16',
             '2019-01-17','2019-01-18','2019-01-21','2019-01-22','2019-01-23',
             '2019-01-24','2019-01-25','2019-01-28','2019-01-29','2019-01-30','2019-01-31',]
    #dates = ['2019-01-29','2019-01-30','2019-01-31',]

    colname = ['日期','主买笔数','主买成交量','大单主买量占比','小单主买量占比',
               '主卖笔数','主卖成交量','大单主卖量占比','小单主卖量占比']
    for code in codes:
        r = []
        for date in dates:
            fn = path + '\\' + date + '\\' + code + '.csv'
            #print(fn)
            if  os.path.exists(fn):
                df = pd.read_csv(fn)
                item = join_zb(df, date)
                r.append(item)

        df1 = pd.DataFrame(r,columns=colname)
        df1['卖买量比'] = round(df1['主卖成交量']/df1['主买成交量'],2)
        df1 = df1[['日期','小单主卖量占比','大单主卖量占比','主卖笔数','卖买量比','主买笔数', '大单主买量占比','小单主买量占比']]
        df1.to_excel(code + '\\'+ code + '_' + date[:7] + '_level5.xlsx', index=False, encoding='gbk')
