import numpy as np
import pandas as pd
import os
import re
import pandas as pd
import datetime


#['日期','大买单委托量'，'大买单成交量','大买单B占比','大卖单成交量','大卖单S占比',]
def js_zb(df,flag):
    r = []
    if len(df) > 0:
        if flag == 'buy':
            df_buy = df[['BuyOrderVolume','BuyOrderID']]
            df_buy = df_buy.drop_duplicates()
            v = df_buy['BuyOrderVolume'].sum()
            r.append(v)

            df_buy = df[['BuyOrderVolume','BuyOrderID','Type','Volume']]
            r.append( round(df_buy['Volume'].sum()/v,2) )

            df_buy_B = df[df_buy.Type == 'B']
            r.append(round(df_buy_B['Volume'].sum()/df_buy['Volume'].sum(),2))


        if flag == 'sell':
            df_sell = df[['SaleOrderVolume','SaleOrderID']]
            df_sell = df_sell.drop_duplicates()
            v = df_sell['SaleOrderVolume'].sum()
            r.append(v)

            df_sell = df[['SaleOrderVolume','SaleOrderID','Type','Volume']]
            r.append( round(df_sell['Volume'].sum()/v,2) )

            df_sell_S = df[df_sell.Type == 'S']
            r.append(round(df_sell_S['Volume'].sum()/df_sell['Volume'].sum(),2))
    else:
        r = ['','','']

    return r

def join_zb(df, date):
    r = [date]

    # 超大资金的标准为100万，大资金标准为30万
    cda = int(30E4/df.at[0,'Price'])
    da = int(3E4/df.at[0,'Price'])

    # 剔除开、收盘集合竞价数据
    df = df[~df.Time.isin(['09:25:00','15:00:00'])]

    df_cda_buy = df[df.BuyOrderVolume>cda]
    r += js_zb(df_cda_buy, 'buy')
    #df_cda_buy.to_excel(code + '\\'+ code + '_' + date + '_cda_buy.xlsx', index=False, encoding='gbk')

    df_cda_sell = df[df.SaleOrderVolume>cda]
    r += js_zb(df_cda_sell, 'sell')
    #df_cda_sell.to_excel(code + '\\'+ code + '_' + date + '_cda_sell.xlsx', index=False, encoding='gbk')

    return r

if __name__ == '__main__':
    path =  r'C:\Users\czh\Documents\critical\data\zhubi\201901'
    codes = ['300408','002273','002570','300433','300461','600776','300602']
    codes = ['300693']

    dates = ['20190129','20190130','20190131',]
    dates = ['2019-01-02','2019-01-03','2019-01-04','2019-01-07','2019-01-09',
             '2019-01-10','2019-01-11','2019-01-14','2019-01-15','2019-01-16',
             '2019-01-17','2019-01-18','2019-01-21','2019-01-22','2019-01-23',
             '2019-01-24','2019-01-25','2019-01-28','2019-01-29','2019-01-30','2019-01-31',]

    colname = ['日期','大买单委托量','大买单成交率','大买单B占比','大卖单委托量','大卖单成交率','大卖单S占比']
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
        df1 = df1[['日期','大卖单委托量','大卖单成交率','大卖单S占比','大买单委托量','大买单成交率','大买单B占比']]
        df1.to_excel(code + '\\'+ code + '_' + date[:7] + '_level4.xlsx', index=False, encoding='gbk')
