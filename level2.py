import numpy as np
import pandas as pd
import os
import re
import pandas as pd
import datetime

# '超大买单笔数','超大买单金额','超大买单B笔数','超大买单B金额','超大买单S笔数','超大买单S金额'
def js_zb(df,flag):
    r = []
    if flag == 'buy':
        df_buy = df[['BuyOrderVolume','BuyOrderID','Type','SaleOrderVolume']]
        df_buy_B = df[df_buy.Type == 'B']
        r.append(len(df_buy_B))
        r.append(df_buy_B['SaleOrderVolume'].sum())
        df_buy_S = df[df_buy.Type == 'S']
        r.append(len(df_buy_S))
        r.append(df_buy_S['SaleOrderVolume'].sum())

        df_buy = df[['BuyOrderVolume','BuyOrderID']]
        df_buy = df_buy.drop_duplicates()
        r.append(len(df_buy))
        r.append(df_buy['BuyOrderVolume'].sum())
    if flag == 'sell':
        df_sell = df[['SaleOrderVolume','SaleOrderID']]
        df_sell = df_sell.drop_duplicates()
        r.append(len(df_sell))
        r.append(df_sell['SaleOrderVolume'].sum())

        df_sell = df[['SaleOrderVolume','SaleOrderID','Type','BuyOrderVolume']]
        df_sell_B = df[df_sell.Type == 'B']
        r.append(len(df_sell_B))
        r.append(df_sell_B['BuyOrderVolume'].sum())
        df_sell_S = df[df_sell.Type == 'S']
        r.append(len(df_sell_S))
        r.append(df_sell_S['BuyOrderVolume'].sum())

    return r

def join_zb(df, date):
    r = [date]

    # 超大资金的标准为100万，大资金标准为30万
    cda = int(100E4/df.at[0,'Price'])
    da = int(30E4/df.at[0,'Price'])

    # 剔除开、收盘集合竞价数据
    df = df[~df.Time.isin(['09:25:00','15:00:00'])]

    df_cda_buy = df[df.BuyOrderVolume>cda]
    r += js_zb(df_cda_buy, 'buy')
    df_cda_buy.to_excel(code + '_' + date + '_cda_buy.xlsx', index=False, encoding='gbk')

    df_cda_sell = df[df.SaleOrderVolume>cda]
    r += js_zb(df_cda_sell, 'sell')
    df_cda_sell.to_excel(code + '_' + date + '_cda_sell.xlsx', index=False, encoding='gbk')

    #r.append(r[2]-r[4])

    df_da_buy = df[(df.BuyOrderVolume>da) & (df.BuyOrderVolume<cda) ]
    r += js_zb(df_da_buy, 'buy')
    df_da_buy.to_excel(code + '_' + date + '_da_buy.xlsx', index=False, encoding='gbk')

    df_da_sell = df[(df.SaleOrderVolume>da) & (df.SaleOrderVolume<cda) ]
    r += js_zb(df_da_sell, 'sell')
    df_da_sell.to_excel(code + '_' + date + '_da_sell.xlsx', index=False, encoding='gbk')

    #r.append(r[7]-r[9])

    return r

if __name__ == '__main__':
    path =  r'C:\Users\czh\Documents\critical\data\zhubi\care'
    codes = ['300408','002273','002570','300433','300461','600776']
    dates = ['20190102','20190103','20190104','20190107','20190109',
             '20190110','20190111','20190114','20190115','20190116',
             '20190117','20190118','20190121','20190122','20190123',
             '20190124','20190125','20190128','20190129','20190130','20190131',]

    # '超大单净量','大单净量',
    colname = ['日期',
               '超大买单B笔数','超大买单B金额','超大买单S笔数','超大买单S金额','超大买单笔数','超大买单金额',
               '超大卖单笔数','超大卖单金额','超大卖单B笔数','超大卖单B金额','超大卖单S笔数','超大卖单S金额',
               '大买单B笔数','大买单B金额','大买单S笔数','大买单S金额','大买单笔数','大买单金额',
               '大卖单笔数','大卖单金额','大卖单B笔数','大卖单B金额','大卖单S笔数','大卖单S金额']
    for code in codes:
        r = []
        for date in dates:
            fn = path + '\\' + code + '_' + date + '.csv'
            #print(fn)
            if  os.path.exists(fn):
                df = pd.read_csv(fn)
                item = join_zb(df, date)
                r.append(item)

        df1 = pd.DataFrame(r,columns=colname)
        df1.to_excel(code + '_' + date[:6] + '.xlsx', index=False, encoding='gbk')
