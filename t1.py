import numpy as np
import pandas as pd
import os
import re
import pandas as pd
import datetime

def js_zb(df):
    r = []

    cda = int(100E4/df.at[0,'Price'])
    da = int(30E4/df.at[0,'Price'])
    #print(cda,da)

    df_cda = df[df.BuyOrderVolume>cda]
    df_cda = df_cda[['BuyOrderVolume','BuyOrderID']]
    #print(len(df_cda))
    df_cda = df_cda.drop_duplicates()
    #print(len(df_cda))
    r.append(df_cda['BuyOrderVolume'].sum())

    df_cda = df[df.SaleOrderVolume>cda]
    df_cda = df_cda[['SaleOrderVolume','SaleOrderID']]
    df_cda = df_cda.drop_duplicates()
    r.append(df_cda['SaleOrderVolume'].sum())

    r.append(r[0]-r[1])

    df_da = df[(df.BuyOrderVolume>da) & (df.BuyOrderVolume<cda) ]
    df_da = df_da[['BuyOrderVolume','BuyOrderID']]
    #print(len(df_da))
    df_da = df_da.drop_duplicates()
    #print(len(df_da))
    r.append(df_da['BuyOrderVolume'].sum())

    df_da = df[(df.SaleOrderVolume>da) & (df.SaleOrderVolume<cda) ]
    df_da = df_da[['SaleOrderVolume','SaleOrderID']]
    #print(len(df_da))
    df_da = df_da.drop_duplicates()
    #print(len(df_da))
    r.append(df_da['SaleOrderVolume'].sum())

    r.append(r[3]-r[4])

    return r


if __name__ == '__main__':
    path = r'C:\Users\czh\Documents\critical\data\zhubi\care'
    codes = ['300408','002273','002570','300433','300461','600776']
    dates = ['20190102','20190103','20190104','20190107','20190109',
             '20190110','20190111','20190114','20190115','20190116',
             '20190117','20190118','20190121','20190122','20190123',
             '20190124','20190125','20190128','20190129','20190130','20190131',]

    colname = ['日期','买超大单','卖超大单','超大净量','买大单','卖大单','大净量']
    for code in codes:
        r = []
        for date in dates:
            fn = path + '\\' + code + '_' + date + '.csv'
            #print(fn)
            if  os.path.exists(fn):
                df = pd.read_csv(fn)
                item = js_zb(df)
                item.insert(0,date)
                r.append(item)

        df1 = pd.DataFrame(r,columns=colname)
        df1.to_csv(code + '_' + date + '.csv', index=False, encoding='gbk')
