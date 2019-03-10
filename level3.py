import numpy as np
import pandas as pd
import os
import re
import pandas as pd
import datetime

#'总卖买笔比','总卖买笔比量','大卖买笔比','大卖买笔比量','中卖买笔比','中卖买笔比量','小卖买笔比','小卖买笔比量',]

def js_zb(df_sale,df_buy):
    r = []
    if len(df_buy) > 0:
        r.append( round(len(df_sale)/len(df_buy),2) )
        r.append( round(df_sale['SaleOrderVolume'].sum()/df_buy['BuyOrderVolume'].sum(),2) )
    else:
        r = ['','']

    return r

def join_zb(df, date):
    r = [date]

    # 超大资金的标准为100万，大资金标准为30万
    l = int(30E4/df.at[0,'Price'])
    s = int(3E4/df.at[0,'Price'])

    # 剔除开、收盘集合竞价数据
    df = df[~df.Time.isin(['09:25:00','15:00:00'])]

    df_sale = df[['SaleOrderVolume','SaleOrderID']]
    df_sale = df_sale.drop_duplicates()
    df_buy = df[['BuyOrderVolume','BuyOrderID']]
    df_buy = df_buy.drop_duplicates()
    r += js_zb(df_sale,df_buy)

    df_sale_l = df_sale[df_sale.SaleOrderVolume>l]
    df_sale_l = df_sale_l.drop_duplicates()
    df_buy_l = df_buy[df_buy.BuyOrderVolume>l]
    df_buy_l = df_buy_l.drop_duplicates()
    r += js_zb(df_sale_l,df_buy_l)

    df_sale_m = df_sale[(df_sale.SaleOrderVolume>s) & (df_sale.SaleOrderVolume<l)]
    df_sale_m = df_sale_m.drop_duplicates()
    df_buy_m = df_buy[(df_buy.BuyOrderVolume>s) & (df_buy.BuyOrderVolume<l)]
    df_buy_m = df_buy_m.drop_duplicates()
    r += js_zb(df_sale_m,df_buy_m)


    df_sale_s = df_sale[df_sale.SaleOrderVolume<=s]
    df_sale_s = df_sale_s.drop_duplicates()
    df_buy_s = df_buy[df_buy.BuyOrderVolume<=s]
    df_buy_s = df_buy_s.drop_duplicates()
    r += js_zb(df_sale_s,df_buy_s)

    return r

if __name__ == '__main__':
    path = r'C:\Users\czh\Documents\critical\data\zhubi\201901'
    codes = ['300408','002273','002570','300433','300461','600776','300602']
    codes = ['300693']

    dates = ['2019-01-02','2019-01-03','2019-01-04','2019-01-07','2019-01-09',
             '2019-01-10','2019-01-11','2019-01-14','2019-01-15','2019-01-16',
             '2019-01-17','2019-01-18','2019-01-21','2019-01-22','2019-01-23',
             '2019-01-24','2019-01-25','2019-01-28','2019-01-29','2019-01-30','2019-01-31',]

    #dates = ['20190128']

    # '超大单净量','大单净量',
    colname = ['日期',
               '卖单总笔','卖单总量','买单总笔','买单总量',
               '大卖单笔','大卖单量','大买单笔','大买单量',
               '中卖单笔','中卖单量','中买单笔','中买单量',
               '小卖单笔','小卖单量','小买单笔','小买单量',
              ]

    colname = ['日期',
               '总卖买笔比','总卖买量比','大卖买笔比','大卖买量比',
               '中卖买笔比','中卖买量比','小卖买笔比','小卖买量比',]
    for code in codes:
        r = []
        for date in dates:
            fn = path + '\\' + date + '\\' + code + '.csv'
            #print(date)
            if  os.path.exists(fn):
                df = pd.read_csv(fn)
                item = join_zb(df, date)
                r.append(item)

    df1 = pd.DataFrame(r,columns=colname)
    df1 = df1[['日期','大卖买笔比','大卖买量比','中卖买笔比','中卖买量比','小卖买笔比','小卖买量比']]
    df1.to_excel(code + '\\'+ code + '_' + date[:7] + '_level3.xlsx', index=False, encoding='gbk')
