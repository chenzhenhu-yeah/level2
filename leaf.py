# 一叶知秋

import numpy as np
import pandas as pd
import os
import re
import pandas as pd
import datetime
import subprocess




def js_zb3(df_sale,df_buy):
    r = []
    if len(df_buy) > 0:
        r.append( round(len(df_sale)/len(df_buy),2) )
        r.append( round(df_sale['SaleOrderVolume'].sum()/df_buy['BuyOrderVolume'].sum(),2) )
    else:
        r = ['','']

    return r

# ['总卖买笔比','总卖买笔比量','大卖买笔比','大卖买笔比量','中卖买笔比','中卖买笔比量','小卖买笔比','小卖买笔比量']
def join_zb3(df, l, s):
    r = []

    df_sale = df[['SaleOrderVolume','SaleOrderID']]
    df_sale = df_sale.drop_duplicates()
    df_buy = df[['BuyOrderVolume','BuyOrderID']]
    df_buy = df_buy.drop_duplicates()
    r += js_zb3(df_sale,df_buy)

    df_sale_l = df_sale[df_sale.SaleOrderVolume>l]
    df_sale_l = df_sale_l.drop_duplicates()
    df_buy_l = df_buy[df_buy.BuyOrderVolume>l]
    df_buy_l = df_buy_l.drop_duplicates()
    r += js_zb3(df_sale_l,df_buy_l)

    df_sale_m = df_sale[(df_sale.SaleOrderVolume>s) & (df_sale.SaleOrderVolume<l)]
    df_sale_m = df_sale_m.drop_duplicates()
    df_buy_m = df_buy[(df_buy.BuyOrderVolume>s) & (df_buy.BuyOrderVolume<l)]
    df_buy_m = df_buy_m.drop_duplicates()
    r += js_zb3(df_sale_m,df_buy_m)


    df_sale_s = df_sale[df_sale.SaleOrderVolume<=s]
    df_sale_s = df_sale_s.drop_duplicates()
    df_buy_s = df_buy[df_buy.BuyOrderVolume<=s]
    df_buy_s = df_buy_s.drop_duplicates()
    r += js_zb3(df_sale_s,df_buy_s)

    return r

# ['委卖笔均', '委买笔均', '委托笔比'，'大单净量','小单净量','委卖大单占比','委买大单占比']
def join_zb4(df, l, s):
    r = []

    df_sale = df[['SaleOrderVolume','SaleOrderID']]
    df_sale = df_sale.drop_duplicates()
    r += [int(df_sale['SaleOrderVolume'].mean())]

    df_buy = df[['BuyOrderVolume','BuyOrderID']]
    df_buy = df_buy.drop_duplicates()
    r += [int(df_buy['BuyOrderVolume'].mean())]

    s_n = df['SaleOrderID'].nunique()
    b_n = df['BuyOrderID'].nunique()

    if s_n > b_n:
        r += [round(-s_n/b_n, 2)]
    else:
        r += [round(b_n/s_n, 2)]

    df_buy_l = df[df.BuyOrderVolume>l]
    df_sell_l = df[df.SaleOrderVolume>l]
    r += [int((df_buy_l['Volume'].sum() - df_sell_l['Volume'].sum())/10000)]

    df_buy_s = df[df.BuyOrderVolume<s]
    df_sell_s = df[df.SaleOrderVolume<s]
    r += [int((df_buy_s['Volume'].sum() - df_sell_s['Volume'].sum())/10000)]

    r += [round((df_sell_l['Volume'].sum()/df['Volume'].sum()), 2)]
    r += [round((df_buy_l['Volume'].sum()/df['Volume'].sum()), 2)]

    return r

# '主动成交量','大单占比'
def js_zb5(df, flag_v, l, s):
    r = []

    if len(df) > 0:
        r.append(df['Volume'].sum())
        df_l = df[df[flag_v]>l]
        r.append(round(df_l['Volume'].sum()/df['Volume'].sum(),2))
    else:
        r = [0,0]

    return r

# ['主动单量比','主卖大单量占比','主买大单量占比']
def join_zb5(df, l, s):
    r = []

    df_sell = df[df.Type=='S']
    r += js_zb5(df_sell, 'SaleOrderVolume', l, s)

    df_buy = df[df.Type=='B']
    r += js_zb5(df_buy, 'BuyOrderVolume', l, s)

    if r[0] > r[2]:
        r[0] = round(-r[0]/r[2], 2)
    else:
        r[0] = round(r[2]/r[0], 2)
    del r[2]
    return r

def js(codes, colname, path, dates):
    for date in dates:
        fc = ''
        for code in codes:
            fn = path + '\\' + date + '\\' + code + '.csv'
            if not os.path.exists(fn):
                fc += code + '.csv '
        if fc != '':
            print(date)
            ins = '7z x -r -y -o' + path + ' ' + path + '\\' + date.replace('-','') + '.7z '+ fc
            #print(ins)
            #os.system(ins)
            #subprocess.call(ins)
            subprocess.check_output(ins) # 无屏幕输出，能加快速度
    for code in codes:
        print(code)
        r = []
        for date in dates:
            fn = path + '\\' + date + '\\' + code + '.csv'
            line = [date]
            if  os.path.exists(fn):
                df = pd.read_csv(fn)

                # 超大资金的标准为100万，大资金标准为30万
                l = int(30E4/df.at[0,'Price'])
                s = int(3E4/df.at[0,'Price'])

                # 剔除开、收盘集合竞价数据
                df = df[~df.Time.isin(['09:25:00','15:00:00'])]

                line += join_zb5(df, l, s)
                line += join_zb4(df, l, s)
                # line += join_zb3(df, l, s)
                r.append(line)

        df1 = pd.DataFrame(r,columns=colname)
        df1.to_excel('leaf\\'+ code + '_' + date[:7] +'_leaf.xlsx', index=False, )

def jan(codes, colname):
    path =  r'C:\Users\czh\Documents\critical\data\zhubi\201901'
    dates = ['2019-01-02','2019-01-03','2019-01-04','2019-01-07','2019-01-08','2019-01-09',
             '2019-01-10','2019-01-11','2019-01-14','2019-01-15','2019-01-16',
             '2019-01-17','2019-01-18','2019-01-21','2019-01-22','2019-01-23',
             '2019-01-24','2019-01-25','2019-01-28','2019-01-29','2019-01-30','2019-01-31',]
    #dates = ['2019-01-29','2019-01-30','2019-01-31',]
    js(codes, colname, path, dates)

def feb(codes, colname):
    path =  r'C:\Users\czh\Documents\critical\data\zhubi\201902'
    dates = ['2019-02-01','2019-02-11','2019-02-12','2019-02-13','2019-02-14',
             '2019-02-15','2019-02-18','2019-02-19','2019-02-20','2019-02-21',
             '2019-02-22','2019-02-25','2019-02-27','2019-02-28',]
    js(codes, colname, path, dates)

def mar(codes, colname):
    path =  r'C:\Users\czh\Documents\critical\data\zhubi\201903'
    dates = ['2019-03-01','2019-03-04','2019-03-05','2019-03-06','2019-03-07',
             '2019-03-08','2019-03-11','2019-03-12','2019-03-13','2019-03-14',
             '2019-03-15','2019-03-18','2019-03-19','2019-03-20','2019-03-21',
             '2019-03-22','2019-03-25','2019-03-26','2019-03-27','2019-03-28','2019-03-29',]
    #dates = ['2019-03-12']
    js(codes, colname, path, dates)

def apr(codes, colname):
    pass

def may(codes, colname):
    pass

def jun(codes, colname):
    pass

def jul(codes, colname):
    pass

def aug(codes, colname):
    pass

def sept(codes, colname):
    pass

def oct(codes, colname):
    pass

def nov(codes, colname):
    pass

def dec(codes, colname):
    pass

if __name__ == '__main__':
    # codes = ['300693']
    codes = ['300408','002273','002570','300433','300461','600776','300461','300602','002792',
             '002859','300136','300502','002273','002341','002454','002475','300134','300693',
             '300394','300036']

    colname = ['日期','主动单量比','主卖大单量占比','主买大单量占比',
                      '委卖笔均', '委买笔均','委托笔比','大单净量(万)','小单净量(万)','委卖大单量占比','委买大单量占比',]

    #jan(codes,colname)
    #feb(codes,colname)
    mar(codes,colname)
