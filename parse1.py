

import numpy as np
import pandas as pd
import os
import re
import pandas as pd
import datetime

import matplotlib.pyplot as plt


# '委卖总笔', '委卖50%', '委卖75%','委卖90%'
def join_zb(df,flag):
    r = []

    df_f = df[[flag[0],flag[1]]]
    df_f = df_f.drop_duplicates()
    r += [len(df_f)]
    df_f = df_f.sort_values(flag[0])
    df_f = df_f[:3000]
    r += list(df_f[flag[0]].quantile([.5, .75, .9]))
    #print(r)

    return r

def js(codes, path, dates, colname):
    for code in codes:
        r = []
        for date in dates:
            fn = path + '\\' + date + '\\' + code + '.csv'
            line = [date]
            if not os.path.exists(fn):
                ins = '7z x -r -y -o' + path + ' ' + path + '\\' + date.replace('-','') + '.7z '+ code + '.csv'
                print(ins)
                #os.system(ins)
                os.popen(ins)
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                line += join_zb(df, ['SaleOrderVolume','SaleOrderID'])
                line += join_zb(df, ['BuyOrderVolume','BuyOrderID'])
                r.append(line)

        df1 = pd.DataFrame(r,columns=colname)
        df1.to_excel('parse\\'+ code + '_' + date[:7] +'_parse.xlsx', index=False, )


def jan(codes,colname):
    path =  r'C:\Users\czh\Documents\critical\data\zhubi\201901'
    dates = ['2019-02-19']
    dates = ['2019-01-02','2019-01-03','2019-01-04','2019-01-07','2019-01-08','2019-01-09',
             '2019-01-10','2019-01-11','2019-01-14','2019-01-15','2019-01-16',
             '2019-01-17','2019-01-18','2019-01-21','2019-01-22','2019-01-23',
             '2019-01-24','2019-01-25','2019-01-28','2019-01-29','2019-01-30','2019-01-31',]

    js(codes, path, dates,colname)


def feb(codes,colname):
    path =  r'C:\Users\czh\Documents\critical\data\zhubi\201902'
    dates = ['2019-02-19']
    dates = ['2019-02-01','2019-02-11','2019-02-12','2019-02-13','2019-02-14',
             '2019-02-15','2019-02-18','2019-02-19','2019-02-20','2019-02-21',
             '2019-02-22','2019-02-25','2019-02-27','2019-02-28',]

    js(codes, path, dates,colname)

if __name__ == '__main__':
    # codes = ['300693']
    codes = ['300433']

    colname = ['日期','委卖总笔', '委卖50%', '委卖75%','委卖90%','委买总笔', '委买50%', '委买75%','委买90%',]

    jan(codes,colname)
    #feb(codes,colname)
        # df_p = pd.DataFrame(r, columns=['date','num'])
        # df_p = df_p.set_index('date')
        # print(df_p)
        # df_p.plot()
        # plt.show()
