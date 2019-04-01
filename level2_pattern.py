
import pandas as pd
import numpy as np

def p1(df):
    """
        主卖或主买大单量占比 大于0.3
    Parameters
    ------
      df:
    return
    -------
      None
    """
    r = []

    df_s = df[df['主卖大单量占比']>0.3]
    for i, row in df_s.iterrows():
        r.append(row['日期'] + ' 主卖大单量占比： ' + str(row['主卖大单量占比']))

    df_b = df[df['主买大单量占比']>0.3]
    for i, row in df_b.iterrows():
        r.append(row['日期'] + '                          主买大单量占比： ' + str(row['主买大单量占比']))

    return r

def p2(df):
    """
        委托笔比 绝对值大于2
    Parameters
    ------
      df:
    return
    -------
      None
    """
    r = []

    df1 = df[(df['委托笔比']>2) | (df['委托笔比']<-2)]
    for i, row in df1.iterrows():
        r.append(row['日期'] + ' 委托笔比： ' + str(row['委托笔比']))
    return r

def level2_pattern(df):
    '''
    输入：
        df：
    '''

    r =[]

    r += p1(df)
    r += p2(df)

    return r


if __name__ == "__main__":
    codes = ['300408','002273','002570','300433','300461','600776','300461','300602','002792',
             '002859','300136','300502','002341','002454','002475','300134','300693',
             '300394','300036']

    for code in codes:
        df = pd.read_excel('leaf\\'+ code + '_2019-01_leaf.xlsx')
        items = level2_pattern(df)
        for item in items:
            print(code,item)
