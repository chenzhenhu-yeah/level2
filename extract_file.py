import numpy as np
import pandas as pd
import os
import re
import pandas as pd
import datetime


path = r'C:\Users\czh\Documents\critical\data\zhubi'
codes = ['002792']
for folderName,subfolders,filenames in os.walk(path+r'\201901'):
    for file_name in filenames:
        for code in codes:
            if file_name.startswith(code):
                fn = folderName + '\\' + file_name
                #print(fn)
                s_date = folderName[-10:]
                s_date = s_date.replace('-','')
                d_fn = path + '\\care\\' + code + '_' + s_date + '.csv'
                os.system('copy ' + fn + ' ' + d_fn )
