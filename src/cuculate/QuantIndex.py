'''
Created on 2015年12月21日

@author: Administrator
'''

import tushare as ts;
import pymysql;
import time as dt
from datashape.coretypes import string
from pandas.io.sql import SQLDatabase
import sqlalchemy
import datetime
from sqlalchemy import create_engine
from pandas.io import sql
import threading
import pandas as pd

conn_get_data = pymysql.connect(user='root', passwd='cqychen882625',host='localhost', db='stock',charset='utf8')
print("===================calculate the quant index================")

rs=sql.read_frame('select * from basic_300228 order by `date`',conn_get_data);

rs[['date','close']]
print(rs[['date','close']]);

ma_list = [5, 20, 60];
ema12=pd.ewma(rs['close'],span=12)
ema26=pd.ewma(rs['close'],span=26)
kmacd=ema12-ema26
kmacd_sign=pd.ewma(kmacd,span=9)
macdz=(kmacd-kmacd_sign)*2
print(kmacd)
print(macdz)


#mask= (macdz['close']>macdz['close'].shift(1));
print(mask)
#print(macdz[mask])













