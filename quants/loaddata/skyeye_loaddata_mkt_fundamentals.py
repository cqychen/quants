#coding=utf8

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
import pandas as pd;

conn_mkt_fundamentals = pymysql.connect(user='skyeye', passwd='QWEqwe123!@#',host='114.215.165.196', db='mkt_fundamentals',charset='utf8')

'''
抓取基本面数据
'''
def load_mkt_fundamentals(): 
    #下载加载股票列表数据
    try:
        rs=ts.get_stock_basics()
        pd.DataFrame.to_sql(rs, "stock_basics", con=conn_mkt_fundamentals, flavor='mysql', if_exists='replace',index=True)
        print("下载股票列表ok")
    except:
        print("下载股票列表出错")

load_mkt_fundamentals()
