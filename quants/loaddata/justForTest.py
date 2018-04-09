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
from urllib import parse


import sys
sys.path.append('../') #添加配置文件
from common_function import  *

import datetime
import dateutil
rs=ts.get_stock_basics()
rs=rs.fillna(0)
print (rs.head(10))

connstr = "mysql+pymysql://skyeye:%s@127.0.0.1:3306/ods_data?charset=utf8" % parse.quote_plus('QWEqwe123!@#')
engine = create_engine(connstr,echo=True,max_overflow=5);

#pd.DataFrame.to_sql(rs, table_name, con= , flavor='mysql', if_exists='replace',index=True)
pd.DataFrame.to_sql(rs,con=engine,name="test_table",schema="db_name",if_exists='replace',index=False)
