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
import sys
sys.path.append('../') #添加配置文件
from common_function import  *
from sqlalchemy.types import VARCHAR


def load_data():
    #下载公司基本信息，包括股票代码、pe、市盈率等数据
    try:
        rs=ts.get_stock_basics()
        pd.DataFrame.to_sql(rs, name="ods_company_basic_info", con=con ,schema=db, if_exists='replace',index=True,dtype={'code': VARCHAR(rs.index.get_level_values('code').str.len().max())})
        print("公司基本信息数据ok")
    except Exception as e:
        print(e)
        print("公司基本信息数据出错")

if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载公司基本信息开始-----------------------------")
    startTime=dt.time()
    con=get_mysql_conn()
    db='ods_data'
    table_name='ods_company_basic_info'
    #--------------------脚本运行开始--------------------------------
    load_data()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
