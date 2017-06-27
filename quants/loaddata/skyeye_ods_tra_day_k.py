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

def create_table(table_name):
    cmd='''
    create table if not exists %s
    (
    data_date	    varchar(20)   	COMMENT '数据日期（索引）'
    ,open_price	    FLOAT	        COMMENT '开盘价'
    ,close_price	FLOAT	        COMMENT '收盘价'
    ,high_price	    FLOAT	        COMMENT '最高价'
    ,low_price	    FLOAT	        COMMENT '最低价'
    ,volume	        FLOAT	        COMMENT '成交量'
    ,stock_code	    varchar(10)	    COMMENT '股票代码（索引）和数据日期联合作为主键'
    ,PRIMARY KEY(stock_code,data_date)
    ,index(stock_code)
    )DEFAULT CHARSET=utf8
    '''%table_name
    print (cmd)
    run_mysql_cmd(cmd,conn)
def get_data():


def load_data():
    #下载公司基本信息，包括股票代码、pe、市盈率等数据
    try:
        rs=ts.get_stock_basics()
        pd.DataFrame.to_sql(rs, "ods_company_basic_info", con=conn , flavor='mysql', if_exists='replace',index=True)
        print("公司基本信息数据ok")
    except:
        print("公司基本信息数据出错")

if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载股票日k线-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    table_name='ods_tra_day_k'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始--------------------------------
    create_table(table_name=table_name)
    #load_company_basic_info()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
