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
    `date`	    varchar(20)   	COMMENT '数据日期（索引）'
    ,`open`	    FLOAT	        COMMENT '开盘价'
    ,`close`	FLOAT	        COMMENT '收盘价'
    ,`high`	    FLOAT	        COMMENT '最高价'
    ,`low`	    FLOAT	        COMMENT '最低价'
    ,`volume`	        FLOAT	        COMMENT '成交量'
    ,`code`	    varchar(10)	    COMMENT '股票代码（索引）和数据日期联合作为主键'
    ,`int_code` VARCHAR(10)    COMMENT '简短的指数代码'
    ,PRIMARY KEY(int_code,`date`)
    ,index(int_code)
    )DEFAULT CHARSET=utf8
    '''%table_name
    print (cmd)
    run_mysql_cmd(cmd,conn)
def load_data():
    stock_code = get_index_info().index
    total_num = len(stock_code);
    tempnum = 1;
    for tmp_stock_code in stock_code:
        tempnum = tempnum + 1
        start_date=get_date_add_days(get_max_date_index(tmp_stock_code),1) #在最大天数加一天作为日期
        end_date=get_date_now()
        print(tempnum,tmp_stock_code,start_date,end_date)
        tmp_rs = ts.get_k_data(code=tmp_stock_code, start=start_date, end=end_date,index=True, ktype='D')
        tmp_rs['int_code']=tmp_stock_code
        pd.DataFrame.to_sql(tmp_rs, table_name, con=conn, flavor='mysql', if_exists='append', index=False)
if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载股票日k线-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    table_name='ods_tra_day_k_index'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始--------------------------------
    create_table(table_name=table_name)
    load_data()
    #print get_max_date_index('sh000001')
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
