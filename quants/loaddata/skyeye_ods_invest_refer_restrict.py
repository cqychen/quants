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
import numpy as np
import math
import sys
sys.path.append('../') #添加配置文件
from common_function import  *
def create_table(table_name):
    cmd='''
    create table if not exists %s
    (
    `code`           VARCHAR (10)  comment '股票代码'
    ,`name`          VARCHAR (63)  comment '股票名称'
    ,`date`          VARCHAR (63)  comment '解禁日期'
    ,`count`         DOUBLE        comment '解禁数量'
    ,`ratio`         DOUBLE        comment '占总盘比例'
    ,`year`          INT            comment '解禁年份'
    ,`month`         INT            comment '解禁月份'
    ,PRIMARY KEY (`code`,`date`)
    ,index(code)
    )DEFAULT CHARSET=utf8
    '''%table_name
    print (cmd)
    run_mysql_cmd(cmd,conn)
def get_data_date(year,month):
    cmd='''delete  from %s where `year`=%s and `month`=%s '''%(table_name,year,month)
    print cmd
    run_mysql_cmd(cmd=cmd,conn=conn) #删除指定年和月的数据
    try:
        rs=ts.xsg_data(year=year,month=month)
        rs['year']=year
        rs['month']=month
        rs=rs.drop_duplicates() #去除重复的数据，没想到还有重复的，心塞塞，这个api不咋地啊，挖地兔
        pd.DataFrame.to_sql(rs, table_name, con=conn , flavor='mysql', if_exists='append',index=False)
        return rs
    except:
        print("this year has no records")
        return None
def load_data():
    #下载公司基本信息，包括股票代码、pe、市盈率等数据
    max_year=int(get_max_year_table(table_name)) #还要获取前一年的情况
    (year,quarter,mon,day,hour,min,sec,wday,yday,isdst)=get_date_struct()
    while True:
        print(max_year,year)
        if(max_year<=year):
            print("geting year is %s"%max_year)
            get_data_date(year=max_year,month=1)
            get_data_date(year=max_year,month=2)
            get_data_date(year=max_year,month=3)
            get_data_date(year=max_year,month=4)
            get_data_date(year=max_year,month=5)
            get_data_date(year=max_year,month=6)
            get_data_date(year=max_year,month=7)
            get_data_date(year=max_year,month=8)
            get_data_date(year=max_year,month=9)
            get_data_date(year=max_year,month=10)
            get_data_date(year=max_year,month=11)
            get_data_date(year=max_year,month=12)
            print("\n\n")
            max_year=max_year+1
        else:
            break
if __name__ == '__main__':
    print("--------------任务开始-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    table_name='ods_invest_refer_restrict'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始--------------------------------
    create_table(table_name=table_name) #建立表格
    load_data()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
