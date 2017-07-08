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
    code             VARCHAR (10)  comment '股票代码'
    ,`name`          VARCHAR (63)  comment '股票名称'
    ,`year`          VARCHAR (10)  comment '分配年份'
    ,report_date     VARCHAR (63)  comment '公布日期'
    ,divi            DOUBLE        comment '分红金额（每10股）'
    ,shares          DOUBLE        comment '转增和送股数（每10股）'
    ,PRIMARY KEY(code,report_date)
    ,index(code)
    )DEFAULT CHARSET=utf8
    '''%table_name
    print (cmd)
    run_mysql_cmd(cmd,conn)
def get_data_date(year):
    cmd='''
    select * from %s
    '''%(table_name)
    print cmd
    df=pd.read_sql(sql=cmd,con=conn) #从mysql中读取指定年份的数据
    rs=ts.profit_data(year=year,top=3000)#从网上获取近3000条分配预案
    last_df=pd.merge(rs, df, how='left', on=['code','report_date'],left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True) #进行左连接,股票代码和发布如期唯一确定一行
    for i in range(len(last_df)):
        last_df.loc[i,'flag']=math.isnan(float(last_df.loc[i,'divi_y'])) #新加一列flag
    last_df=last_df[last_df.flag] #剔除重复的行，插入数据库
    #剔除不需要的列
    del last_df['name_y']
    del last_df['year_y']
    del last_df['divi_y']
    del last_df['shares_y']
    del last_df['flag']
    last_df.columns = ['code','name','year','report_date','divi','shares'] #重新修改列名
    return last_df
def load_data():
    #下载公司基本信息，包括股票代码、pe、市盈率等数据
    max_year=int(get_max_date_profit_dis())-1 #还要获取前一年的情况
    (year,quarter,mon,day,hour,min,sec,wday,yday,isdst)=get_date_struct()
    while True:
        if(max_year<=year):
            print("geting year is %s"%max_year)
            rs=get_data_date(year=max_year)
            print("\n\n")
            pd.DataFrame.to_sql(rs, table_name, con=conn , flavor='mysql', if_exists='append',index=False)
            max_year=max_year+1
        else:
            break
if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载公司基本信息开始-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    table_name='ods_invest_refer_profit_dis'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始--------------------------------
    create_table(table_name=table_name) #建立表格
    load_data()
    #get_data_date(year='2012')
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
