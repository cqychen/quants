#coding=utf8

import tushare as ts
import pymysql
import time as dt
from datashape.coretypes import string
from pandas.io.sql import SQLDatabase
import sqlalchemy
import datetime
from sqlalchemy import create_engine
from pandas.io import sql
import threading
import pandas as pd
import sys
sys.path.append('../') #添加配置文件
from config import  *
'''
抓取宏观经济数据
'''
def load_macro_economy(): 
    #下载存款利率
    try:
        rs=ts.get_deposit_rate()
        pd.DataFrame.to_sql(rs, "deposit_rate", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载存款利率ok")
    except:
        print("下载存款利率出错")
    #下载贷款利率
    try:
        rs=ts.get_loan_rate()
        pd.DataFrame.to_sql(rs, "loan_rate", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载贷款利率ok")
    except:
        print("下载贷款利率出错")
    #下载存款准备金率
    try:
        rs=ts.get_rrr()
        pd.DataFrame.to_sql(rs, "rrr", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载存款准备金率ok")
    except:
        print("下载存款准备金率出错")
    #下载货币供应量
    try:
        rs=ts.get_money_supply()
        pd.DataFrame.to_sql(rs, "money_supply", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载货币供应量ok")
    except:
        print("下载货币供应量出错")
    #下载货币供应量(年底余额)
    try:
        rs=ts.get_money_supply_bal()
        pd.DataFrame.to_sql(rs, "money_supply_bal", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载货币供应量(年底余额)ok")
    except:
        print("下载货币供应量(年底余额)出错")
    #下载国内生产总值(年度)
    try:
        rs=ts.get_gdp_year()
        pd.DataFrame.to_sql(rs, "gdp_year", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载国内生产总值(年度)ok")
    except:
        print("下载国内生产总值(年度)出错")
    #下载国内生产总值(季度)
    try:
        rs=ts.get_gdp_quarter()
        pd.DataFrame.to_sql(rs, "gdp_quarter", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载国内生产总值(季度)ok")
    except:
        print("下载国内生产总值(季度)出错")
    #下载三大需求对GDP贡献
    try:
        rs=ts.get_gdp_for()
        pd.DataFrame.to_sql(rs, "gdp_for", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载三大需求对GDP贡献ok")
    except:
        print("下载三大需求对GDP贡献出错")
    #下载三大产业对GDP拉动
    try:
        rs=ts.get_gdp_pull()
        pd.DataFrame.to_sql(rs, "gdp_pull", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载三大产业对GDP拉动ok")
    except:
        print("下载三大产业对GDP拉动出错")
    #下载三大产业贡献率
    try:
        rs=ts.get_gdp_contrib()
        pd.DataFrame.to_sql(rs, "gdp_contrib", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载三大产业贡献率ok")
    except:
        print("下载三大产业贡献率出错")
    #下载居民消费价格指数
    try:
        rs=ts.get_cpi()
        pd.DataFrame.to_sql(rs, "gdp_cpi", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载居民消费价格指数ok")
    except:
        print("下载居民消费价格指数出错")
    #下载工业品出厂价格指数
    try:
        rs=ts.get_ppi()
        pd.DataFrame.to_sql(rs, "gdp_ppi", con=conn_macro_economy, flavor='mysql', if_exists='replace',index=True)
        print("下载工业品出厂价格指数ok")
    except:
        print("下载工业品出厂价格指数出错")

if __name__ == '__main__':
    print("--------------加载宏观信息开始-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='macro_economy'
    charset='utf8'
    conn_macro_economy = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始--------------------------------
    load_macro_economy()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))

