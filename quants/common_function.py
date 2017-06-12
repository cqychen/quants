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

def get_mysql_conn():
    '''
    :return:返回数据库链接信息 
    '''
    iphost='114.215.165.196'
    user='skyeye'
    passwd='QWEqwe123!@#'
    return iphost,user,passwd

def get_stock_info():
    '''
    从companyclassified中提取股票信息，这个表会每天进行更新，以获取最新的数据
    包括股票代码，上市日期，市盈率等信息
    '''
    sql_str="SELECT *  FROM  stock_company.`company_basic_info`"
    iphost, user, passwd = get_mysql_conn()
    db='stock_company'
    charset='utf8'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    rs=sql.read_sql_query(sql=sql_str, con=conn, index_col='code', coerce_float=True)
    return rs




