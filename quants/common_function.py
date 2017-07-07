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
import commands

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
    sql_str="SELECT *  FROM  ods_data.`ods_company_basic_info`"
    iphost, user, passwd = get_mysql_conn()
    db='ods_data'
    charset='utf8'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    rs=sql.read_sql_query(sql=sql_str, con=conn, index_col='code', coerce_float=True)
    return rs
def get_index_info():
    '''
    从companyclassified中提取股票信息，这个表会每天进行更新，以获取最新的数据
    包括股票代码，上市日期，市盈率等信息
    '''
    sql_str="SELECT *  FROM  ods_data.`ods_classified_index`"
    iphost, user, passwd = get_mysql_conn()
    db='ods_data'
    charset='utf8'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    rs=sql.read_sql_query(sql=sql_str, con=conn, index_col='code', coerce_float=True)
    return rs
def run_mysql_cmd(cmd,conn):
    '''
    :param cmd:执行的cmd命令 
    :param conn: 连接情况
    :return: 
    '''
    cmd=cmd
    conn=conn
    cur=conn.cursor()
    cur.execute(cmd)
    conn.commit()
    return cur.fetchall()
def get_max_date_stock(stock_code):
    '''
    :param stock_code:输入股票代码 
    :return: 得到股票代码的最新数据日期
    '''
    cmd='''
    select IFNULL(max(date),'2005-01-01') 
    from ods_data.ods_tra_day_k
    where code='%s'
    '''%stock_code
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    return run_mysql_cmd(cmd, conn)[0][0]
def get_max_date_stock_w(stock_code):
    '''
    :param stock_code:输入股票代码 
    :return: 得到股票代码的最新数据日期
    '''
    cmd='''
    select IFNULL(max(date),'2005-01-01') 
    from ods_data.ods_tra_week_k
    where code='%s'
    '''%stock_code
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    return run_mysql_cmd(cmd, conn)[0][0]
def get_min_date_stock(stock_code):
    '''
    :param stock_code:输入股票代码 
    :return: 得到股票代码的最新数据日期
    '''
    cmd = '''
    select IFNULL(min(date),'2005-01-01') 
    from ods_data.ods_tra_day_k
    where code='%s'
    ''' % stock_code
    iphost, user, passwd = get_mysql_conn()
    db = 'ods_data'
    charset = 'utf8'
    conn = pymysql.connect(user=user, passwd=passwd, host=iphost, db=db, charset=charset)
    return run_mysql_cmd(cmd, conn)[0][0]
def get_min_date_stock_w(stock_code):
    '''
    :param stock_code:输入股票代码 
    :return: 得到股票代码的最新数据日期
    '''
    cmd = '''
    select IFNULL(min(date),'2005-01-01') 
    from ods_data.ods_tra_week_k
    where code='%s'
    ''' % stock_code
    iphost, user, passwd = get_mysql_conn()
    db = 'ods_data'
    charset = 'utf8'
    conn = pymysql.connect(user=user, passwd=passwd, host=iphost, db=db, charset=charset)
    return run_mysql_cmd(cmd, conn)[0][0]
def get_date_add_days(date,nums):
    rsdate = int(dt.mktime(datetime.datetime.strptime(date,'%Y-%m-%d').timetuple())) + nums*3600 * 24
    rsdate = dt.localtime(rsdate)
    rsdate = dt.strftime("%Y-%m-%d", rsdate)
    return rsdate
def get_date_now():
    return datetime.datetime.now().strftime('%Y-%m-%d')
def get_max_date_index(stock_code):
    '''
    :param stock_code:输入股票代码
    :return: 得到股票代码的最新数据日期
    '''
    cmd='''
    select IFNULL(max(date),'2005-01-01')
    from ods_data.ods_tra_day_k_index
    where int_code='%s'
    '''%stock_code
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    return run_mysql_cmd(cmd, conn)[0][0]
def get_min_date_index(stock_code):
    '''
    :param stock_code:输入股票代码
    :return: 得到股票代码的最新数据日期
    '''
    cmd = '''
    select IFNULL(min(date),'2005-01-01')
    from ods_data.ods_tra_day_k_index
    where code='%s'
    ''' % stock_code
    iphost, user, passwd = get_mysql_conn()
    db = 'ods_data'
    charset = 'utf8'
    conn = pymysql.connect(user=user, passwd=passwd, host=iphost, db=db, charset=charset)
    return run_mysql_cmd(cmd, conn)[0][0]
def get_max_date_index_w(stock_code):
    '''
    :param stock_code:输入股票代码
    :return: 得到股票代码的最新数据日期
    '''
    cmd='''
    select IFNULL(max(date),'2005-01-01')
    from ods_data.ods_tra_week_k_index
    where int_code='%s'
    '''%stock_code
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    return run_mysql_cmd(cmd, conn)[0][0]
def get_max_date_profit_dis():
    '''
    :param stock_code:输入股票代码
    :return: 得到股票代码的最新数据日期
    '''
    cmd='''
    select IFNULL(max(`year`),'2005')
    from ods_data.ods_invest_refer_profit_dis
    '''
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    return run_mysql_cmd(cmd, conn)[0][0]

def get_date_struct(timestamp=dt.time()):
    (year,mon,day,hour,min,sec,wday,yday,isdst)=dt.localtime(timestamp)
    quarter=(mon-1)/3+1
    return (year,quarter,mon,day,hour,min,sec,wday,yday,isdst)
if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------main 函数测试-----------------------------")
    print get_date_struct()
    for mon in range(1,13):
        quarter=(mon-1)/3+1
        print (mon,quarter)







