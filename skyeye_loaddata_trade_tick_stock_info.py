#coding=utf8
'''
Created on 2015
@author: chenyuqing
@contact: chen_yu_qin_g@163.com
@attention: 
'''
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
import pandas as pd
import os
import math
from blaze import nan
from numpy import NAN
#获取股票代码和股票名称
conn = pymysql.connect(user='skyeye', passwd='QWEqwe123!@#',host='114.215.165.196', db='stock_tratick',charset='utf8')
#mysql_conn = create_engine('mysql://root:cqychen882625@localhost:3306/stock')

def get_stock_info():
    '''
            从companyclassified中提取股票信息，这个表会每天进行更新，以获取最新的数据
            包括股票代码，上市日期，市盈率等信息
    '''
    sql_str="SELECT *  FROM  stock_company.`company_basic_info`";
    rs=sql.read_sql_query(sql=sql_str, con=conn, index_col='code', coerce_float=True)
    return rs
#获取股票列表
stock_info=get_stock_info();


def create_stock_info_table (stock_code):
    '''
            创建股票天交易表格,第一次加载数据执行
    '''
    create_st="create table if not exists tickdata_" \
            +stock_code \
            +"(`time` varchar(32),`price` double,`change` varchar(32),volume double,amount bigint,type varchar(32),`date` DATE, `datetime` datetime,stock_code varchar(32),index(`date`)) "\
            +"DEFAULT CHARSET=utf8"
    print(create_st)
    cur=conn.cursor()
    cur.execute(create_st)

def drop_stock_info_table(table_name):
    '''
    drop掉表，如果存在
    '''
    drop_st="drop table if exists tickdata_" \
            +table_name
    cur=conn.cursor()
    cur.execute(drop_st)
    
def is_empty_table(table_name):
    '''
            得到表格的行数
    '''
    get_str="select  count(1) from tickdata_"+table_name;
    cur=conn.cursor()
    try:
        cur.execute(get_str);
        return cur.fetchall()[0][0];
    except:
        print("查询记录出错")

def last_date_table(table_name):
    '''
            获取表格最新的日期。
    '''
    get_str="select  max(`date`) from tickdata_"+table_name;
    print("执行脚本：")
    print(get_str)
    cur=conn.cursor()
    cur.execute(get_str);
    rs=cur.fetchall()  #注意fethchall只能使用一次。
    return datetime.datetime.strptime('2016-04-01','%Y-%m-%d')  if rs[0][0] is None else rs[0][0]
'''
初始化一个股票的历史交易数据
'''
def inital_stock_tick_data(stock_code,datadate):
    df = ts.get_tick_data(stock_code,date=datadate)
    df['date']=datadate
    df['datetime']=df['date']+' '+df['time']
    print(len(df))
    print(df.head(10))

def load_stock_tick_data(stock_code,data_date):
    file_name=stock_code+"_"+data_date+".csv"
    #my path
    #loadpath="/home/cqychen/";
    #server path
    loadpath="/var/lib/mysql.dat/tick_data/"+data_date[0:7]+"/";
    
    file=loadpath+file_name;
    if(not (os.path.isfile(file))):
        print("%s文件不存在"%file)
        return
    
    #创建表格
    create_stock_info_table (stock_code)
    df=pd.read_csv(loadpath+file_name);
    df['date']=data_date
    df['datetime']=df['date']+' '+df['time']
    df['stock_code']=stock_code
    #写入到mysql中
    try:
        pd.DataFrame.to_sql(df, "tickdata_"+stock_code, con=conn, flavor='mysql', if_exists='append',index=False)
    except:
        print("加载股票%s分笔数据出错"%stock_code);
        
def load_stock_tick_data_all():
    for tempdate in pd.date_range('01/01/2015','01/31/2015'):
        tempdate=tempdate.strftime('%Y-%m-%d');
        for tempcode in stock_info.index:
            print(tempcode+"----"+tempdate);
            load_stock_tick_data(tempcode,tempdate);

def load_stock_tick_data_date(stock_code):
    '''
    本函数主要是从tushare中拉取数据
    '''
    #创建表格：
    create_stock_info_table (stock_code)
    ##获取开始时间和结束时间,结束时间为当前时间，
    startdate=int(dt.mktime(last_date_table(stock_code).timetuple()))+3600*24;
    startdate=dt.localtime(startdate)
    startdate=dt.strftime("%Y-%m-%d", startdate)
    enddate=dt.strftime("%Y-%m-%d",dt.localtime())
    
    ##字符串转换成时间
    startdate =  dt.strptime(startdate, "%Y-%m-%d")
    enddate=dt.strptime(enddate, "%Y-%m-%d")
    
    #提取开始日期
    y1,m1,d1 = startdate[0:3]
    startdate=datetime.date(y1,m1,d1)
    #提取结束日期
    y2,m2,d2 = enddate[0:3]
    enddate=datetime.date(y2,m2,d2)
    
    print("startdate is :",startdate,"   end date is:",enddate, "  stock code is:",stock_code)
    
    while startdate <= enddate:  
        print(startdate)
        tempdate=startdate.strftime("%Y-%m-%d")  
        df = ts.get_tick_data(stock_code,date=tempdate)
        startdate = startdate + datetime.timedelta(1)
        if(math.isnan(df.loc[0, ['price']])):
            print("this day has no data")
        else:
            print("this day has data")
            df['date']=tempdate
            df['datetime']=df['date']+' '+df['time']
            df['stock_code']=stock_code
            try:
                pd.DataFrame.to_sql(df, "tickdata_"+stock_code, con=conn, flavor='mysql', if_exists='append',index=False)
                print("加载股票%s数据，日期%s完成"%(stock_code,tempdate))
            except:
                print("加载股票%s分笔数据出错"%stock_code)
                
def load_stock_tick_data_all_date():
    tempnum=1;
    rs=stock_info.index;
    total_num=len(rs);
    for tempcode in stock_info.index:
        load_stock_tick_data_date(tempcode)
        tempnum+=1
        print("this is the number %s  there are  %s stock code is %s"%(tempnum,total_num,tempcode))

#load_stock_tick_data_date('600401')
load_stock_tick_data_all_date()
    
    
