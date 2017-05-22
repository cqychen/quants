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
#获取股票代码和股票名称
conn = pymysql.connect(user='skyeye', passwd='QWEqwe123!@#',host='114.215.165.196', db='stock_tradata',charset='utf8')
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
    create_st="create table if not exists histdata_" \
            +stock_code \
            +"(date DATE primary key,open double,high double,close double,low double,volume bigint,price_change double,p_change double,ma5 double,ma10 double,ma20 double,v_ma5 double,v_ma10 double,v_ma20 double,turnover double) "\
            +"DEFAULT CHARSET=utf8"
    cur=conn.cursor()
    cur.execute(create_st)

def drop_stock_info_table(table_name):
    '''
    drop掉表，如果存在
    '''
    drop_st="drop table if exists histdata_" \
            +table_name
    cur=conn.cursor()
    cur.execute(drop_st)
    
def is_empty_table(table_name):
    '''
            得到表格的行数
    '''
    get_str="select  count(1) from hist_"+table_name;
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
    get_str="select  max(`date`) from histdata_"+table_name;
    print("执行脚本：")
    print(get_str)
    cur=conn.cursor()
    cur.execute(get_str);
    rs=cur.fetchall()  #注意fethchall只能使用一次。
    return datetime.datetime.strptime('1991-01-01','%Y-%m-%d')  if rs[0][0] is None else rs[0][0]
'''
初始化一个股票的历史交易数据
'''
def inital_stock_data(stock_code):   
    '''
            初始化股票数据，
            首先创建股票表格，如果没有的话，第一次加载会自动创建表格
            然后获取开始时间，开始时间是股票上市的时间与上一次加载的时间中的最小时间。
            加载股票数据是每三年加载一次，防止一次加载过多导致网络终端。
            本方法可以自动全量以及增量加载股票数据
    '''
        #初始化表格,如果没有该表格，则进行创建表格.
    try:
        create_stock_info_table(stock_code)    
        #开始时间要加一天
        startdate=int(dt.mktime(last_date_table(stock_code).timetuple()))+3600*24;
        startdate=dt.localtime(startdate)
        startdate=dt.strftime("%Y-%m-%d", startdate)
    
        enddate=dt.strftime("%Y-%m-%d",dt.localtime())
        print("startdate is :",startdate,"   end date is:",enddate, "  stock code is:",stock_code)
        rs=ts.get_hist_data(stock_code,start=startdate,end=enddate)
        
        
        if rs is None:#如果得到的结果为空，比如停牌，或者刚好获取的数据没有，要使用continue，不然下面的语句调用不起来
            print("股票%s在这段日期内没有交易数据\n"%stock_code)
            return 1
        pd.DataFrame.to_sql(rs, "histdata_"+stock_code, con=conn, flavor='mysql', if_exists='append',index=True)
        print("提取股票%s数据正确"%stock_code)
        return 1  #如果抽取成功，那返回1
    except:
        print("提取股票%s数据出错"%stock_code)
        return 0  #抽取失败，返回0

def initial_stock_hist_data_all():
    #将日志写入文件
    start_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
    logdate=dt.strftime("%Y%m%d",dt.localtime())
    
    loadpath='/root/cronscript/logs/'; #server
    #loadpath='/home/cqychen/stockproject/tradata'; #my
    basicloadlog=loadpath+"initialloghdata"+logdate+".txt";
    fileop=open(basicloadlog, mode='w')
    fileop.write("start time is :"+start_date_formate)
    
    errorlog=loadpath+"initialerrorloghdata"+logdate+".txt";
    errorfileop=open(errorlog, mode='w')
    errorfileop.write("start time is :"+start_date_formate)
    #开始进行顺序加载

    rs=stock_info.index;
    total_num=len(rs);
    tempnum=1;
    for stock_code in rs:
        #startstr="stock code is:"+stock_code+"there are:"+total_num+" task"+" this is the number:"+tempnum,"finished:"+tempnum/total_num+"\n"
        startstr=["stock code is:",stock_code,"there are:",'%d' % total_num," task"," this is the number:",'%d' % tempnum,"finished:",'%d' % (tempnum/total_num),"\n"]
        inital_stock_data(stock_code)
        print("stock code is:",stock_code,"there are:",total_num," task"," this is the number:",tempnum,"finished:",tempnum/total_num)
        tempnum=tempnum+1;
        fileop.writelines(startstr)
    
    #写入结束时间
    end_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
    fileop.write("end time is :"+end_date_formate)
    fileop.close();  
    errorfileop.write("end time is :"+end_date_formate)
    errorfileop.close()

#initial_stock_h_data_all()
initial_stock_hist_data_all()

    
