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
#获取股票代码和股票名称
conn = pymysql.connect(user='root', passwd='cqychen123!@#',host='localhost', db='stock',charset='utf8')


#mysql_conn = create_engine('mysql://root:cqychen882625@localhost:3306/stock')

def get_stock_info():
    '''
            从companyclassified中提取股票信息，这个表会每天进行更新，以获取最新的数据
            包括股票代码，上市日期，市盈率等信息
    '''
    sql_str="SELECT *  FROM  companyclassified.`company_basic_info`";
    rs=sql.read_sql_query(sql=sql_str, con=conn, index_col='code', coerce_float=True)
    return rs

#获取股票列表
stock_info=get_stock_info();


def create_stock_info_table (table_name):
    '''
            创建股票天交易表格
    '''
    create_st="create table if not exists basic_" \
            +table_name \
            +"(date DATE primary key,open double,high double,close double,low double,volume bigint,amount bigint) "\
            +"DEFAULT CHARSET=utf8"
    cur=conn.cursor()
    cur.execute(create_st)

def drop_stock_info_table(table_name):
    '''
    drop掉表，如果存在
    '''
    drop_st="drop table if exists basic_" \
            +table_name
    cur=conn.cursor()
    cur.execute(drop_st)
def is_empty_table(table_name):
    '''
            得到表格的行数
    '''
    get_str="select  count(1) from basic_"+table_name;
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
    get_str="select  max(`date`) from basic_"+table_name;
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
    try :
        #初始化表格,如果没有该表格，则进行创建表格.
        create_stock_info_table(stock_code)    
        startdate=stock_info.ix[stock_code,'timeToMarket']
        startdate='%d' % startdate #转换成字符串需要。 
        startse1=int(dt.mktime(dt.strptime(startdate,'%Y%m%d')));
        startse2=int(dt.mktime(last_date_table(stock_code).timetuple()))+3600*24;
        #获取开始时间的秒以及结束时间的秒。然后每三年获取一次数据插入到表格中。
        startse=startse1 if startse1>startse2 else startse2
        endse=int(dt.time())
        for temp in range(startse,endse,93312000): #按照360天计算：3*360*24*3600=93312000   
            #获取开始日期
            ltime=dt.localtime(temp)
            startdate=dt.strftime("%Y-%m-%d", ltime)
            #获取结束日期
            ltime=dt.localtime(temp+93225600)
            enddate=dt.strftime("%Y-%m-%d", ltime)
            if (endse-temp)<93312000:
                ltime=dt.localtime()
                enddate=dt.strftime("%Y-%m-%d", ltime)
            print("startdate is :",startdate,"   end date is:",enddate, "  stock code is:",stock_code)
            #=============如下进行异常处理，这个地方比较容易出现异常=========
            try:
                rs=ts.get_h_data(stock_code,start=startdate,end=enddate)
            except:
                print("在抽取股票代码：",stock_code,"的时候发生了错误")
                return 0 #如果出现异常，那么将返回这个股票的代码，便于重新进行抽取。
            if rs is None:#如果得到的结果为空，比如停牌，或者刚好获取的数据没有，要使用continue，不然下面的语句调用不起来
                continue
            sql.write_frame(rs, "basic_"+stock_code, con=conn, flavor='mysql', if_exists='append',index=True)
        return 1  #如果抽取成功，那返回1
    except: 
        print("提取股票数据出错")
        return 0  #抽取失败，返回0
        

def inital_data(stock_code_list):
    '''
            这里采用多线程进行拉取数据。貌似不太好用。先留着，以后研究。
    '''
    code_len=len(stock_code_list)
    theads_num=50
    for i in range(0,code_len,theads_num):
        theads=[]
    # 创建线程
        for j in range(i,i+theads_num):
            if(j>=code_len) :
                break
            print("i is :",i, "  j is:",j ," stock code is :",stock_code_list[j]) 
            t=threading.Thread(target=inital_stock_data,args=(stock_code_list[j],))
            t.setDaemon(True)
            theads.append(t)
    #启动线程
        for k in range(0,len(theads)):        
            theads[k].start()
    #结束线程
        for k in range(0,len(theads)):
            theads[k].join(20)

def initial_tick_data():
    pass

def load_stock_trade_data_order():
    '''
            初始化股票交易数据，这里是通过串行抽取数据的，
            多线程因为有点问题， 先采用串行方式，后期进行改造
    '''
    #将日志写入文件
    start_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
    logdate=dt.strftime("%Y%m%d",dt.localtime())
    basicloadlog="E:/logs/loaddatalog/loadbasicstockdata"+logdate+".txt";
    fileop=open(basicloadlog, mode='w')
    fileop.write("start time is :"+start_date_formate)
    
    errorlog="E:/logs/loaddatalog/errortockdata"+logdate+".txt";
    errorfileop=open(errorlog, mode='w')
    errorfileop.write("start time is :"+start_date_formate)
    #开始进行顺序加载

    rs=stock_info.index;
    total_num=len(rs);
    tempnum=1;
    for stock_code in rs:
        #startstr="stock code is:"+stock_code+"there are:"+total_num+" task"+" this is the number:"+tempnum,"finished:"+tempnum/total_num+"\n"
        startstr=["stock code is:",stock_code,"there are:",'%d' % total_num," task"," this is the number:",'%d' % tempnum,"finished:",'%d' % (tempnum/total_num),"\n"]
        if(0==inital_stock_data(stock_code)):
            errorfileop.writelines(stock_code+"发生错误") 
        print("stock code is:",stock_code,"there are:",total_num," task"," this is the number:",tempnum,"finished:",tempnum/total_num)
        tempnum=tempnum+1;
        fileop.writelines(startstr)
    
    #写入结束时间
    end_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
    fileop.write("end time is :"+end_date_formate)
    fileop.close();  
    errorfileop.write("end time is :"+end_date_formate)
    errorfileop.close()
    
