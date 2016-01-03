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
conn = pymysql.connect(user='root', passwd='cqychen882625',host='localhost', db='stock',charset='utf8')


#mysql_conn = create_engine('mysql://root:cqychen882625@localhost:3306/stock')
'''
创建basic_code表格，将数据加载进入到该表格中。
传入股票代码名称，根据名称创建表格。
'''
def create_stock_info_table (table_name):
    create_st="create table if not exists basic_" \
            +table_name \
            +"(date DATE primary key,open double,high double,close double,low double,volume bigint,amount bigint) "\
            +"DEFAULT CHARSET=utf8"
    cur=conn.cursor()
    cur.execute(create_st)
'''
drop掉表，如果存在
'''
def drop_stock_info_table(table_name):
    drop_st="drop table if exists basic_" \
            +table_name
    cur=conn.cursor()
    cur.execute(drop_st)
'''
从stock_info表中提取股票代码，然后通过代码进行创建表格，插入数据等一系列操作。
'''
def get_stock_code():
    get_str="select  substring(stock_code,3) from stock_info";
    cur=conn.cursor()
    cur.execute(get_str);
    return cur.fetchall();
'''
得到表格的行数
'''
def is_empty_table(table_name):
    get_str="select  count(1) from basic_"+table_name;
    cur=conn.cursor()
    try:
        cur.execute(get_str);
        return cur.fetchall()[0][0];
    except:
        print("查询记录出错")
'''
获取表格最新的日期。
'''
def last_date_table(table_name):
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
    #初始化表格
    try :
        create_stock_info_table(stock_code)   
        df = ts.get_stock_basics()
        startdate = df.ix[stock_code]['timeToMarket'] #上市日期YYYYMMDD
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
                return stock_code #如果出现异常，那么将返回这个股票的代码，便于重新进行抽取。
            if rs is None:#如果得到的结果为空，比如停牌，或者刚好获取的数据没有，要使用continue，不然下面的语句调用不起来
                continue
            sql.write_frame(rs, "basic_"+stock_code, con=conn, flavor='mysql', if_exists='append',index=True)
        return 'success'
    except: 
        print("提取股票数据出错")
'''
这里采用多线程进行拉取数据。貌似不太好用。先留着，以后研究。
'''
def inital_data(stock_code_list):
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

'''
主函数区
'''



#==========================================================================================================================
print("=================task start,calculate start time==================")
start_time=dt.time()
start_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
print("start time is :",start_date_formate," start seconds is :",start_time)
print("===================================================================")
#==========================================================================================================================
   
#测试区域


#
'''
#运行区域-------------------------

#如下是进行顺序执行的代码。
'''
#将程序执行情况输出到文件中，方便日志查看.
'''


logdate=dt.strftime("%Y%m%d",dt.localtime())
basicloadlog="E:/logs/loaddatalog/loadbasicstockdata"+logdate+".txt";
fileop=open(basicloadlog, mode='w')
fileop.write("start time is :"+start_date_formate)

stock_code_res=get_stock_code();
total_num=len(stock_code_res);
tempnum=1;
for stock_code in stock_code_res:
    #startstr="stock code is:"+stock_code[0]+"start count is:"+is_empty_table(stock_code[0])+"there are:"+total_num+" task"+" this is the number:"+tempnum,"finished:"+tempnum/total_num+"\n"
    startstr=["stock code is:",stock_code[0],"start count is:",'%d' % is_empty_table(stock_code[0]),"there are:",'%d' % total_num," task"," this is the number:",'%d' % tempnum,"finished:",'%d' % (tempnum/total_num),"\n"]
    fileop.writelines(startstr)
    print("stock code is:",stock_code[0],"start count is:",is_empty_table(stock_code[0]),"there are:",total_num," task"," this is the number:",tempnum,"finished:",tempnum/total_num)
    inital_stock_data(stock_code[0])
    print("stock code is",stock_code[0],"end count is :",is_empty_table(stock_code[0]),"there are:",total_num," task"," this is the number:",tempnum,"finished:",tempnum/total_num)
    tempnum=tempnum+1;

#采用while循环不断检测表格数据。没有拉入的数据重新进行拉取。
#首先进行初始化操作，对股票代码列表

#采用多线程进行抽取数据，如果因为网络出现问，那么将会循环抽取直到数据完全过来。


#inital_data(stock_code_list)
  
'''    
    
    
    
    
#============================================================================================================================
print("===========================task finished,caculate the end time ====================")
end_time=dt.time()
end_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
print("start time is :",end_date_formate," start seconds is :",end_time," the program cost :",end_time-start_time,"seconds")
print("===================================================================================")

'''
fileop.write("end time is :"+end_date_formate)
fileop.close();
'''
#==========================================================================================================================