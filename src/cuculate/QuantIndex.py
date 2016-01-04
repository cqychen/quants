'''
Created on 2015年12月21日

@author: Administrator
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
from loaddata.loadstockdata import get_stock_code

conn_get_data = pymysql.connect(user='root', passwd='cqychen882625',host='localhost', db='stock',charset='utf8')
print("===================calculate the quant index================")

def macd_sign(code_name):
    '''
    通过macd获取购买信号
    '''
    get_str="select  * from basic_"+code_name+ " order by `date`";
    rs=sql.read_frame(get_str,conn_get_data);
    ema12=pd.ewma(rs['close'],span=12)
    ema26=pd.ewma(rs['close'],span=26)
    kmacd=ema12-ema26
    kmacd_sign=pd.ewma(kmacd,span=9)
    macdz=(kmacd-kmacd_sign)*2
    
    #通过macd发出信号
    macdlen=len(macdz)
    tempres=pd.Series();
    
    pre_rs=1;
    now_rs=1;  
    sign_buy=True;
    
    for i in range(1,macdlen):  
        if(macdz[i]>macdz[i-1]):
            now_rs=1
            if(now_rs==pre_rs):
                #print(macdz[i],"ture  hold")
                sign_buy=False
            else:     
                sign_buy=True
                #print(macdz[i],"true  buy","getvol:",getvol,"getmartmoney:",getmartmoney,"canbuymoney:",canbuymoney,rs.ix[i])
                
        else:
            now_rs=0
            if(now_rs==pre_rs):
                #print(macdz[i],"false  hold")
                sign_buy=False
            else:
                sign_buy=False
                #print(macdz[i],"false  sell",getvol,"getmartmoney:",getmartmoney,"canbuymoney:",canbuymoney,rs.ix[i])
        pre_rs=now_rs;        
    return sign_buy;




print("=================task start,calculate start time==================")
start_time=dt.time()
start_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
print("start time is :",start_date_formate," start seconds is :",start_time)
print("===================================================================")

logdate=dt.strftime("%Y%m%d",dt.localtime())
basicloadlog="E:/result/macdresult/mackresulttockdata"+logdate+".txt";
fileop=open(basicloadlog, mode='w')
fileop.write("start time is :"+start_date_formate)

stock_code_res=get_stock_code()
for stock_code in stock_code_res:
    if(macd_sign(stock_code[0])):
        print(stock_code[0])
        fileop.writelines(stock_code[0])


#============================================================================================================================
print("===========================task finished,caculate the end time ====================")
end_time=dt.time()
end_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
print("start time is :",end_date_formate," start seconds is :",end_time," the program cost :",end_time-start_time,"seconds")
print("===================================================================================")
fileop.write("end time is :"+end_date_formate)
fileop.close();
#==========================================================================================================================    












