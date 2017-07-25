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
import sys
sys.path.append('./') #添加配置文件
from common_function import  *
print "input path is :", sys.argv[1]
path=sys.argv[1]
sql_content=open(path).read()
iphost,user,passwd=get_mysql_conn()
db='ods_data'
charset='utf8'
conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
try:
    run_mysql_cmd(cmd=sql_content,conn=conn)
except:
    print("执行文件报错啦！！！----%s"%path)






