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


def read_scripts_orders():
    fin=open("schedule_scripts_orders",'rt')
    while True:
        line=fin.read()
        if not line:
            break
        print line
        (status, output)=commands.getstatusoutput(line)
        print status

if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------main 函数测试-----------------------------")
    read_scripts_orders()
