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
import sys
sys.path.append('../') #添加配置文件
from config import  *

'''
抓取公司行业等信息
'''
def load_company_industry_info(): 
    #下载加载行业分类数据
    try:
        rs=ts.get_industry_classified()
        pd.DataFrame.to_sql(rs, "company_industry_classified", con=conn_stock_company , flavor='mysql', if_exists='replace',index=True)
        print("下载公司行业分类信息ok")
    except:
        print("下载公司行业分类信息出错")
    #下载加载概念分类数据
    try:
        rs=ts.get_concept_classified()
        pd.DataFrame.to_sql(rs, "company_concept_classified", con=conn_stock_company, flavor='mysql', if_exists='replace',index=True)
        print("载公司概念分类信息ok")
    except:
        print("下载公司概念分类信息出错")
    #下载加载地域分类数据
    try:
        rs=ts.get_area_classified()
        pd.DataFrame.to_sql(rs, "company_area_classified", con=conn_stock_company, flavor='mysql', if_exists='replace',index=True)
        print("下载公司区域分类信息ok")
    except:
        print("下载公司区域分类信息出错")
    #下载加载中小板分类数据
    try:
        rs=ts.get_sme_classified()
        pd.DataFrame.to_sql(rs, "company_sme_classified", con=conn_stock_company, flavor='mysql', if_exists='replace',index=True)
        print("下载中小板分类数据ok")
    except:
        print("下载中小板分类数据出错")
    #下载加载创业板分类数据
    try:
        rs=ts.get_gem_classified()
        pd.DataFrame.to_sql(rs, "company_gem_classified", con=conn_stock_company, flavor='mysql', if_exists='replace',index=True)
        print("下载创业板分类数据ok")
    except:
        print("下载创业板分类数据出错")
    #下载加载st板分类数据
    try:
        rs=ts.get_st_classified()
        pd.DataFrame.to_sql(rs, "company_st_classified", con=conn_stock_company, flavor='mysql', if_exists='replace',index=True)
        print("下载st板分类数据ok")
    except:
        print("下载st板分类数据出错")
    #下载加载沪深300板分类数据
    try:
        rs=ts.get_hs300s()
        pd.DataFrame.to_sql(rs, "company_hs300_classified", con=conn_stock_company , flavor='mysql', if_exists='replace',index=True)
        print("下载加载沪深300板分类数据ok")
    except:
        print("下载加载沪深300板分类数据出错")
    #下载加载上证50板分类数据
    try:
        rs=ts.get_sz50s()
        pd.DataFrame.to_sql(rs, "company_sz50_classified", con=conn_stock_company, flavor='mysql', if_exists='replace',index=True)
        print("下载加载上证50板分类数据ok")
    except:
        print("下载加载上证50板分类数据出错")
    #下载加载中证500板分类数据
    try:
        rs=ts.get_zz500s()
        pd.DataFrame.to_sql(rs, "company_zz500_classified", con=conn_stock_company , flavor='mysql', if_exists='replace',index=True)
        print("下载加载中证500板分类数据ok")
    except:
        print("下载加载中证500板分类数据出错")
    #下载加载终止上市分类数据
    try:
        rs=ts.get_terminated()
        pd.DataFrame.to_sql(rs, "company_terminated_classified", con=conn_stock_company, flavor='mysql', if_exists='replace',index=True)
        print("下载加载终止上市分类数据ok")
    except:
        print("下载加载终止上市分类数据出错")
    #下载加载暂停上市分类数据
    try:
        rs=ts.get_suspended()
        pd.DataFrame.to_sql(rs, "company_suspended_classified", con=conn_stock_company, flavor='mysql', if_exists='replace',index=True)
        print("下载加载暂停上市分类数据ok")
    except:
        print("下载加载暂停上市分类数据出错")
'''
抓取公司基本信息,每股收益等情况 
'''
def load_company_basic_info():
    #下载公司基本信息，包括股票代码、pe、市盈率等数据
    try:
        rs=ts.get_stock_basics()
        pd.DataFrame.to_sql(rs, "company_basic_info", con=conn_stock_company , flavor='mysql', if_exists='replace',index=True)
        print("公司基本信息数据ok")
    except:
        print("公司基本信息数据出错")

if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载公司分类信息开始-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='stock_company'
    charset='utf8'
    conn_stock_company = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始--------------------------------
    load_company_basic_info()
    load_company_industry_info()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
