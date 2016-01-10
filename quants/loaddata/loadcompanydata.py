'''
Created on 2015年11月30日

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
import pandas as ps;

conn_company_classified = pymysql.connect(user='root', passwd='cqychen123!@#',host='localhost', db='companyclassified',charset='utf8')

'''
抓取公司行业等信息
'''
def load_company_industry_info(): 
    #下载加载行业分类数据
    try:
        rs=ts.get_industry_classified()
        sql.write_frame(rs, "company_industry_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载公司行业分类信息出错")
    #下载加载概念分类数据
    try:
        rs=ts.get_concept_classified()
        sql.write_frame(rs, "company_concept_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载公司概念分类信息出错")
    #下载加载地域分类数据
    try:
        rs=ts.get_area_classified()
        sql.write_frame(rs, "company_area_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载公司区域分类信息出错")
    #下载加载中小板分类数据
    try:
        rs=ts.get_sme_classified()
        sql.write_frame(rs, "company_sme_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载中小板分类数据出错")
    #下载加载创业板分类数据
    try:
        rs=ts.get_gem_classified()
        sql.write_frame(rs, "company_gem_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载创业板分类数据出错")
    #下载加载st板分类数据
    try:
        rs=ts.get_st_classified()
        sql.write_frame(rs, "company_st_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载st板分类数据出错")
    #下载加载沪深300板分类数据
    try:
        rs=ts.get_hs300s()
        sql.write_frame(rs, "company_hs300_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载加载沪深300板分类数据出错")
    #下载加载上证50板分类数据
    try:
        rs=ts.get_sz50s()
        sql.write_frame(rs, "company_sz50_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载加载上证50板分类数据出错")
    #下载加载中证500板分类数据
    try:
        rs=ts.get_zz500s()
        sql.write_frame(rs, "company_zz500_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载加载中证500板分类数据出错")
    #下载加载终止上市分类数据
    try:
        rs=ts.get_terminated()
        sql.write_frame(rs, "company_terminated_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载加载终止上市分类数据出错")
    #下载加载暂停上市分类数据
    try:
        rs=ts.get_suspended()
        sql.write_frame(rs, "company_suspended_classified", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("下载加载暂停上市分类数据出错")
'''
抓取公司基本信息,每股收益等情况 
'''
def load_company_basic_info():
    #下载公司基本信息，包括股票代码、pe、市盈率等数据
    try:
        rs=ts.get_stock_basics()
        sql.write_frame(rs, "company_basic_info", con=conn_company_classified , flavor='mysql', if_exists='replace',index=True)
    except:
        print("公司基本信息数据出错")