'''
Created on 2016年1月7日
本文件主要是整合数据下载的脚本，
提供初次初始化，增量加载等功能
@author: Administrator
'''
from loaddata.loadcompanydata import load_company_basic_info,\
    load_company_industry_info
from loaddata.loadstockdata import load_stock_trade_data_order
def load_company_data():
    #加载公司基本信息
    load_company_basic_info()
    #加载公司分类信息
    load_company_industry_info()
def load_trade_data():
    load_stock_trade_data_order()
 
load_company_data()   