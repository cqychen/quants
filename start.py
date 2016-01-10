'''
Created on 2016年1月4日

@author: cqychen
'''
import time as dt
import acc_loaddata as ld

if __name__ == '__main__':
    '''
    程序运行开始计时
    '''
    print("======================start to go=======================")
    print("=================task start,calculate start time==================")
    start_time=dt.time()
    start_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
    print("start time is :",start_date_formate," start seconds is :",start_time)
    print("===================================================================")
    
    '''
    运行代码区域，调用数据下载接口，计算接口，报表短信推送接口。
    '''
    ld.load_company_data() #加载基本信息数据
    ld.load_trade_data()   #加载公司当天交易数据
    '''
    程序运行结束计时
    '''
    print("===========================task finished,caculate the end time ====================")
    end_time=dt.time()
    end_date_formate=dt.strftime("%Y-%m-%d %H:%M:%S",dt.localtime())
    print("start time is :",end_date_formate," start seconds is :",end_time," the program cost :",end_time-start_time,"seconds")
    print("===================================================================================")
    