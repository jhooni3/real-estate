#!/usr/bin/env python
# coding: utf-8
import time
import os
import pandas as pd
import schedule

from util import send_msg_to_slack
import xml.etree.ElementTree as ET

SERVICE_KEY = "<service_key>"
HOST = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/"
URLS = {
    'apt-trade': {
        'url': HOST + "getRTMSDataSvcAptTrade?"
        ,'header': "resultCode, resultMsg, 거래금액, 건축년도, 년, 법정동, 아파트, 월, 일, 전용면적, 지번, 지역코드, 층"
    }
    ,'apt-rent': {
        'url': HOST + "getRTMSDataSvcAptRent?"
        ,'header': "resultCode, resultMsg, 건축년도, 년, 법정동, 보증금액, 아파트, 월, 월세금액, 일, 전용면적, 지번, 지역코드, 층"
    }
    , 'office-trade': {
        'url': 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiTrade?'
        # 'url':  HOST + "getRTMSDataSvcOffiTrade?"
        ,'header': "resultCode, resultMsg, 거래금액, 년, 단지, 법정동, 시군구, 월, 일, 전용면적, 지번, 지역코드, 층"
    }
    , 'office-rent': {
        'url': 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiRent?'
        # 'url': HOST + "getRTMSDataSvcOffiRent?" 
        ,'header': "resultCode, resultMsg, 년, 단지, 법정동, 보증금, 시군구, 월, 월세, 일, 전용면적, 지번, 지역코드, 층"
    }
}


def get_data(url, rcode, date):
    import requests
    # url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"
    # url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiRent"
    querystring = {"pageNo":"1","startPage":"1","numOfRows":"99999","pageSize":"10","LAWD_CD":""+rcode+ "","DEAL_YMD":""+date+"","type":"json", "serviceKey":""}
    # print querystring
    headers = {
        'cache-control': "no-cache",
        'postman-token': "e8d4c5d9-9287-549d-b5bc-9cdd60e76e1d"
        }
    response = requests.request("GET", url, headers=headers, params=querystring)
    return response

def get_items(response):
    root = ET.fromstring(response.content)
    item_list = []
    for child in root.find('body').find('items'):
        elements = child.findall('*')
        data = {}
        for element in elements:
            tag = element.tag.strip()
            text = element.text.strip()
            # print tag, text
            data[tag] = text
        item_list.append(data)  
    return item_list

# print root.find('body').find('totalCount').text
# print root.find('body').find('numOfRows').text
# print root.find('body').find('pageNo').text

def get_months(year):
    import datetime
    import dateutil.relativedelta
    now = datetime.datetime.now()
    month = 12
    if year == now.year:
        month = datetime.datetime.now().month - 1
    d = datetime.datetime.strptime(str(year)+str(month), "%Y%m")
    delta = 1
    ymd_list = []
    for idx in range(0, month):
        ymd_list.append(d.strftime('%Y%m'))
        d = d - dateutil.relativedelta.relativedelta(months=delta)
    return ymd_list

def get_result_code_msg(response):
    # print response
    root = ET.fromstring(response.content)
    return root.find('header').find('resultCode').text,  root.find('header').find('resultMsg').text

def get_rcodes():
    f_in = file('road_code.csv', 'r')
    lines = [x[:-1].split(',') for x in f_in.readlines()]
    rcodes = [x[0] for x in lines]
    rcodes = list(set(rcodes))
    return rcodes

def data_exists(dir_path, filename):
    import os
    filelist = os.listdir(dir_path)
    for f in filelist:
        # print f, filename
        if filename in f:
            print filename, 'is already exist'
            return True
    return False                          

def scrap():
    send_msg_to_slack("> start scrap real-estate")
    total_count = 0
    for sale_type in URLS:
        counts = 0
        is_limit = False
        print sale_type, URLS[sale_type]['url']
        url = URLS[sale_type]['url']
        for rcode in get_rcodes():
            print rcode,
            for year in ['2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']:
                print year
                for date in sorted(get_months(year)):
                    dir_path = './data/{}/{}'.format(sale_type, rcode)
                    if not os.path.exists(dir_path):
                        os.makedirs(dir_path)
                    filename = '{}.csv'.format(date)
                    # if date == '201808': break
                    print dir_path, filename
                    if not data_exists(dir_path, filename):
                        response = get_data(url, rcode, date)
                        try:
                            result_code, response_msg = get_result_code_msg(response)
                        except Exception as e:
                            send_msg_to_slack(sale_type+","+rcode+","+date+": " + str(e))
                            # continue
                        if '00' != result_code:
                            is_limit = True
                            send_msg_to_slack(sale_type+","+rcode+","+date+": " + response_msg)
                            break
                        item_list = get_items(response)
                        time.sleep(1)
                        items = pd.DataFrame.from_dict(item_list)
                        # data = items.set_index(u'지역코드').join(df)
                        # data[u'지역코드'] = data.index
                        items['date'] = date  
                        items.to_csv(os.path.join(dir_path, filename), index=False, encoding='utf8') 
                        counts += 1
                        total_count += 1
                    else:
                        print 'already exist', dir_path, date
                if is_limit: break
            if is_limit: break
        send_msg_to_slack("{} # of data: {}".format(sale_type, str(counts)))
    send_msg_to_slack("total # of data: {}".format(str(total_count)))

schedule.every().day.at("08:00").do(scrap)
schedule.every().day.at("19:00").do(scrap)

# scrap()
while True:
    # scrap()
    # job()
    schedule.run_pending()
    time.sleep(1)
