# -*- coding: utf-8 -*-
import time
from datetime import datetime
import pymysql
from pymongo import MongoClient
from dateutil import parser



class MiddleTable(object):

    def __init__(self):
        self.mysql_host = "192.168.10.121"
        self.mysql_user = 'hzyg'
        self.mysql_password = '@hzyq20180426..'
        # self.mysql_host = "120.55.45.141"
        # self.mysql_user = 'hzyg2018'
        # self.mysql_password = 'hzyq@..2018'
        self.MONGO_HOST = '106.14.176.62'
        # self.MONGO_HOST = "localhost"
        self.MONGO_PORT = 27017
        # self.MONGO_USER = ''
        # self.PSW = ''

    def open_sql(self, ms_db, mo_db, mo_coll):
        self.link = pymysql.connect(self.mysql_host, self.mysql_user, self.mysql_password, ms_db)
        self.link.set_charset('utf8')
        self.cursor = self.link.cursor()
        self.client = MongoClient(host=self.MONGO_HOST, port=self.MONGO_PORT)
        self.mo_db = self.client[mo_db]
        self.coll = self.mo_db[mo_coll]

    def input_sql(self):
        sql = "select * from testing_security"
        self.cursor.execute(sql)
        allData = self.cursor.fetchall()
        for data in allData:
            # print(data)
            item = {}
            if data[3]:
                if data[1]:
                    item["corpName"] = data[1]
                else:
                    item['corpName'] = '/'
                item["stampDateTime"] = datetime.now()
                if data[5]:
                    item["commodityName"] = data[5]
                else:
                    item["commodityName"] = "/"
                if data[3]:
                    item["corpNameBy"] = data[3]
                else:
                    item["corpNameBy"] = "/"
                if data[4]:
                    item["addressBy"] = data[4]
                else:
                    item["addressBy"] = "/"
                item["addressByRegionId"] = self.zoning(data[4])
                if data[2]:
                    item["address"] = data[2]
                else:
                    item["address"] = "/"
                item["addressRegionId"] = self.zoning(data[2])
                if data[14] and "-" in data[14]:
                    item["ggh"] = data[24]
                    item["ggrq"] = data[14]
                    if data[17]:
                        item["trademark"] = data[17]
                    else:
                        item["trademark"] = '/'
                    if data[12]:
                        item["fl"] = data[12]
                        item["flId"] = self.fl(item['fl'])
                    else:
                        item["fl"] = "/"
                        item["flId"] = 82
                elif data[14] == None:
                    item["ggh"] = "/"
                    if data[15]:
                        item["ggrq"] = self.str_time(data[15])
                    else:
                        item["ggrq"] = self.time_format(data[8])
                    item["trademark"] = "/"
                    if data[24]:
                        item["fl"] = data[24]
                        item["flId"] = self.fl(item['fl'])
                    else:
                        item["fl"] = "/"
                        item["flId"] = 82
                else:
                    item["ggh"] = data[14]
                    item["ggrq"] = data[17]
                    if data[7]:
                        item["trademark"] = data[7]
                    else:
                        item["trademark"] = '/'
                    if data[24]:
                        item["fl"] = data[24]
                        item["flId"] = self.fl(item['fl'])
                    else:
                        item["fl"] = "/"
                        item["flId"] = 82
                if data[15]:
                    item["createDate"] = self.str_time(data[15])
                else:
                    if item["ggrq"] == None:
                        item["ggrq"] = datetime.now()
                        item["createDate"] = item["ggrq"]
                    else:
                        item["createDate"] = item["ggrq"]
                if data[9]:
                    item["rwly"] = data[9]
                    item["rwly_id"] = self.rwly(data[9])
                else:
                    item["rwly"] = 0
                    item["rwly_id"] = 524
                item["id"] = "/"
                item["inspectionUnit"] = "/"
                if data[6]:
                    item["model"] = data[6]
                else:
                    item["model"] = "/"
                item["newsDetailType"] = 0
                if data[25]:
                    item["newsDetailTypeId"] = data[25]
                else:
                    item["newsDetailTypeId"] = 0
                item["note"] = "/"
                if data[8]:
                    item["productionDate"] = self.time_format(data[8])
                else:
                    item["productionDate"] = item["createDate"]
                item["sampleOrderNumber"] = "/"
                item["status"] = data[22]
                item["statusEnumValue"] = "正常"
                item["transId"] = "/"
                item["unqualifiedItem"] = data[11]
                item["checkResult"] = "/"
                item["standardValue"] = "/"
                item["approvalNumber"] = "/"
                item["batchNumber"] = "/"
                print(item)
                self.coll.save(item)
            else:
                continue

    def close_sql(self):
        self.link.close()

    @staticmethod
    def zoning(str):
        if str == None or str == "/":
            return 0
        else:
            mysql_host = "192.168.10.121"
            mysql_user = 'hzyg'
            mysql_password = '@hzyq20180426..'
            msyql_db = 'yfhunt'
            link = pymysql.connect(mysql_host, mysql_user, mysql_password, msyql_db)
            link.set_charset('utf8')
            cursor = link.cursor()
            sql1 = "select region_name from region r where r.parent_id in (select r.region_id from region r where r.parent_id=(select r.region_id from region r where r.region_name='浙江省'))"
            cursor.execute(sql1)
            allData1 = cursor.fetchall()
            sql2 = "select region_name from region r where r.parent_id=(select r.region_id from region r where r.region_name='浙江省')"
            cursor.execute(sql2)
            allData2 = cursor.fetchall()
            i = 0
            stp = []
            while i < len(allData1):
                j = 0
                while j < len(allData1[i]):
                    if allData1[i][j][:-1] in str:
                        stp.append(allData1[i][j])
                        sql3 = "select region_id from region r where r.region_name='%s'" % stp[0]
                        cursor.execute(sql3)
                        allData3 = cursor.fetchall()
                        region_id = int(allData3[0][0])
                        return region_id
                        break
                    else:
                        j += 1
                i += 1
            if not stp:
                m = 0
                while m < len(allData2):
                    n = 0
                    while n < len(allData2[m]):
                        if allData2[m][n][:-1] in str:
                            stp.append(allData2[m][n])
                            sql4 = "select region_id from region r where r.region_name='%s'" % stp[0]
                            cursor.execute(sql4)
                            allData4 = cursor.fetchall()
                            region_id = int(allData4[0][0])
                            return region_id
                            break
                        else:
                            n += 1
                    m += 1
            if not stp and "浙江" in str:
                return 12
            elif not stp and "浙江" not in str:
                return 1

    @staticmethod
    def str_time(str):
        return parser.parse(str)

    @staticmethod
    def rwly(num):
        if num == 1:
            return 520
        elif num == 2:
            return 521
        elif num == 3:
            return 522
        elif num == 4:
            return 523
        else:
            return 524

    @staticmethod
    def fl(name):
        if name != None:
            mysql_host = "192.168.10.121"
            mysql_user = 'hzyg'
            mysql_password = '@hzyq20180426..'
            msyql_db = 'yfhunt'
            link = pymysql.connect(mysql_host, mysql_user, mysql_password, msyql_db)
            link.set_charset('utf8')
            cursor = link.cursor()
            sql = "select sys_data_group_id from sys_data_item where key_value='%s'" % name
            cursor.execute(sql)
            allData = cursor.fetchone()
            if allData:
                return allData[0]
            else:
                return 82
        else:
            return 82

    @staticmethod
    def time_format(str):
        if str[:2] == "20":
            if "-" in str:
                try:
                    stp = str[:10]
                    ss1 = parser.parse(stp)
                    return ss1
                except:
                    return datetime.now()
            elif "." in str:
                ls = str.split(".")
                if len(ls) == 3:
                    try:
                        stp = time.strptime(str, '%Y.%m.%d')
                        ss2 = parser.parse(time.strftime("%Y-%m-%d", stp))
                        return ss2
                    except:
                        return datetime.now()
                elif len(ls) == 2:
                    str = ls[0]
                    if len(str) >= 8:
                        stp = str[:4] + "-" + str[4:6] + "-" + str[6:8]
                        try:
                            ss3 = parser.parse(stp)
                            return ss3
                        except:
                            return datetime.now()
                    else:
                        return datetime.now()
                else:
                    return datetime.now()
            elif "/" in str:
                ls = str.split("/")[:3]
                if len(ls) >= 3:
                    try:
                        stp = ls[0] + "-" + ls[1] + "-" + ls[2][:2]
                        ss4 = parser.parse(stp)
                        return ss4
                    except:
                        return datetime.now()
                else:
                    return datetime.now()
            else:
                return datetime.now()
        else:
            return datetime.now()


mt = MiddleTable()
mt.open_sql('yfhunt', 'zhejiang', 'sheng')
mt.input_sql()
mt.close_sql()







