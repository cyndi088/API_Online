# -*- coding: utf-8 -*-
import pymysql
from pymongo import MongoClient


class MiddleTable(object):

    def __init__(self):
        # self.mysql_host = "202.91.244.43"
        # self.mysql_port = 8306
        # self.mysql_user = 'root'
        # self.mysql_password = 'woowoxdu3742kdkdkdx'

        self.mysql_host = "120.55.45.141"
        self.mysql_port = 3306
        self.mysql_user = 'hzyg2018'
        self.mysql_password = 'hzyq@..2018'

        # self.mysql_host = "192.168.10.121"
        # self.mysql_port = 3306
        # self.mysql_user = 'hzyg'
        # self.mysql_password = '@hzyq20180426..'
        self.MONGO_HOST = '106.14.176.62'
        self.MONGO_PORT = 27017
        # self.MONGO_USER = ''
        # self.PSW = ''

    def open_sql(self, ms_db, mo_db, mo_coll):
        self.link = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, password=self.mysql_password, db=ms_db)
        self.link.set_charset('utf8')
        self.cursor = self.link.cursor()
        self.client = MongoClient(host=self.MONGO_HOST, port=self.MONGO_PORT)
        self.mo_db = self.client[mo_db]
        self.coll = self.mo_db[mo_coll]

    def input_sql(self):
        producer_list = self.coll.distinct('corpName', {})
        seller_list = self.coll.distinct('corpNameBy', {})
        company_list = list(set(producer_list + seller_list))
        for name in company_list:
            if name and name != '/':
                detail_list = self.coll.find({'$or': [{'corpName': name}, {'corpNameBy': name}]})
                for detail in detail_list:
                    # print(detail)
                    inspection_id = detail['_id']
                    product_name = self.food_name(detail['commodityName'])
                    produce_name = detail['corpName']
                    seller_name = detail['corpNameBy']
                    security_results = detail['newsDetailTypeId']
                    source = detail['rwly_id']
                    data_type = detail['flId']
                    # status = detail['status']
                    status = 1
                    notice_date = detail['ggrq']
                    if produce_name and produce_name != '/':
                        sql = "select id from sys_organization where name='%s'" % produce_name
                        self.cursor.execute(sql)
                        produce_id = self.cursor.fetchone()
                        # produce_id = self.get_id(sql)
                        if produce_id:
                            produce_id = produce_id[0]
                        else:
                            produce_id = 0
                    else:
                        produce_id = 0
                    if seller_name and seller_name != '/':
                        sql = "select id from sys_organization where name='%s'" % seller_name
                        self.cursor.execute(sql)
                        seller_id = self.cursor.fetchone()
                        # seller_id = self.get_id(sql)
                        if seller_id:
                            seller_id = seller_id[0]
                        else:
                            seller_id = 0
                    else:
                        seller_id = 0
                    if produce_id == 0 and seller_id == 0:
                        continue
                    else:
                        if detail['addressByRegionId'] == detail['addressRegionId']:
                            supervise_id = detail['addressByRegionId']
                            sql = """INSERT INTO organization_inspection_relation(inspection_id, producer_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, organization_name, casual_organization_name) VALUES("%s","%d", "%d", "%d","%d", "%d", "%d", "%d", "%s", "%s", "%s", "%s")"""  % (inspection_id, produce_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, produce_name, seller_name)
                            self.cursor.execute(sql)
                            self.link.commit()
                        else:
                            supervise_id1 = detail['addressByRegionId']
                            supervise_id2 = detail['addressRegionId']
                            sql1 = """INSERT INTO organization_inspection_relation(inspection_id, producer_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, organization_name, casual_organization_name) VALUES("%s","%d", "%d", "%d","%d", "%d", "%d", "%d", "%s", "%s", "%s", "%s")"""  % (inspection_id, produce_id, seller_id, supervise_id1, security_results, source, data_type, status, notice_date, product_name, produce_name, seller_name)
                            self.cursor.execute(sql1)
                            self.link.commit()
                            sql2 = """INSERT INTO organization_inspection_relation(inspection_id, producer_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, organization_name, casual_organization_name) VALUES("%s","%d", "%d", "%d","%d", "%d", "%d", "%d", "%s", "%s", "%s", "%s")""" % (inspection_id, produce_id, seller_id, supervise_id2, security_results, source, data_type, status, notice_date, product_name, produce_name, seller_name)
                            self.cursor.execute(sql2)
                            self.link.commit()

    def close_sql(self):
        self.link.close()

    # @staticmethod
    # def get_id(sql):
    #     mysql_host = "202.91.244.43"
    #     mysql_port = 8306
    #     mysql_user = 'root'
    #     mysql_password = 'woowoxdu3742kdkdkdx'
    #     mysql_db = 'devyq'
    #     link = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, db=mysql_db)
    #     link.set_charset('utf8')
    #     cursor = link.cursor()
    #     cursor.execute(sql)
    #     company_id = cursor.fetchone()
    #     return company_id

    @staticmethod
    def food_name(name):
        new_name = name.replace('"', '')
        return new_name

mt = MiddleTable()
mt.open_sql('bfhunt', 'zhejiang', 'sheng')
mt.input_sql()
mt.close_sql()



