# -*- coding: utf-8 -*-
import time
import json
from datetime import datetime
from dateutil import parser
from flask import Flask
from flask import jsonify
from flask import request
import pymysql
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


app = Flask(__name__)

MONGO_HOST = '106.14.176.62'
MONGO_PORT = 27017
MONGO_DB = "zhejiang"
MONGO_COLL = "sheng"
MYSQL_HOST = "120.55.45.141"
MYSQL_USER = 'hzyg2018'
MYSQL_PASSWORD = 'hzyq@..2018'
MYSQL_DB = "bfhunt"

executor = ThreadPoolExecutor(10)    # 同时处理的最大线程数


@app.route('/', methods=['GET'])
def index():
    return "Hello，欢迎访问API！"


@app.route('/company', methods=['POST'])
def add_user():
    # name = request.json['name']
    data = json.loads(request.get_data())
    name = data['name']
    output = {'result': data, 'status': 1}
    executor.submit(input_company, name)
    return jsonify(output)


@app.route('/inspection', methods=['POST'])
def add_inspection():
    # data = request.json
    data = json.loads(request.get_data())
    output = {'result': data, 'status': 1}
    executor.submit(insert_inspection, data)
    return jsonify(output)


def input_company(name):
    # print("1:'%s'" % name)
    mt = Relation()
    mt.open_sql('bfhunt', 'zhejiang', 'sheng')
    mt.input_sql(name)
    mt.close_sql()


def insert_inspection(data):
    data['stampDateTime'] = datetime.now()
    data['addressByRegionId'] = zoning(data['addressBy'])
    data['addressRegionId'] = zoning(data['address'])
    data['fl'] = '/'
    data['rwly'] = 0
    data['id'] = '/'
    data['inspectionUnit'] = '/'
    data['newsDetailType'] = 0
    data['sampleOrderNumber'] = '/'
    data['status'] = 1
    data['statusEnumValue'] = '正常'
    data['transId'] = '/'
    data['approvalNumber'] = '/'
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    mo_db = client[MONGO_DB]
    coll = mo_db[MONGO_COLL]
    coll.insert_one(data)
    obj = ElasticObj("106.14.176.62:27017", "zhejiang", "sheng", "zhejiang", "sheng", "http://47.98.210.22:9200")
    obj.create_index()
    obj.bulk_Index_Data()
    ra1 = RelationAll()
    ra1.open_sql('bfhunt', 'zhejiang', 'sheng')
    ra1.input_sql(data['corpName'])
    ra1.close_sql()
    ra2 = RelationAll()
    ra2.open_sql('bfhunt', 'zhejiang', 'sheng')
    ra2.input_sql(data['corpNameBy'])
    ra2.close_sql()
    re1 = Relation()
    re1.open_sql('bfhunt', 'zhejiang', 'sheng')
    re1.input_sql(data['corpName'])
    re1.close_sql()
    re2 = Relation()
    re2.open_sql('bfhunt', 'zhejiang', 'sheng')
    re2.input_sql(data['corpNameBy'])
    re2.close_sql()
    print("ok")


def zoning(str):
    link = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB)
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


def str_times(st):
    stp = time.strptime(st, '%Y.%m.%d')
    return time.strftime("%Y-%m-%d", stp)


def str_time(st):
    return parser.parse(st)


class ElasticObj:
    def __init__(self, mongo_url, mongo_db, collection, index_name, index_type, es_ip):
        """
        :param index_name: 索引名称
        :param index_type: 索引类型
        """
        self.client = MongoClient(mongo_url)
        self.db_mongo = self.client[mongo_db]
        self.db_coll = self.db_mongo[collection]
        self.index_name = index_name
        self.index_type = index_type
        # 无用户名密码状态
        self.es = Elasticsearch(es_ip)
        # 用户名密码状态
        # self.es = Elasticsearch([ip],http_auth=('elastic', 'password'),port=9200)

    def create_index(self):
        """
        创建索引,创建索引名称为zhejiang，类型为sheng的索引
        :param ex: Elasticsearch对象
        :return:
        """
        # 创建映射
        _index_mappings = {
            "mappings": {
                self.index_type: {
                    "properties": {
                        "stampDateTime": {
                            "type": "date",
                        },
                        "commodityName": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "corpNameBy": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "address_by": {
                            "type": "text",
                            # "analyzer": "ik_max_word",
                            # "search_analyzer": "ik_max_word"
                        },
                        "addressByRegionId": {
                            "type": "integer"
                        },
                        "corpName": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "address": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "addressRegionId": {
                            "type": "integer"
                        },
                        "createDate": {
                            "type": "date",
                        },
                        # "fl": {
                        #     "type": "text",
                        #     "analyzer": "ik_max_word",
                        #     "search_analyzer": "ik_max_word"
                        # },
                        "flId": {
                            "type": "integer"
                        },
                        "ggh": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "ggrq": {
                            "type": "date",
                        },
                        # "rwly": {
                        #     "type": "text",
                        #     "analyzer": "ik_max_word",
                        #     "search_analyzer": "ik_max_word"
                        # },
                        "rwly_id": {
                            "type": "integer"
                        },
                        # "id": {
                        #     "type": "text",
                        # },
                        # "inspectionUnit": {
                        #     "type": "text",
                        #     "analyzer": "ik_max_word",
                        #     "search_analyzer": "ik_max_word"
                        # },
                        "model": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        # "newsDetailType": {
                        #     "type": "integer",
                        # },
                        "newsDetailTypeId": {
                            "type": "integer"
                        },
                        "note": {
                            "type": "text",
                        },
                        "productionDate": {
                            "type": "date",
                        },
                        # "sampleOrderNumber": {
                        #     "type": "text",
                        # },
                        # "status": {
                        #     "type": "integer",
                        # },
                        # "statusEnumValue": {
                        #     "type": "text",
                        # },
                        "trademark": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        # "transId": {
                        #     "type": "text",
                        # },
                        "unqualifiedItem": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "checkResult": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "standardValue": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        # "approvalNumber": {
                        #     "type": "text",
                        # },
                        "batchNumber": {
                            "type": "text",
                        }
                    }
                }

            }
        }
        if self.es.indices.exists(index=self.index_name) is not True:
            res = self.es.indices.create(index=self.index_name, body=_index_mappings)
            print(res)

    def bulk_Index_Data(self):
        """
        用bulk将批量数据存储到es
        :return:
        """

        ACTIONS = []
        i = 1
        self.mongoRecordRes = self.db_coll.find()
        for record in self.mongoRecordRes:
            print(i)
            # print(record)
            if "addressBy" in record:
                action = {
                    "_index": self.index_name,
                    "_type": self.index_type,
                    "_id": str(record.pop('_id')),
                    "_source": {
                        "stampDateTime": record["stampDateTime"],
                        "commodityName": record["commodityName"],
                        "corpNameBy": record["corpNameBy"],
                        "address_by": record["addressBy"],
                        "addressByRegionId": record["addressByRegionId"],
                        "corpName": record["corpName"],
                        "address": record["address"],
                        "addressRegionId": record["addressRegionId"],
                        "createDate": record["createDate"],
                        # "fl": record["fl"],
                        "flId": record["flId"],
                        "ggh": record["ggh"],
                        "ggrq": record["ggrq"],
                        # "rwly": record["rwly"],
                        "rwly_id": record["rwly_id"],
                        # "id": record["id"],
                        # "inspectionUnit": record["inspectionUnit"],
                        "model": record["model"],
                        # "newsDetailType": record["newsDetailType"],
                        "newsDetailTypeId": record["newsDetailTypeId"],
                        "note": record["note"],
                        "productionDate": record["productionDate"],
                        # "sampleOrderNumber": record["sampleOrderNumber"],
                        # "status": record["status"],
                        # "statusEnumValue": record["statusEnumValue"],
                        "trademark": record["trademark"],
                        # "transId": record["transId"],
                        "unqualifiedItem": record["unqualifiedItem"],
                        "checkResult": record["checkResult"],
                        "standardValue": record["standardValue"],
                        # "approvalNumber": record["approvalNumber"],
                        "batchNumber": record["batchNumber"]}
                }
                print(action)
                i += 1
                ACTIONS.append(action)
            # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=self.index_name, raise_on_error=True)
        print('Performed %d actions' % success)

    @staticmethod
    def time_process(st):
        try:
            if "/" in st:
                return datetime.now()
        except:
            pass

    @staticmethod
    def address_by(st):
        try:
            return st
        except KeyError as e:
            pass


class RelationAll(object):
    def __init__(self):
        self.mysql_host = "120.55.45.141"
        self.mysql_user = 'hzyg2018'
        self.mysql_password = 'hzyq@..2018'
        # self.mysql_host = "192.168.10.121"
        # self.mysql_user = 'hzyg'
        # self.mysql_password = '@hzyq20180426..'
        self.MONGO_HOST = '106.14.176.62'
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

    def input_sql(self, name):
        detail_list = self.coll.find({'$or': [{'corpName': name}, {'corpNameBy': name}]})
        for detail in detail_list:
            # print(detail)
            inspection_id = detail['_id']
            product_name = detail['commodityName']
            produce_name = detail['corpName']
            seller_name = detail['corpNameBy']
            security_results = detail['newsDetailTypeId']
            source = detail['rwly_id']
            data_type = detail['flId']
            status = detail['status']
            notice_date = detail['ggrq']
            if detail['addressByRegionId'] == detail['addressRegionId']:
                supervise_id = detail['addressByRegionId']
                sql = """INSERT INTO organization_inspection_all(inspection_id, supervise_id, status, security_results, source, data_type, product_name, organization_name, casual_organization_name, notice_date) VALUES("%s","%d", "%d", "%d","%d", "%d", "%s", "%s", "%s", "%s")"""  % (inspection_id, supervise_id, status, security_results, source, data_type, product_name, produce_name, seller_name, notice_date)
                self.cursor.execute(sql)
                self.link.commit()
            else:
                supervise_id1 = detail['addressByRegionId']
                supervise_id2 = detail['addressRegionId']
                sql1 = """INSERT INTO organization_inspection_all(inspection_id, supervise_id, status, security_results, source, data_type, product_name, organization_name, casual_organization_name, notice_date) VALUES("%s","%d", "%d", "%d","%d", "%d", "%s", "%s", "%s", "%s")"""  % (inspection_id, supervise_id1, status, security_results, source, data_type, product_name, produce_name, seller_name, notice_date)
                self.cursor.execute(sql1)
                self.link.commit()
                sql2 = """INSERT INTO organization_inspection_all(inspection_id, supervise_id, status, security_results, source, data_type, product_name, organization_name, casual_organization_name, notice_date) VALUES("%s","%d", "%d", "%d","%d", "%d", "%s", "%s", "%s", "%s")"""  % (inspection_id, supervise_id2, status, security_results, source, data_type, product_name, produce_name, seller_name, notice_date)
                self.cursor.execute(sql2)
                self.link.commit()

    def close_sql(self):
        self.link.close()


class Relation(object):
    def __init__(self):
        self.mysql_host = "120.55.45.141"
        self.mysql_user = 'hzyg2018'
        self.mysql_password = 'hzyq@..2018'
        # self.mysql_host = "192.168.10.121"
        # self.mysql_user = 'hzyg'
        # self.mysql_password = '@hzyq20180426..'
        self.MONGO_HOST = '106.14.176.62'
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

    def input_sql(self, name):
        print("2:'%s'" % name)
        detail_list = self.coll.find({'$or': [{'corpName': name}, {'corpNameBy': name}]})
        for detail in detail_list:
            print(detail)
            inspection_id = detail['_id']
            product_name = detail['commodityName']
            produce_name = detail['corpName']
            seller_name = detail['corpNameBy']
            security_results = detail['newsDetailTypeId']
            source = detail['rwly_id']
            data_type = detail['flId']
            status = detail['status']
            notice_date = detail['ggrq']
            if produce_name and produce_name != '/':
                sql = "select id from sys_organization where name='%s'" % produce_name
                # print(sql)
                self.cursor.execute(sql)
                produce_id = self.cursor.fetchone()
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
                    # print(inspection_id, produce_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, produce_name, seller_name)
                    sql = """INSERT INTO organization_inspection_relation(inspection_id, producer_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, organization_name, casual_organization_name) VALUES("%s","%d", "%d", "%d","%d", "%d", "%d", "%d", "%s", "%s", "%s", "%s")"""  % (inspection_id, produce_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, produce_name, seller_name)
                    self.cursor.execute(sql)
                    self.link.commit()
                else:
                    supervise_id1 = detail['addressByRegionId']
                    supervise_id2 = detail['addressRegionId']
                    # print(inspection_id, produce_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, produce_name, seller_name)

                    sql1 = """INSERT INTO organization_inspection_relation(inspection_id, producer_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, organization_name, casual_organization_name) VALUES("%s","%d", "%d", "%d","%d", "%d", "%d", "%d", "%s", "%s", "%s", "%s")"""  % (inspection_id, produce_id, seller_id, supervise_id1, security_results, source, data_type, status, notice_date, product_name, produce_name, seller_name)
                    self.cursor.execute(sql1)
                    self.link.commit()
                    # print(inspection_id, produce_id, seller_id, supervise_id2, security_results, source, data_type, status, notice_date, product_name, produce_name, seller_name)
                    sql2 = """INSERT INTO organization_inspection_relation(inspection_id, producer_id, seller_id, supervise_id, security_results, source, data_type, status, notice_date, product_name, organization_name, casual_organization_name) VALUES("%s","%d", "%d", "%d","%d", "%d", "%d", "%d", "%s", "%s", "%s", "%s")""" % (inspection_id, produce_id, seller_id, supervise_id2, security_results, source, data_type, status, notice_date, product_name, produce_name, seller_name)
                    self.cursor.execute(sql2)
                    self.link.commit()

    def close_sql(self):
        self.link.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)
