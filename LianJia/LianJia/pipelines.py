# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from twisted.enterprise import adbapi


class LianjiaPipeline(object):
    def __init__(self, db_config, tables):
        self.dbpool = None
        self.__db_config = db_config
        self.__tables = tables
        self.__db_config['cursorclass'] = pymysql.cursors.DictCursor

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBCONFIG'], crawler.settings['TABLES'])

    def process_item(self, item, spider):
        self.dbpool.runInteraction(self.do_insert, item)
        return item

    def do_insert(self, cursor, item):
        cursor.execute(item.create_sql(self.__tables.get('data_table')))

    def open_spider(self, spider):
        self.dbpool = adbapi.ConnectionPool('pymysql', **self.__db_config)


class CommunityPipeline(object):
    def __init__(self, db_config, tables):
        self.dbpool = None
        self.__db_config = db_config
        self.__tables = tables
        self.__db_config['cursorclass'] = pymysql.cursors.DictCursor

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBCONFIG'], crawler.settings['TABLES'])

    def process_item(self, item, spider):
        self.dbpool.runInteraction(self.do_insert, item)
        return item

    def do_insert(self, cursor, item):
        cursor.execute(item.get_sql(self.__tables.get('data_table')))

    def open_spider(self, spider):
        self.dbpool = adbapi.ConnectionPool('pymysql', **self.__db_config)
