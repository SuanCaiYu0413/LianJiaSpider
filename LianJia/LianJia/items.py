# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class LianJiaItem(scrapy.Item):
    looks = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    price = scrapy.Field()
    area = scrapy.Field()
    houseType = scrapy.Field()
    floor = scrapy.Field()
    toward = scrapy.Field()
    traffic = scrapy.Field()
    community = scrapy.Field()
    district = scrapy.Field()
    sendTime = scrapy.Field()
    insertTime = scrapy.Field()
    communityNumber = scrapy.Field()
    houseNumber = scrapy.Field()
    lease = scrapy.Field()
    tags = scrapy.Field()
    cur = scrapy.Field()
    warm = scrapy.Field()
    lat_long = scrapy.Field()

    def create_sql(self, table_name):
        dic = dict(self)

        def produce_insert_sql(dict1, table_name1):
            base_sql = 'insert into `{}` (%s) VALUES (%s)'.format(table_name1)
            keys = dict1.keys()
            vals = []
            for key in keys:
                vals.append(dict1.get(key, [''])[0])
            return base_sql % ('`%s`' % '`,`'.join(keys), "'%s'" % "','".join(vals))

        return produce_insert_sql(dic, table_name)


class CommunityspidersItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    name = scrapy.Field()
    region_plate = scrapy.Field()
    year = scrapy.Field()
    type = scrapy.Field()
    wuyeprice = scrapy.Field()
    wuyecompany = scrapy.Field()
    developers = scrapy.Field()
    beam = scrapy.Field()
    room = scrapy.Field()
    nearby = scrapy.Field()
    url = scrapy.Field()
    lng_lat = scrapy.Field()

    def get_sql(self, table_name):
        sql = u'insert into `{}` (`lng_lat`,`name`,`url`,`region_plate`,`year`,`type`,`wuyeprice`,`wuyecompany`,`developers`,`beam`,`room`,`nearby`) VALUES ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")'.format(
            table_name) % (
                  self.get('lng_lat', [''])[0], self.get('name', [''])[0], self.get('url', [''])[0],
                  self.get('region_plate', [''])[0],
                  self.get('year', [''])[0],
                  self.get('type', [''])[0], self.get('wuyeprice', [''])[0], self.get('wuyecompany', [''])[0],
                  self.get('developers', [''])[0], self.get('beam', [''])[0], self.get('room', [''])[0],
                  self.get('nearby', [''])[0])
        return sql
