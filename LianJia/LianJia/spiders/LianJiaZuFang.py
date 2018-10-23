# *_*coding:utf-8 *_*
import json
import re

import time
from scrapy import Request
from scrapy.loader import ItemLoader
from scrapy.spiders import Spider

from LianJia.items import LianJiaItem


class LianJiaZuFang(Spider):
    name = 'lianjiazufang'

    base_url = 'https://cd.lianjia.com'
    start_urls = ['https://cd.lianjia.com/zufang/']
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'LianJia.middlewares.MyUserAgentMiddleware': 543,
            'LianJia.middlewares.DuplicateemovalRUrlLianJiaZuFang': 543,
        },
        'ITEM_PIPELINES': {
            'LianJia.pipelines.LianjiaPipeline': 300,
        },
        'TABLES': {
            # 数据表名
            'data_table': 'LianJia_v2'
        },
        # 每个版块抓取页数
        'PAGES': 45
    }

    def parse(self, response):
        regions = response.css('#filter-options dl.dl-lst:nth-child(1)')
        region_urls = regions.css('.option-list a::attr(href)').extract()
        for url in region_urls:
            yield Request('{}{}'.format(self.base_url, url))
        sub_urls = regions.css('.sub-option-list a::attr(href)').extract()
        for url in sub_urls:
            for i in range(1, self.custom_settings.get('PAGES', 45)):
                yield Request('{}{}pg{}/'.format(self.base_url, url, i), callback=self.parse2)

    def parse2(self, response):
        for detail in response.css('#house-lst li'):
            try:
                yield Request(url=detail.css('h2 a::attr(href)').extract_first(), callback=self.parse_detail)
            except Exception as e:
                pass

    def parse_detail(self, response):
        l = ItemLoader(LianJiaItem(), response)
        l.add_value('insertTime', time.strftime('%Y-%m-%d', time.localtime(time.time())))
        l.add_css('title', '.title h1::text')
        l.add_css('price', '.price .total::text')
        l.add_value('url', response.url)
        tags = response.css('.zf-tag ul li.tags::text').extract()
        tags = [x.replace(' ', '').replace('\n', '') for x in tags]
        l.add_value('tags', '|'.join(tags))
        for li in response.css('.introContent div.content ul li'):
            kv = li.css('::text').extract()
            if len(kv) >= 2:
                key = kv[0].strip()
                val = kv[1].strip()
                if key.find(u'租赁') != -1:
                    l.add_value('lease', val)
                elif key.find(u'付款') != -1:
                    l.add_value('pay', val)
                elif key.find(u'房屋') != -1:
                    l.add_value('cur', val)
                elif key.find(u'供暖') != -1:
                    l.add_value('warm', val)
        for ptag in response.css('.overview .zf-room p'):
            kv = ptag.css('::text').extract()
            if len(kv) >= 2:
                key = kv[0].strip()
                val = '|'.join([x.strip() for x in kv[1:]])
                if key.find(u'面积') != -1:
                    l.add_value('area', val)
                elif key.find(u'户型') != -1:
                    l.add_value('houseType', val)
                elif key.find(u'楼层') != -1:
                    l.add_value('floor', val)
                elif key.find(u'地铁') != -1:
                    l.add_value('traffic', val)
                elif key.find(u'小区') != -1:
                    l.add_value('community', val)
                elif key.find(u'位置') != -1:
                    l.add_value('district', val)
                elif key.find(u'时间') != -1:
                    l.add_value('sendTime', val)
                elif key.find(u'朝向') != -1:
                    l.add_value('toward', val)
        houseNumber = re.findall(r"houseId:'(\d+)',", response.text)
        if houseNumber != []:
            l.add_value('houseNumber', houseNumber[0])
        comNumber = re.findall(r"resblockId:'(\d+)',", response.text)
        if comNumber != []:
            l.add_value('communityNumber', comNumber[0])
        item = l.load_item()
        if item.get('houseNumber', '') == '' or item.get('communityNumber', '') == '':
            yield item
        else:
            url = 'https://cd.lianjia.com/zufang/housestat?hid={}&rid={}'.format(item.get('houseNumber', [1])[0],
                                                                                 item.get('communityNumber', [1])[0])
            yield Request(url=url, callback=self.parser_exrtinfo, meta={'item': l.load_item()})

    def parser_exrtinfo(self, response):
        item = response.meta['item']
        l = ItemLoader(item, response)
        json_obj = json.loads(response.text)
        looks = json_obj.get('data', {}).get('seeRecord', {}).get('totalCnt', '')
        lat_lnt = json_obj.get('data', {}).get('resblockPosition', '')
        l.add_value('looks', looks)
        l.add_value('lat_long', lat_lnt)
        yield l.load_item()
