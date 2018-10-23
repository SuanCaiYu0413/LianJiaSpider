# *_*coding:utf-8 *_*
from scrapy.spiders import Spider
from scrapy.loader import ItemLoader
from scrapy import Request
import re

from LianJia.items import CommunityspidersItem


class LianJiaSpider(Spider):
    name = 'lianjiacommunity'
    cunzai_url = []
    start_urls = ['https://cd.lianjia.com/xiaoqu/']
    base_url = 'https://cd.lianjia.com'
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'LianJia.middlewares.MyUserAgentMiddleware': 543,
            'LianJia.middlewares.DuplicateemovalRUrlLianJiaZuFang': 543,
        },
        'TABLES': {
            # 数据表名
            'data_table': 'Community_lianjia'
        },
        'ITEM_PIPELINES': {
            'LianJia.pipelines.CommunityPipeline': 300,
        },
        'PAGES': 19
    }

    def parse(self, response):
        divs = response.css('div.position dd div')
        if divs and len(divs) == 2:
            regions = divs[1].css('a::attr(href)').extract()
            for region in regions:
                yield Request(url='{}{}'.format(self.base_url, region))
        if divs and len(divs) == 3:
            blocks = divs[2].css('a::attr(href)').extract()
            for block in blocks:
                for url in ['{}{}pg{}/'.format(self.base_url, block, x) for x in
                            range(1, self.custom_settings.get('PAGES', 18))]:
                    yield Request(url=url, callback=self.parse1)

    def parse1(self, response):
        listContent = response.css('.listContent li')
        for Content in listContent:
            l = ItemLoader(CommunityspidersItem(), response)
            next_page = Content.css('a.img::attr(href)').extract_first()
            l.add_value('name', Content.css('.info .title a::text').extract_first())
            l.add_value('region_plate', '|'.join(Content.css('.positionInfo a::text').extract()))
            yield Request(url=next_page, callback=self.parse_detail, meta={'item': l.load_item()})

    def parse_detail(self, response):
        l = ItemLoader(response.meta.get('item'), response)
        l.add_value('url', response.url)
        xiaoquInfoItems = response.css('.xiaoquInfoItem')
        for xiaoquInfoItem in xiaoquInfoItems:
            key = xiaoquInfoItem.css('.xiaoquInfoLabel::text').extract_first().strip()
            val = xiaoquInfoItem.css('.xiaoquInfoContent::text').extract_first().strip()
            if key.find(u'建筑年代') != -1:
                l.add_value('year', val)
            elif key.find(u'建筑类型') != -1:
                l.add_value('type', val)
            elif key.find(u'物业费用') != -1:
                l.add_value('wuyeprice', val)
            elif key.find(u'物业公司') != -1:
                l.add_value('wuyecompany', val)
            elif key.find(u'开发商') != -1:
                l.add_value('developers', val)
            elif key.find(u'楼栋总数') != -1:
                l.add_value('beam', val)
            elif key.find(u'房屋总数') != -1:
                l.add_value('room', val)
            elif key.find(u'附近门店') != -1:
                l.add_value('nearby', val)
        lng_lat = re.findall(r"resblockPosition:'(.+)',", response.text)
        if not lng_lat == []:
            l.add_value('lng_lat', lng_lat[0])
        yield l.load_item()
