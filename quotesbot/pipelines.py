# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from py2_apify import ApifyClient

class QuotesbotPipeline(object):
    
    def def __init__(self, APIFY_TOKEN):
        self.apify_client = ApifyClient()

    def process_item(self, item, spider):
        self.apify_client.pushRecords( {data: item.__dict__['_values']} )
        return item
