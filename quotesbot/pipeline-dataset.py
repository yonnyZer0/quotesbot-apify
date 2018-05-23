# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from ./py_apify import ApifyClient
import os, sys

class QuotesbotToDataset(object):
    
    def __init__(self):
        self.apify_client = ApifyClient()
        self.items_to_push = []
        self.chunk_size = 50  

    def process_item(self, item, spider):
        self.items_to_push.append( item )
        if len( self.items_to_push ) == self.chunk_size:
            self.pushData_chunk() 
        return item
    
    def close_spider(self, spider):
        print('Closing spider... pushing last data.')
        self.pushData_chunk()        
    
    def pushData_chunk(self):
        print('pushing chunk of data...')
        self.apify_client.pushData( {'data': self.items_to_push} )
        self.items_to_push = []
        
        
        
