# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from py_apify import ApifyClient
import os

class QuotesbotToDataset(object):
    
    def __init__(self):
        self.apify_client = ApifyClient()
        self.items_to_push = []
        # how often to pushData to dataset and how ofter save state of crawler to kvstore
        self.chunk_size = 10

    def process_item(self, item, spider):
        self.items_to_push.append( item )
        if len( self.items_to_push ) == self.chunk_size:
            self.pushData_chunk()
            self.state_to_kvstore()
             
        return item
    
    def close_spider(self, spider):
        self.pushData_chunk()
    
    def pushData_chunk(self):
        self.apify_client.pushData( {'data': self.items_to_push} )
        self.items_to_push = []
        
    def state_to_kvstore(self):
        print('saving...')
        os.system('sync && zip -9 -r "current_run.zip" "current_run"')
        #print(os.system('ls'))
        current_run_zip = open('current_run.zip', 'rb').read()
        print( str( current_run_zip ) )
        self.apify_client.keyValueStores.putRecord({ "recordKey": "state_of_the_current_run", "data": current_run_zip, "contentType": "application/zip" })
        
    def state_from_kvstore(self):
        print('saving...')
        
        
