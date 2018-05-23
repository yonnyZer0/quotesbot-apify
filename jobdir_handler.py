# -*- coding: utf-8 -*-
#! /usr/bin/env python

from py_apify import ApifyClient
from websocket import create_connection
import os, sys, time, json

class RunHandler(object):
    
    def __init__(self):
        self.apify_client = ApifyClient()
        self.migration = 0
    
    def check_migration_or_restart(self):
        ws_url = self.apify_client.options['APIFY_ACTOR_EVENTS_WS_URL']
        while 1:
            try:
                self.migration = json.loads( ws.recv() )
                print( self.migration )
                #if self.migration:
                    
            except Exception as e:
                print(e)
                ws = create_connection( ws_url )
            time.sleep(1)        
    
    def unwrap_current_run(self):
        binary_data = self.apify_client.keyValueStores.getRecord({'recordKey': 'state_of_the_current_run'})
        print( len( binary_data ) )
        if len( binary_data ):
            current_run_zip = open( 'current_run.zip', 'wb' )
            current_run_zip.write( binary_data )
            current_run_zip.close()
        
        os.system('unzip current_run.zip')
        
    def wrap_current_run(self):
        os.system('sync && zip -R "current_run.zip" "current_run"')
        current_run_zip = open('current_run.zip', 'rb').read()
        self.apify_client.keyValueStores.putRecord({ "recordKey": "state_of_the_current_run", "data": current_run_zip, "contentType": "application/zip" })


handle = Handler()
if '-start' in sys.argv:
    h.unwrap_current_run()
    os.popen("scrapy crawl toscrape-css --set JOBDIR=current_run")
    h.apify_client.check_migration_or_restart()

elif '-restart' in sys.argv:
    h.wrap_current_run()
    os.popen("scrapy crawl toscrape-css --set JOBDIR=current_run")
    h.apify_client.check_migration_or_restart()

else:
    h.apify_client.check_migration_or_restart()
