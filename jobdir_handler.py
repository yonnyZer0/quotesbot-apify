# -*- coding: utf-8 -*-
#! /usr/bin/env python

from py_apify import ApifyClient
from websocket import create_connection
import os, sys, time, json

class RunHandler(object):
    
    def __init__(self):
        self.apify_client = ApifyClient()
        self.migration = 0
        self.kill_interval = 3 # in seconds
    
    def check_migration_or_restart(self):
        ws_url = self.apify_client.options['APIFY_ACTOR_EVENTS_WS_URL']
        start_time = time.time()
        while 1:
            try:
                self.migration = json.loads( ws.recv() )
                print( self.migration )
                if "migrating" in self.migration["data"]:
                    os.system("pkill -SIGINT scrapy")
                    self.wrap_current_run()
                elif time.time() - start_time > self.kill_interval:
                    os.system("pkill -SIGINT scrapy")
                    time.sleep(10)
                    self.wrap_current_run()
                    # os.popen("scrapy crawl toscrape-css --set JOBDIR=current_run")
                    break
            except Exception as e:
                print(e)
                ws = create_connection( ws_url )
            time.sleep(0.1)        
    
    def unwrap_current_run(self):
        try:
            binary_data = self.apify_client.keyValueStores.getRecord({'recordKey': 'state_of_the_current_run'})
            print( len( binary_data ) )
            if len( binary_data ):
                current_run_zip = open( 'current_run.zip', 'wb' )
                current_run_zip.write( binary_data )
                current_run_zip.close()
            
            os.system('unzip current_run.zip')
        except Exception as e:
            print(e)
        
    def wrap_current_run(self):
        os.system('zip -r "current_run.zip" "current_run"')
        current_run_zip = open('current_run.zip', 'rb').read()
        self.apify_client.keyValueStores.putRecord({ "recordKey": "state_of_the_current_run", "data": current_run_zip, "contentType": "application/zip" })


h = RunHandler()


if '-test' in sys.argv:
    h.check_migration_or_restart()
else:
    h.unwrap_current_run()
    os.popen("scrapy crawl toscrape-css --set JOBDIR=current_run")
    h.check_migration_or_restart()
