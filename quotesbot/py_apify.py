import os, json, sys
from time import sleep
import gzip
 
if sys.version_info[0] > 2:
    import urllib.request as u2
    print('python3.x')
else:
    import urllib2 as u2
    print('python2.x')
# sys.version_info


# Main CLASS
class ApifyClient(object):
    
    # default option preset
    options = {'contentType': 'application/json', 'expBackOffMaxRepeats': 8, 'expBackOffMillis': 500, 'disableBodyParser': 0, 'data': {} }
    
    # simple function for pushing records - input {'data': }
    def pushData(self, options):
        
        _options = self.merge_options( options )
        
        url = 'https://api.apify.com/v2/datasets/' + _options['APIFY_DEFAULT_DATASET_ID'] + '/items'
        
        return self.make_request(url, values=_options['data'], headers={'Content-Type': _options['contentType']}, method='POST')
        
        
    def openDataset(self, name):
    
        self.options = self.merge_options( { 'APIFY_DEFAULT_DATASET_ID': self.datasets.getOrCreateDataset({'datasetName': name})['data']['id'] } )
    
    ## Add timeout/delay to repeat - global request function
    def make_request(self, url, values=None, headers={}, method='GET', disable_body_parser=0):
            
        url = url.strip('?')
        
        if type( values ) is dict or type( values ) is list:
            values = str( json.dumps( values ) ).encode()
        elif type( values ) is str:
            values = values.encode()
            
        req = u2.Request( url, data=values, headers=headers)    
        
        # repeat counter
        for i in range( self.options['expBackOffMaxRepeats'] ):
     
            
            if method == 'PUT':
                req.get_method = lambda: 'PUT'
                
            elif method == 'DELETE':
                req.get_method = lambda: 'DELETE'
            
            elif method == 'POST':
                req.get_method = lambda: 'POST'
            
            elif method == 'GET':
                req.get_method = lambda: 'GET'
            
            response = u2.urlopen( req )
            
            code = response.getcode()
            
            if code >= 500:
                pass
                
            elif 300 <= code <= 499:
                raise Exception('RATE_LIMIT_EXCEEDED_STATUS_CODE')
                
            else:
                if response.info().get('Content-Encoding') == 'gzip':
                    pagedata = gzip.decompress(response.read())
                
                else:
                    pagedata = response.read()
                
                if method != 'DELETE' and disable_body_parser == 0:
                    return json.loads( pagedata.decode() )
                else:
                    return pagedata
            
            sleep( self.options['expBackOffMillis'] / 1000 )
                
        return False
        
    # default options merger
    def merge_options(self, options):
    
        _options = dict( self.options )
        _options.update( options )
        
        return _options 
    
    
    def __init__(self, options={}):
        
        # detects and imports all APIFY env variables
        for env in ['APIFY_ACT_ID', 'APIFY_ACT_RUN_ID', 'APIFY_USER_ID', 'APIFY_TOKEN', 'APIFY_STARTED_AT', 'APIFY_TIMEOUT_AT', 'APIFY_DEFAULT_KEY_VALUE_STORE_ID', 'APIFY_DEFAULT_DATASET_ID', 'APIFY_WATCH_FILE', 'APIFY_HEADLESS', 'APIFY_MEMORY_MBYTES']:
            if env in os.environ:
                self.options[ env ] = os.environ.get( env )
        
        if 'APIFY_DEFAULT_KEY_VALUE_STORE_ID' in self.options:
            self.options['storeId'] = self.options['APIFY_DEFAULT_KEY_VALUE_STORE_ID']
        if 'APIFY_DEFAULT_DATASET_ID' in self.options:
            self.options['datasetId'] = self.options['APIFY_DEFAULT_DATASET_ID']
        if 'APIFY_TOKEN' in self.options:
            self.options['token'] = self.options['APIFY_TOKEN']
        
        self.options = self.merge_options( options )
        
        ## initialize all inner classes
        self.keyValueStores = self.KeyValueStores( self.options, self.make_request, self.merge_options )
        self.datasets = self.Datasets( self.options, self.make_request, self.merge_options )
    
    # override existing options, also for inner classes
    def setOptions(self, options):
        
        self.options.update( options )
        
        self.keyValueStores.options = self.options
        self.datasets.options = self.options
    
    # returns current options
    def getOptions(self):
    
        return self.options
    
    
    ## DATASETS - INNER CLASS
    class Datasets(object):
        
        def getParams(self, options, params=[]):
            
            serialized = '?'
            for opt in options:
                if opt in params:
                    if options[opt] != '':
                        serialized += opt + '=' + str(options[opt]) + '&'
                        
            return serialized.strip('&')
                
             
        def __init__(self, options, make_request, merge_options):
            
            self.options = options
            self.make_request = make_request
            self.merge_options = merge_options
            
            self.defaultDatasetsUrl = 'https://api.apify.com/v2/datasets/'
        
        def deleteStore(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultDatasetsUrl  + _options['datasetId']
            
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method = 'DELETE' )
            
        def getDataset(self, options={}):
            
            _options = self.merge_options( options )
                        
            url = self.defaultDatasetsUrl + _options['datasetId']
            
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method='GET' )
        
        def getItems(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultDatasetsUrl + _options['datasetId'] + '/items' + self.getParams( _options, params=['format', 'offset', 'limit', 'desc', 'fields', 'unwind', 'omit', 'attachment', 'delimiter', 'bom', 'xmlRoot', 'xmlRow', 'skipHeader', 'token'] ) #+ get_params
            
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method="GET", disable_body_parser=_options[ "disableBodyParser" ] )
        
        def getOrCreateDataset(self, options={}):
            
            _options = self.merge_options( options ) 
            
            url = self.defaultDatasetsUrl +'?token='+ _options['token'] + '&name=' + _options['datasetName']

            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method='POST' )
            
        def listDatasets(self, options={}):
            
            _options = self.merge_options( options )            
            url = self.defaultDatasetsUrl + self.getParams( _options, params=['token', 'offset', 'limit', 'desc'] )
            
            return self.make_request( url )
        
        def putItems(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultDatasetsUrl + _options['datasetId'] + '/items'
            
            return self.make_request( url, values=options['data'], headers={'Content-Type': _options['contentType']}, method='POST' )
    
    ## KVSTORES - INNER CLASS
    class KeyValueStores(object):
    
        def getParams(self, options, params=[]):
            
            serialized = '?'
            for opt in options:
                if opt in params:
                    if options[opt] != '':
                        serialized += opt + '=' + str(options[opt]) + '&'
                        
            return serialized.strip('&')
        
        
        def __init__(self, options, make_request, merge_options):
            
            self.options = options
            self.make_request = make_request
            self.merge_options = merge_options
            
            self.defaultKeyValueStoresUrl = 'https://api.apify.com/v2/key-value-stores'
            # https://api.apify.com/v2/key-value-stores?token=rWLaYmvZeK55uatRrZib4xbZs&offset=10&limit=99&desc=1&unnamed=1
               
        def getListOfKVStores(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultKeyValueStoresUrl + self.getParams( _options, params=['token', 'offset', 'limit', 'desc', 'unnamed'] )
    
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method='GET' )
            
        def getOrCreateKVStore(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultKeyValueStoresUrl + self.getParams( _options, params=['token', 'name'] )
            
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method='POST' )
        
        def getStore(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultKeyValueStoresUrl + '/' + _options['storeId'] + self.getParams( _options, params=['token'] )
            
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method='GET' )
       
        def deleteStore(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultKeyValueStoresUrl + '/' + _options['storeId'] + self.getParams( _options, params=['token'] )
            
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method = 'DELETE' )
            
        def getListOfKeys(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultKeyValueStoresUrl + '/' + _options['storeId'] + '/keys' + self.getParams( _options, params=['token', 'limits', 'exklusiveStartKey'] )
            
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method='GET' )
        
        def getRecord(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultKeyValueStoresUrl + '/' + _options['storeId'] + '/records/' + _options['recordKey'] + self.getParams( _options, params=['token', 'disableRedirect', 'disableBodyParser'] )
            
            return self.make_request( url, headers={'Accept-Encoding':'gzip;q=0'}, method='GET', disable_body_parser=1)
        
        
        def putRecord(self, options={}):
            
            _options = self.merge_options( options )
        
            url = self.defaultKeyValueStoresUrl + '/' + _options['storeId'] + '/records/' + _options['recordKey'] + self.getParams( _options, params=['token'] )
            
            return self.make_request( url, values=options['data'],headers={'Content-Type': _options['contentType']}, method='POST', disable_body_parser=1 )
        
        def getDirectUploadURL(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultKeyValueStoresUrl + '/' + _options['storeId'] + '/records/' + _options['recordKey'] + '/direct-upload-url' + self.getParams( _options, params=['token'] )
            
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method='GET' )
        
        def deleteRecord(self, options={}):
            
            _options = self.merge_options( options )
            
            url = self.defaultKeyValueStoresUrl + '/' + _options['storeId'] + '/records/' + _options['recordKey'] + self.getParams( _options, params=['token'] )
            
            return self.make_request( url, headers={'Content-Type': _options['contentType']}, method='DELETE' )
            
