import os, logging, datetime
import metadata_querying

DATADIR = '/mnt/datadrive'
LOGIN = 'http://localhost:8000/kantele/login'
URL = 'http://localhost:8000/kantele/rawstatus'

logging.basicConfig(filename='transfer_cleaning.log',level=logging.DEBUG, 
	format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Checking files to delete on transfer box...')

files = os.listdir(DATADIR)
response = metadata_querying.query_rawfiles(files, URL, LOGIN)

todelete = []
for fn in response:
    if response[fn][0] == 'done':
        try:
            td = datetime.datetime.now() - datetime.datetime.strptime(response[fn][1],
                '%Y%m%d')
        except:
            td = datetime.datetime.now() - datetime.datetime.strptime(response[fn][1][:10],
                '%Y-%m-%d')
        if td.days > 7:
            todelete.append(fn)
if todelete:
    logging.info('Found {0} files archived in Kalevala older than 7 days. Deleting them.'.format(len(todelete)))
else:
    logging.info('No archived files found. Not deleting')
for fn in todelete:
    logging.info('Deleting {0}'.format(fn))
    os.remove(os.path.join(DATADIR, fn))


        
