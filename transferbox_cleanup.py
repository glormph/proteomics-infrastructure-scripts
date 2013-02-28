import os, urllib2, urllib, cookielib, json, logging, datetime
from lxml import html

DATADIR = '/mnt/datadrive'

logging.basicConfig(filename='transfer_cleaning.log',level=logging.DEBUG, 
	format='%(asctime)s - %(levelname)s - %(message)s')

logging.info('Checking files to delete on transfer box...')

cj = cookielib.CookieJar()
opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
urllib2.install_opener(opener)

doc = urllib2.urlopen('http://localhost:8000/kantele/login').read()
token = html.fromstring(doc).xpath('//input[@name="csrfmiddlewaretoken"]/@value')[0]

data = [('fn',x) for x in os.listdir(DATADIR)] 
data.append(('csrfmiddlewaretoken', token))
data = urllib.urlencode(data)
response = urllib2.urlopen('http://localhost:8000/kantele/rawstatus/', data).read()
response = json.loads(response)

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


        
