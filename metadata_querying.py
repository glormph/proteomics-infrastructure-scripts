import cookielib, urllib2, urllib, json
from lxml import html

def query_metadata_server(files, url, login=None):
    data = [('fn',x) for x in files] 
    if login:
        cj = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        urllib2.install_opener(opener)
        
        doc = urllib2.urlopen(login).read()
        token = html.fromstring(doc).xpath('//input[@name="csrfmiddlewaretoken"]/@value')[0]
        data.append(('csrfmiddlewaretoken', token))
    
    data = urllib.urlencode(data)
    response = urllib2.urlopen(url, data).read()
    return json.loads(response)
