from warcio.archiveiterator import ArchiveIterator
import pandas as pd
import requests

def readcontent():
    df = pd.read_csv("InputFiles/NewsData.csv")
    filenamelist = df['filename']
    for name in filenamelist:
        warc_url = 'https://commoncrawl.s3.amazonaws.com/' + name
        # wet_url = warc_url.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz')
        # wat_url = warc_url.replace('/warc/', '/wat/').replace('warc.gz', 'warc.wat.gz')
        resp = requests.get(warc_url, stream=True)
        for record in ArchiveIterator(resp.raw, arc2warc=True):

            if record.rec_type == 'warcinfo':
                print(record.raw_stream.read())

            elif record.rec_type == 'response':
                if record.http_headers.get_header('Content-Type') == 'text/html':
                    print("response ****************************************")
                    print(record.rec_headers.get_header('WARC-Target-URI'))
                    print(record.content_stream().read())