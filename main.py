from comcrawl import IndexClient
import pandas as pd
import requests
from warcio.archiveiterator import ArchiveIterator


# list of available indices, "2014-52"
# index_list = ["2017-39", "2017-34", "2017-30", "2017-26", "2017-22", "2017-17"]
index_list = ["2021-49"]

def newscrawl():
    client = IndexClient()
    appended_data = []
    for index in index_list:
        # cc_url = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
        # cc_url += "url=%s&matchType=domain&output=json" % "mergersandacquisitions"
        cc_url = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
        cc_url += "url=%s&matchType=domain&output=json" % "financenews"
        client.search(cc_url, threads=4)
        # client.search("reddit.com/r/finance/")
        client.results = (pd.DataFrame(client.results)
                          .sort_values(by="timestamp")
                          .drop_duplicates("urlkey", keep="last")
                          .to_dict("records"))

        client.download(threads=4)
        appended_data.append(client.results)
        df = pd.DataFrame(client.results,columns=['urlkey','timestamp','url','mime','mime-detected','status','digest','length','offset','filename','languages','encoding','redirect','html'])

    pd.DataFrame(df).to_csv("NewsData.csv")



def readcontent():
    df = pd.read_csv("NewsData.csv")
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    newscrawl()
    readcontent()

