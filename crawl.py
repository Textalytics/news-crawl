from comcrawl import IndexClient
import pandas as pd
import os
import requests
import json
import time
# list of available indices, "2014-52"
# index_list = ["2017-39", "2017-34", "2017-30", "2017-26", "2017-22", "2017-17"]
# index_list = ["2021-49","2021-43","2021-39"]

def newscrawl():
    from comcrawl import IndexClient
    df = pd.DataFrame()
    client = IndexClient()
    client.search("news", threads=4)

    client.results = (pd.DataFrame(client.results)
                          .sort_values(by="timestamp")
                          .drop_duplicates("urlkey", keep="last")
                          .to_dict("records"))

    client.download(threads=4)
    df = pd.DataFrame(client.results,columns=['urlkey','timestamp','url','mime','mime-detected','status','digest','length','offset','filename','languages','encoding','redirect','html'])
    # Create directory
    dirName = 'InputFiles'
    if not os.path.exists(dirName):
        os.mkdir(dirName)

    pd.DataFrame(df).to_csv(dirName + "/NewsData" + ".csv")

# arr = []
    # for index in index_list:
    #     url = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
    #     url += "url=news&output=json"            # url = "https://index.commoncrawl.org/CC-MAIN-2021-49-index?url=news&output=json"
    #
    #     response = requests.get(url)
    #     if(response.ok == True):
    #         json_data = json.loads(response.text)
    #         arr.append(json_data)
    #
    # df = pd.DataFrame(arr,columns=['urlkey', 'timestamp', 'url', 'mime', 'mime-detected', 'status', 'digest', 'length',
    #                                'offset', 'filename', 'languages', 'encoding', 'redirect', 'html'])
    #

