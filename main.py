import os
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
import glob
import numpy as np

import content
import crawl
from content import readcontent


# def getdata():
#     lastcrawl= getlatestcrawl()
#     URL = "https://index.commoncrawl.org/"
#     page = requests.get(URL)
#     soup = BeautifulSoup(page.content, "html.parser")
#     results = soup.find("table", class_="listing")
#
#     # Obtain every title of columns with tag <th>
#     headers = []
#     for i in results.find_all('th'):
#         title = i.text
#         headers.append(title)
#     # Create a dataframe
#     tabledf = pd.DataFrame(columns=headers)
#
#     # Create a for loop to fill data
#     for j in results.find_all('tr')[1:]:
#         row_data = j.find_all('td')
#         row = [i.text.strip() for i in row_data]
#         length = len(tabledf)
#         tabledf.loc[length] = row
#
#     # Create directory
#     dirName = 'CommonCrawlIndex'
#     if not os.path.exists(dirName):
#         os.mkdir(dirName)
#     timestr = time.strftime("%Y%m%d-%H%M%S")
#     filename = dirName+'/IndexData_'+ timestr+'.csv'
#     tabledf.to_csv(filename)
#     # print(tabledf)
#     latestcrawl = getlatestcrawl()
#     newdata = compareDiff(lastcrawl,latestcrawl)
#
#     print(newdata)
#
# def getlatestcrawl():
#     list_of_files = glob.glob('CommonCrawlIndex/*')  # * means all if need specific format then *.csv
#     latest_file= max(list_of_files, key=os.path.getctime)
#     print(latest_file)
#     return latest_file
#
# def compareDiff(lastfile,newfile):
#     old_df = pd.read_csv(lastfile)
#     new_df = pd.read_csv(newfile)
#     df = new_df.merge(old_df, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
#     return df

# schedule.every(2).minutes.do(getdata)
# schedule.every(4).hour.do(getdata)
# schedule.every(30).day.at("10:30").do(getdata)

# while 1:
#     schedule.run_pending()
#     time.sleep(1)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # getdata()
    crawl.newscrawl()
    content.readcontent()


