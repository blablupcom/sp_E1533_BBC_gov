# -*- coding: utf-8 -*-

#### IMPORTS 1.0

import os
import re
import scraperwiki
import urllib2
from datetime import datetime
from dateutil.rrule import rrule, MONTHLY
from bs4 import BeautifulSoup

#### FUNCTIONS 1.2
import requests  # import requests to make a post request

def validateFilename(filename):
    filenameregex = '^[a-zA-Z0-9]+_[a-zA-Z0-9]+_[a-zA-Z0-9]+_[0-9][0-9][0-9][0-9]_[0-9QY][0-9]$'
    dateregex = '[0-9][0-9][0-9][0-9]_[0-9QY][0-9]'
    validName = (re.search(filenameregex, filename) != None)
    found = re.search(dateregex, filename)
    if not found:
        return False
    date = found.group(0)
    now = datetime.now()
    year, month = date[:4], date[5:7]
    validYear = (2000 <= int(year) <= now.year)
    if 'Q' in date:
        validMonth = (month in ['Q0', 'Q1', 'Q2', 'Q3', 'Q4'])
    elif 'Y' in date:
        validMonth = (month in ['Y1'])
    else:
        try:
            validMonth = datetime.strptime(date, "%Y_%m") < now
        except:
            return False
    if all([validName, validYear, validMonth]):
        return True


def validateURL(url, data_dict):
    try:
        r = requests.post(url, data=data_dict)
        count = 1
        while r.status_code == 500 and count < 4:
            print ("Attempt {0} - Status code: {1}. Retrying.".format(count, r.status_code))
            count += 1
            r = requests.post(url, data=data_dict)
        sourceFilename = r.headers.get('Content-Disposition')

        if sourceFilename:
            ext = os.path.splitext(sourceFilename)[1].replace('"', '').replace(';', '').replace(' ', '')
        else:
            ext = os.path.splitext(url)[1]
        validURL = r.status_code == 200
        validFiletype = ext.lower() in ['.csv', '.xls', '.xlsx']
        return validURL, validFiletype
    except:
        print ("Error validating URL.")
        return False, False


def validate(filename, file_url, data_dict):
    validFilename = validateFilename(filename)
    validURL, validFiletype = validateURL(file_url, data_dict)
    if not validFilename:
        print filename, "*Error: Invalid filename*"
        print file_url
        return False
    if not validURL:
        print filename, "*Error: Invalid URL*"
        print file_url
        return False
    if not validFiletype:
        print filename, "*Error: Invalid filetype*"
        print file_url
        return False
    return True


def convert_mth_strings ( mth_string ):
    month_numbers = {'JAN': '01', 'FEB': '02', 'MAR':'03', 'APR':'04', 'MAY':'05', 'JUN':'06', 'JUL':'07', 'AUG':'08', 'SEP':'09','OCT':'10','NOV':'11','DEC':'12' }
    for k, v in month_numbers.items():
        mth_string = mth_string.replace(k, v)
    return mth_string


#### VARIABLES 1.0

entity_id = "E1533_BBC_gov"
url = "http://opendata.brentwood.gov.uk/View/finance/payments-to-suppliers"
errors = 0
data = []
start_date = datetime(2014,1,1).strftime('%d/%m/%Y')
end_date = datetime.now().strftime('%d/%m/%Y')
user_agent = {'User-agent': 'Mozilla/5.0'}
data_dict = {"OrderByColumn":"[DatePaid]",
"OrderByDirection":"DESC",
"Download":"csv",
"radio":"on",
"chartType":"table",
"filter[0].ColumnToSearch":"DatePaid",
"filter[0].SearchOperator":"contains",
"filter[0].SearchText":"",
"filter[0].SearchOperatorNumber":"greaterthan",
"filter[0].SearchNumber":"",
"filter[0].From":"{}".format(start_date),
"filter[0].To":"{}".format(end_date),
"VisibleColumns":"1_DatePaid",
"VisibleColumns":"2_ExpensesType",
"VisibleColumns":"5_NetAmount",
"VisibleColumns":"8_Service",
"getVisualisationData":"false",
"xAxis":"Text#ServiceArea",
"yAxis":"Currency#NetAmount",
"yAxisAggregate":"sum",
"chartCurrentPage":"1",
"chartNumberToShow":"10",
"numberToShow":"10",
"CurrentPage":"1",
"PageNumber":""}

#### READ HTML 1.0

html = urllib2.urlopen(url)
soup = BeautifulSoup(html, 'lxml')

#### SCRAPE DATA

download_link = "http://opendata.brentwood.gov.uk/View/finance/payments-to-suppliers"
csvMth = 'Y1'
csvYr = datetime.now().strftime('%Y')
link = soup.find('a', id='downloadData')
if link:
   csvMth = convert_mth_strings(csvMth.upper())
   data.append([csvYr, csvMth, download_link, data_dict])


#### STORE DATA 1.0

for row in data:
    csvYr, csvMth, url, data_dict = row
    filename = entity_id + "_" + csvYr + "_" + csvMth
    todays_date = str(datetime.now())
    file_url = url.strip()

    valid = validate(filename, file_url, data_dict)

    if valid == True:
        scraperwiki.sqlite.save(unique_keys=['l'], data={"l": file_url, "f": filename, "d": todays_date })
        print filename
    else:
        errors += 1

if errors > 0:
    raise Exception("%d errors occurred during scrape." % errors)


#### EOF
