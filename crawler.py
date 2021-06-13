# coding: utf-8

from time import time, sleep

import sys
import subprocess
import os
from newsplease import NewsPlease
from warcio.archiveiterator import ArchiveIterator
import pycld2
import json
import warnings
import faulthandler
faulthandler.enable()

def process_warc(file_name, limit=1):
    n_documents = {}
    n_documents["limit"] = limit
    n_documents["total"] = 0
    for i in range(10):
        n_documents[10+i] = 0
    count = 0
    docList = [0 for i in range(limit)]
    titleDict = {}
    queue = []
    dictOkNok = {}
    start = time()
    with open(file_name, "rb") as stream, open("results/"+sys.argv[3], "w+") as docf:
        for record in ArchiveIterator(stream):
            try:
                #news = from_warc(record, fetch_images = False)
                news = from_warc(record)
                warnings.filterwarnings("error")
            except Exception as e:
                count += 1
                if count%100 == 0:
                    end = time()
                    print(count, n_documents, end - start)
                continue
            if news.title != None and news.date_publish != None and news.maintext != None:
                date = news.date_publish.date()
                valid, topicId =  isValid(news.title, news.date_publish, n_documents)
                if valid:
                    if checkDoc(news, titleDict, topicId):
                        queue.append("Ok")
                        dictOkNok["Ok"] = dictOkNok.get("Ok", 0) + 1
                        if len(queue) > 20:
                            dictOkNok[queue.pop(0)] -= 1
                        text = news.maintext
                        date = str(date)
                        title = news.title
                        strToWrite = "{\"title\":\"" + title + "\",\"date_publish\":\"" + date + "\",\"topicId\":\""  + str(topicId) + "\",\"maintext\":\""  + text +"\"}\n"
                        docf.write(strToWrite)
                        docf.flush()
                        neededDict = titleDict.get((date, topicId), {})
                        neededDict[title.lower()] = None
                        titleDict[(date, topicId)] = neededDict
                        n_documents[topicId] = n_documents.get(topicId, 0) + 1
                        n_documents["total"] =  n_documents.get("total", 0) + 1
                        print("Ok", title, n_documents)
                    else:
                        queue.append("Nok")
                        print("Nok", news.title, topicId, date)
                        dictOkNok["Nok"] = dictOkNok.get("Nok", 0) + 1
                        if len(queue) > 20:
                            dictOkNok[queue.pop(0)] -= 1
                            if dictOkNok["Nok"]/(dictOkNok["Nok"] + dictOkNok["Ok"]) >= 0.9:
                                break
            count += 1
            if count%100 == 0:
                end = time()
                print(count, n_documents, end - start)

    end = time()
    print(count, n_documents, end - start)



def isValid(str, publish_date, n_documents):
    str = str.lower()
    year = publish_date.year
    month = publish_date.month
    if year <= 2019 or (year == 2019 and month < 4):
        if "music" in str or "album" in str or "song" in str:
            return n_documents[10] < n_documents["limit"], 10
        elif "immigration" in str or "migrant" in str or "refugee" in str:
            return n_documents[11] < n_documents["limit"], 11
        elif "christmas" in str:
            return n_documents[12] < n_documents["limit"], 12
        elif "education" in str or "school" in str:
            return n_documents[13] < n_documents["limit"], 13
        elif "canada" in str or "quebec" in str or "monreal" in str or "vancouver" in str or "ottawa" in str:
            return n_documents[14] < n_documents["limit"], 14
    elif year > 2019 or (year == 2019 and month >= 5):
        if "capitol" in str and ("storming" in str or "invasion" in str or "riot" in str):
            return n_documents[19] < n_documents["limit"], 19
        elif "covid" in str and "19" in str:
            return n_documents[16] < n_documents["limit"], 16
        elif "beirut" in str and ("explosion" in str or "blast" in str):
            return n_documents[17] < n_documents["limit"], 17
        elif "george" in str and "floyd" in str:
            return n_documents[18] < n_documents["limit"], 18
        elif "biden" in str and "elect" in str:
            return n_documents[15] < n_documents["limit"], 15
    return False, None

def checkDoc(news, titleDict, topicId):
    if (news.date_publish.date(), topicId) in titleDict:
        for title in titleDict[(news.date_publish.date(), topicId)]:
            if title == news.title:
                return False
    return True


def from_warc(warc_record):
    raw_stream = warc_record.raw_stream.read()
    encoding = None
    try:
        encoding = warc_record.http_headers.get_header('Content-Type').split(';')[1].split('=')[1]
    except:
        pass
    if not encoding:
        encoding = EncodingDetector.find_declared_encoding(raw_stream, is_html=True)
    if not encoding:
        # assume utf-8
        encoding = 'utf-8'

    try:
        html = raw_stream.decode(encoding, errors="replace")
    except LookupError:
        # non-existent encoding: fallback to utf-9
        html = raw_stream.decode('utf-8', errors="replace")
    if not html:
        raise ValueError()
    url = warc_record.rec_headers.get_header('WARC-Target-URI')
    download_date = warc_record.rec_headers.get_header('WARC-Date')
    article = from_html(html, url=url, download_date=download_date, fetch_images=False)
    return article



import datetime
import os
import sys
import urllib

from bs4.dammit import EncodingDetector
from six.moves import urllib

from newsplease.pipeline.extractor import article_extractor
from newsplease.crawler.items import NewscrawlerItem
from dotmap import DotMap
from newsplease.pipeline.pipelines import ExtractedInformationStorage
from newsplease.crawler.simple_crawler import SimpleCrawler

def from_html(html, url=None, download_date=None, fetch_images=True):
        """
        Extracts relevant information from an HTML page given as a string. This function does not invoke scrapy but only
        uses the article extractor. If you have the original URL make sure to provide it as this helps NewsPlease
        to extract the publishing date and title.
        :param html:
        :param url:
        :return:
        """
        extractor = article_extractor.Extractor(
            (
                ['newspaper_extractor']
                if fetch_images
                else [("newspaper_extractor_no_images", "NewspaperExtractorNoImages")]
            ) +
            ['readability_extractor', 'date_extractor']
            #['date_extractor']
        )

        title_encoded = ''.encode()
        if not url:
            url = ''

        # if an url was given, we can use that as the filename
        filename = urllib.parse.quote_plus(url) + '.json'

        item = NewscrawlerItem()
        item['spider_response'] = DotMap()
        item['spider_response'].body = html
        item['url'] = url
        item['source_domain'] = urllib.parse.urlparse(url).hostname.encode() if url != '' else ''.encode()
        item['html_title'] = title_encoded
        item['rss_title'] = title_encoded
        item['local_path'] = None
        item['filename'] = filename
        item['download_date'] = download_date
        item['modified_date'] = None

        item = extractor.extract(item)
        tmp_article = ExtractedInformationStorage.extract_relevant_info(item)
        final_article = ExtractedInformationStorage.convert_to_class(tmp_article)
        return final_article


#i = "https://commoncrawl.s3.amazonaws.com/"+i.split(" ")[3].replace("\n", "")
#subprocess.run(["wget " + i + " -P archives/"], shell=True)

if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[4] == "Count":
        count = 0
        with open("archives/"+sys.argv[1], 'rb') as stream, open("results/"+sys.argv[3], "w+") as docf:
            for record in ArchiveIterator(stream):
                count +=1
                if count%1000==0:
                    print(count)
        print(count)
    else:
        process_warc("archives/"+sys.argv[1], limit=int(sys.argv[2]))
