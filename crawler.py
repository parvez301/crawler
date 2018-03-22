import urllib.request as urlcon
from urllib.parse import urlparse
from urllib.parse import urlunparse
from urllib.error import URLError, HTTPError
from bs4 import BeautifulSoup
import sys
import socket
import os
import codecs
import time
import threading
import traceback
from collections import deque

VISITED_URLS = {}
CRAWLED_URLS = {}
CRAWL_BUFFER = deque([])
CRAWLER_DEFAULT_WORKERS = 4
WORKER_WAIT_INTERVAL = 1
MAX_COUNT_LIMIT = None


class WorkerThread(threading.Thread):
    def __init__(self, crawler, name):
        threading.Thread.__init__(self)
        self.__crawler = crawler
        self.name = name
    
    def run(self):
        """Start function for each thread"""

        while not self.__crawler.kill and self.is_alive():
            try:
                if len(CRAWL_BUFFER) > 0:
                    strURL = CRAWL_BUFFER.poplef()
                    urlObj = URL(strURL)
                    print("URL " + str(urlObj.url) + " about to be crawled by worker: " + str(self.name))
                    self.__crawler.crawl(urlObj)
                else:
                    print("NO work for worker:" + str(self.name))
                time.sleep(WORKER_WAIT_INTERVAL)
            except Exception as e:
                print("Unknown exception occured while doing worker task" + str(e))
                traceback.print_exc()
        print("Stopping Worker : " + str(self.name))


class URL():
    def __init__(self, strURL):
        self.url = strURL
        self.netloc = None
        self.scheme = None
        self.validateURL()

    def validateURL(self):
        """Do url validation"""

        parse_url = urlparse(self.url)
        if parse_url.netloc:
            self.netloc = parse_url.netloc
        else:
            self.valid = False
        if parse_url.scheme:
            self.scheme = parse_url.scheme
        else:
            self.valid = False
