import sys
import socket
import os
import codecs
import time
import traceback
import requests
from urllib.parse import urlparse
from urllib.parse import urlunparse
from urllib.error import URLError, HTTPError
from bs4 import BeautifulSoup
from threading import Thread, Lock
from collections import deque

VISITED_URLS = {}
CRAWLED_URLS = {}
CRAWL_BUFFER = deque([])
CRAWLER_DEFAULT_WORKERS = 4
WORKER_WAIT_INTERVAL = 1
MAX_COUNT_LIMIT = None


class WorkerThread(Thread):
    def __init__(self, crawler, name):
        Thread.__init__(self)
        self.__crawler = crawler
        self.name = name
    
    def run(self):
        """Start function for each thread"""

        while not self.__crawler.kill and self.is_alive():
            try:
                if len(CRAWL_BUFFER) > 0:
                    strURL = CRAWL_BUFFER.popleft()
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
        self.valid = True
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


class WebCrawler():
    _lock = Lock()
    kill = False
    count = 0
    listworkers = []

    def __init__(self):
        self.activeWorkers = []
        self.__startWorkers()

    def __startWorkers(self):
        """Start worker thread"""
        try:
            for workerIndex in range(CRAWLER_DEFAULT_WORKERS):
                strWorkerName = "Worker " + str(workerIndex)
                worker = WorkerThread(self, strWorkerName)
                worker.start()
                self.listworkers.append(worker)
        except Exception as e:
            print("Exception occured in crawler " + str(e))
            traceback.print_exc()
            exit()

    def crawl(self, urlObj):
        """Main Function which crawl URL's """
        try:
            if ((urlObj.valid) and (urlObj.url not in CRAWLED_URLS.keys())):
                response = requests.get(urlObj.url, timeout=2)
                page = response.text
                soup = BeautifulSoup(page, "lxml")
                links = self.scrap(soup)
                boolStatus = self.checkmax()
                if boolStatus:
                    CRAWLED_URLS.setdefault(urlObj.url, "True")
                else:
                    return
                for link in links:
                    if link not in VISITED_URLS:
                        parsed_url = urlparse(link)
                        if parsed_url.scheme and "javascript" in parsed_url.scheme:
                            print("***Javascript found in scheme " + str(link) + "***")
                            continue
                        # handle internal URLs
                        try:
                            if not parsed_url.scheme and not parsed_url.netloc:
                                print("No scheme and host found for " + str(link))
                                newURL = urlunparse(parsed_url._replace(**{"scheme":urlObj.scheme,"netloc":urlObj.netloc}))
                                link = newURL
                            elif not parsed_url.scheme:
                                print("Scheme not found for " + str(link))
                                newURL = urlunparse(parsed_url._replace(**{"scheme":urlObj.scheme}))
                                link = newURL
                            # Check again for internal URLs
                            if link not in VISITED_URLS:
                                print("Found child link " + link)
                                CRAWL_BUFFER.append(link)
                                with self._lock:
                                    self.count += 1
                                    print(" Count is =================> " + str(self.count))
                                    boolStatus = self.checkmax()
                                    if boolStatus:
                                        VISITED_URLS.setdefault(link, "True")
                                    else:
                                        return
                        except TypeError:
                            print("Type error occured")
                    else:
                        print("URL already present in visited " + str(urlObj.url))
        except socket.timeout as e:
            print("**************** Socket timeout occured *******************")
        except URLError as e:
            if isinstance(e.reason, ConnectionRefusedError):
                print("**************** Connection refused error occured*******************")
            elif isinstance(e.reason, socket.timeout):
                print("**************** Socket timed out error occured***************")
            elif isinstance(e.reason, OSError):
                print("**************** OS error occured*************")
            elif isinstance(e, HTTPError):
                print("**************** HTTP Error occured*************")
            else:
                print("**************** URL Error occured***************")
        except Exception as e:
            print("Unknown exception occured while fetching HTML code" + str(e))
            traceback.print_exc()

    def scrap(self, soup):
        """Scrap all links"""
        rec_links = []
        for link in soup.find_all('a'):
            rec_links.append(link.get('href'))
        return rec_links

    def checkmax(self):
        """Check if upper limit on URL's to be scrapped is reached or not"""
        boolStatus = True
        if MAX_COUNT_LIMIT and self.count >= MAX_COUNT_LIMIT:
            print(" Maximum count reached. Now exiting and stopping workers :( ")
            self.kill = True
            boolStatus = False
        return boolStatus

def saveDataToFile(listData):
    """Save output data in a file under current directory"""
    boolToReturn = True
    fileName = None
    try:
        path = os.getcwd()
        fileName = path + "/" +"output.txt"
        file_obj = codecs.open(fileName,'w')
        for eachLink in listData:
            file_obj.write(str(eachLink) + "\n")
    except Exception as e:
        boolToReturn = False
        print("Unknown exception occured in saving data to file " + str(e))
        traceback.print_exc()
    finally:
        return boolToReturn, fileName


if __name__ == '__main__':
    try:
        if len(sys.argv) == 3:
            MAX_COUNT_LIMIT = int(sys.argv[2])
        CRAWL_BUFFER.append(str(sys.argv[1]))
        VISITED_URLS.setdefault(str(sys.argv[1]), "True")
        web_crawler = WebCrawler()
        while web_crawler.listworkers[0].is_alive():
            web_crawler.listworkers[0].join(1)
    except KeyboardInterrupt:
        print("**************** Keyboard interrupt occured. Stopping all threads *******************")
        exit(0)
    except Exception as e:
        print("Unknown exception occured in main" + str(e))
        traceback.print_exc()
    finally:
        boolStatus, fileName = saveDataToFile(VISITED_URLS.keys())
        if boolStatus:
            print("Links saved in the repository " + str(fileName) + " successfully")
            print("Number of links : "  + str(len(VISITED_URLS)))
        else:
            print("Unable to save links " + str(VISITED_URLS))
        web_crawler.kill = True
