from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
import shelve
import os
import time
from urllib.parse import urlparse


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.worker_id = worker_id
        
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)

    def get_report(self):
        print('===================GET_REPORT====================')
        filename = 'REPORT.txt'

        if os.path.isfile(filename):
            os.remove(filename)

        file = open(filename, 'w')
            
        amount_unique_page_visited = 0
        ics_domain_dic = {}
        
        for url_hash in self.frontier.save: #hash_url = (url, is_visited)
            cur_url = self.frontier.save[url_hash][0] #self.frontier = { url_hash = ('http://vision.ics.uci.edu/about', True)}
            url_parts = [part for part in cur_url.split('/') if part] # url_parts will be something like this ['http:', 'vision.ics.uci.edu', 'about']
            is_url_visited = self.frontier.save[url_hash][1]
            
            if len(url_parts) <= 1: #just to be safe
                continue
            key_domain = url_parts[1] # ['http:', 'vision.ics.uci.edu', 'about'][1] => 'vision.ics.uci.edu'
            
            if 'ics.uci.edu' in key_domain and is_url_visited:
                if key_domain in ics_domain_dic:
                    ics_domain_dic[key_domain] += 1
                else:
                    ics_domain_dic[key_domain] = 1
            if is_url_visited:
                amount_unique_page_visited += 1

        ics_domain_sorted = sorted(ics_domain_dic.items(), key=lambda el: el[0], reverse=False)
        
        
        file.write('#1: Out of '+str(len(self.frontier.save)) + ' unique URLs found, we visited '+ str(amount_unique_page_visited)+ ' unique pages.\n\n')
            
        with shelve.open("largest_page.shelve") as db:
            str_res = '#2 Largest website is in terms of number of words: ' + str(db['largest_site'][0]) + " with " + str(db['largest_site'][1]) + ' words\n\n'
            file.write(str_res) #(url, len)

        file.write('#3: 50 most common words are: '+ '\n')
        with shelve.open("wordCount.shelve") as db: #{words: amount_seen}
            ranked_list = sorted(db.items(), key=lambda el: (-el[1], el[0]), reverse=False)
            for word in ranked_list[:50]:
                str_write = str(word[0]) + ': ' + str(word[1]) + '\n'
                file.write(str_write)
        
        file.write("\n#4: \n")
        write_ics_domain_len = str(len(ics_domain_dic)) + ' subdomain of isc.uci.edu found\n'
        file.write(write_ics_domain_len)
        
        file.write("List of subdomains in alphabetical order:\n")
        for domain in ics_domain_sorted:
            file.write(str(domain[0]) + ' seen ' + str(domain[1]) + ' times \n')
            
        file.close()
        print('===================DONE====================')
    
    def run(self):
        while True:
            gave_up = False

            current_ts = time.time()         # current timestamp, in seconds, FP number
            with self.frontier.lock:         # unlock automatically at break and continue (bc we go to top of loop)
                tbd_url = self.frontier.get_tbd_url()
            
            # when there is no work, stay idle for 1 sec
            if not tbd_url:
                self.logger.info("Sleeping, no workload.")
                time.sleep(1)  # sleep 1 sec, sleeping in a lock is bad
                with self.frontier.lock:
                    if current_ts - max(self.frontier.domain_to_timestamp.values()) > 140:       # if thread sits idle for 100 sec
                        break     # only way to exit while true loop (end of thread)
                    continue
            
            parsed_url = urlparse(tbd_url)
            parsed_host = parsed_url.netloc

            with self.frontier.lock:
                # decide if we can download url
                last_accessed_ts = self.frontier.domain_to_timestamp.get(parsed_host, 0)      # .get() is safe-- doesn't crash if key not there
                if current_ts - last_accessed_ts <= self.config.time_delay:
                    # give up, url is no good, put back in Frontier
                    self.frontier.add_url(tbd_url)
                    gave_up = True
                else:
                    self.frontier.domain_to_timestamp[parsed_host] = time.time()              # update dictionary with fresh time (bc we're going to download and scrape the url page)
            if gave_up:
                time.sleep(0.01)        # sleep 1 ms (lower thread contention for the lock, aka lock politeness)
                continue

            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)   # thread safe, doesn't alter anything self.x
            
            with self.frontier.lock:                        # lock and save result (urls to frontier)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                self.frontier.mark_url_complete(tbd_url)

            #time.sleep(self.config.time_delay)            # put current thread to sleep for 0.5 sec (politeness maintained per thread), update: use dictionary in frontier 
        
        self.get_report()

        