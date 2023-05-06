from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
import shelve
import os

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        
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
        
        #for every url_hash in the frontier...
        for url_hash in self.frontier.save: #hash_url = (url, is_visited)
            # get the url
            cur_url = self.frontier.save[url_hash][0] #self.frontier = { url_hash = ('http://vision.ics.uci.edu/about', True)}
            #get each url part and add it to "url" parts if that part is not empty 
            url_parts = [part for part in cur_url.split('/') if part] # url_parts will be something like this ['http:', 'vision.ics.uci.edu', 'about']
            #save visited status
            is_url_visited = self.frontier.save[url_hash][1]
            
            if len(url_parts) <= 1: #just to be safe
                continue
            # get the domain from the list of parts
            key_domain = url_parts[1] # ['http:', 'vision.ics.uci.edu', 'about'][1] => 'vision.ics.uci.edu'
            
            # make sure the domain contains 'ics.uci.edu' and is visited
            if 'ics.uci.edu' in key_domain and is_url_visited:
                if key_domain in ics_domain_dic:
                    ics_domain_dic[key_domain] += 1 #incriment the # of domains in the ics domain
                else:
                    ics_domain_dic[key_domain] = 1 # create the item in the dictionary
            if is_url_visited:
                amount_unique_page_visited += 1 #incriment the is_url_visited count

        # sort the domains by alphebatical order 
        ics_domain_sorted = sorted(ics_domain_dic.items(), key=lambda el: el[0], reverse=False)
        
        # write to the report the # of unique URLs found and the # of unique pages visited
        file.write('Out of '+str(len(self.frontier.save)) + ' unique URLs found, we visited '+ str(amount_unique_page_visited)+ ' unique pages.\n')
        
        # write to the report the largest file
        with shelve.open("largest_page.shelve") as db:
            str_res = 'Largest website is in terms of number of words:' + str(db['largest_site']) + '\n'
            file.write(str_res) #(url, len)
        
        # write to the report the number of subdomains from ics found
        write_ics_domain_len = str(len(ics_domain_dic)) + ' subdomain of isc.uci.edu found\n'
        file.write(write_ics_domain_len)
        
        # write to the report how many times you have seen the differnt sorted domains
        for domain in ics_domain_sorted:
            file.write(str(domain[0]) + ' seen ' + str(domain[1]) + ' times. \n')
            
        
       
        # write to the report the 50 most common words
        file.write('50 most common words are: '+ '\n')
        with shelve.open("wordCount.shelve") as db: #{words: amount_seen}
            ranked_list = sorted(db.items(), key=lambda el: (-el[1], el[0]), reverse=False)
            for word in ranked_list[:50]:
                str_write = str(word) + ' '
                file.write(str_write)

        file.close()
        print('===================DONE====================')
    

    def run(self):
    '''
    run: the main function to run the program
    - will run a loop until the frontier is empty
    '''
        # print_counter = 0
        while True:

            # get "to be downloaded" url
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
    
            # download the url and get the resp
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            # pass the response and the current url to the scraper
            scraped_urls = scraper.scraper(tbd_url, resp)
            # add the scrapped urls to the frontier
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            # print_counter += 1
            #sleep here for politeness
            time.sleep(self.config.time_delay)
        self.get_report()

        