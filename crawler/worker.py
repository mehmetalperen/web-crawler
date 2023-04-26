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
        
    def show_what_you_have(self):
        #This function is only for testing purposes
        print('===================PRINT_TESTING====================')
        amount_ics_domain = 0
        amount_unique_page_visited = 0
        for url_hash in self.frontier.save:
            if 'ics.uci.edu' in self.frontier.save[url_hash][0]:# self.frontier = { url_hash = (url, is_visited)}
                amount_ics_domain += 1
            if self.frontier.save[url_hash][1]:
                amount_unique_page_visited += 1
        
        print(len(self.frontier.save), ' unique URLs found.')
        print(amount_unique_page_visited, ' unique pages visited.')
        
        with shelve.open("largest_page.shelve") as db:
            print('Largest website is :' ,db['largest_site'])
        db.close()
        
        with shelve.open("wordCount.shelve") as db:
            ranked_list = sorted(db.items(), key=lambda el: (-el[1], el[0]), reverse=False)
            print('50 most common words are: ', ranked_list[:50])
        db.close()
        
        print(amount_ics_domain, ' subdomain of isc.uci.edu found')
        print('===================DONE====================')

    def get_report(self):
        print('===================GET_REPORT====================')
        filename = 'REPORT.txt'

        if os.path.isfile(filename):
            os.remove(filename)

        with open(filename, 'w') as file:
            
            with shelve.open("largest_page.shelve") as db:
                file.write('Largest website is :' ,str(db['largest_site']) + '\n') #(url, len)
            db.close()
            
            file.write('50 most common words are: '+ '\n')
            
            with shelve.open("wordCount.shelve") as db: #{words: amount_seen}
                ranked_list = sorted(db.items(), key=lambda el: (-el[1], el[0]), reverse=False)
                for word in ranked_list[:50]:
                    file.write(str(word) + ' ')
            db.close()
            file.write('\n')
            
        amount_ics_domain = 0
        amount_unique_page_visited = 0
        for url_hash in self.frontier.save: #hash_url = (url, is_visited)
            if 'ics.uci.edu' in self.frontier.save[url_hash][0]:# self.frontier = { url_hash = (url, is_visited)}
                amount_ics_domain += 1
            if self.frontier.save[url_hash][1]:
                amount_unique_page_visited += 1
        
        
        file.write(str(len(self.frontier.save)), ' unique URLs found.\n')
        file.write(str(amount_unique_page_visited), ' unique pages visited.\n')
        file.write(str(amount_ics_domain), ' subdomain of isc.uci.edu found\n')
        file.close()
        print('===================DONE====================')
    
    def run(self):
        print_counter = 0
        while True:
            if print_counter >= 10: #just to see what we have. i will remove this
                self.show_what_you_have()
                print_counter = 0
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
    
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            print_counter += 1
            time.sleep(self.config.time_delay)
        self.get_report()

        