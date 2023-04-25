from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
import shelve

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
        print('===================PRINTING====================')
        amount_ics_domain = 0
        amount_unique_page_visited = 0
        for url_hash in self.frontier.save:
            if 'ics.uci.edu' in self.frontier.save[url_hash][0]:# self.frontier = { url_hash = (url, is_visited)}
                amount_ics_domain += 1
            if self.frontier.save[url_hash][1]:
                amount_unique_page_visited += 1
        
        with shelve.open("wordCount.shelve") as db:
            for key in db:
                print(key, ' => ', db[key])
        db.close()
            

        print(len(self.frontier.save), ' unique URLs found.')
        print(amount_unique_page_visited, ' unique pages visited.')
        print(amount_ics_domain, ' subdomain of isc.uci.edu found')
        print('===================DONE====================')

    def run(self):
        print_counter = 0
        while True:
            if print_counter >= 10:
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
        

        