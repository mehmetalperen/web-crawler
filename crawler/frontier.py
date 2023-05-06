import os
import shelve
import time

from threading import Thread, RLock
from queue import Queue, Empty            # we should change it to a queue: https://stackoverflow.com/questions/1296511/efficiency-of-using-a-python-list-as-a-queue 

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = Queue()              # add urls to this queue
 
'''
    the below: if it is restart, delete the file
               if it is not restart, attempt to open it
'''
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        #--------------------------------
        if not os.path.exists(self.config.word_count) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.word_count}, "
                f"starting from seed.")
        elif os.path.exists(self.config.word_count) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.word_count}, deleting it.")
        #--------------------------------
        if not os.path.exists('largest_page.shelve') and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {'largest_page.shelve'}, "
                f"starting from seed.")
        elif os.path.exists('largest_page.shelve') and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {'largest_page.shelve'}, deleting it.")
            os.remove('largest_page.shelve')
        #--------------------------------
        if not os.path.exists('hash_values.shelve') and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {'hash_values.shelve'}, "
                f"starting from seed.")
        elif os.path.exists('hash_values.shelve') and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {'hash_values.shelve'}, deleting it.")
            os.remove('hash_values.shelve')
            #-------------------------
        self.save = shelve.open(self.config.save_file)                      # save file = frontier.shelve (dictionary-like object)
        
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
'''
    _parse_save_file: checks the save file and prints the tbd_count and the total urls discovered
'''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.put(url)        # put from Queue library
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            if not self.to_be_downloaded.empty():            # queue of urls cannot be empty
                return self.to_be_downloaded.get()  
        except IndexError:
            return None


    def add_url(self, url):
'''
    add_url: hashes teh url and adds the hash to self.save
    - also places the url in the to_be_downloaded queue
'''
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.to_be_downloaded.put(url)                  # put() from Queue library
    
    def mark_url_complete(self, url):
'''
    mark_url_complete: find the url in self.save and marks it as complete (meaning it has been crawled)
    - also checks to make sure it is in self.save as extra protection
'''
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen. Because it once pulled the URL from the saved URLs
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()
