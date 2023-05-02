from threading import Thread
import re
from inspect import getsource
from utils import get_logger
import scraper
import time
import shelve
import os
import hashlib
import time
from urllib.parse import urlparse

def get_report():
        print('===================GET_REPORT====================')
        filename = 'REPORT.txt'

        if os.path.isfile(filename):
            os.remove(filename)

        with open(filename, 'w') as file:
            try:
                with shelve.open("largest_page.shelve") as db:
                    str_res = 'Largest website is :' + str(db['largest_site']) + '\n'
                    file.write(str_res) #(url, len)
                    print(str_res)
                db.close()
            except KeyError:
                print("Sorry, no largest file found.")
            
            file.write('50 most common words are: '+ '\n')
            print('50 most common words are: '+ '\n')
            with shelve.open("wordCount.shelve") as db: #{words: amount_seen}
                ranked_list = sorted(db.items(), key=lambda el: (-el[1], el[0]), reverse=False)
                for word in ranked_list[:50]:
                    str_write = str(word) + ' '
                    file.write(str_write)
                print(ranked_list[:50], '\n')
            db.close()
            file.write('\n')
            
        amount_unique_page_visited = 0
        ics_domain_dic = {}
        
        with shelve.open("frontier.shelve") as db_f:

            for url_hash in db_f: #hash_url = (url, is_visited)
                cur_url = db_f[url_hash][0] #db = { url_hash = ('http://vision.ics.uci.edu/about', True)}
                url_parts = [part for part in cur_url.split('/') if part] # url_parts will be something like this ['http:', 'vision.ics.uci.edu', 'about']
                is_url_visited = db_f[url_hash][1]
                
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
            
            #file.write(str(len(db_f)) + ' unique URLs found.\n')
            print(str(len(db_f)) + ' unique URLs found.\n')
        
        ics_domain_sorted = sorted(ics_domain_dic.items(), key=lambda el: el[0], reverse=False)
        
        
        #file.write(str(amount_unique_page_visited)+ ' unique pages visited.\n')
        print(str(amount_unique_page_visited)+ ' unique pages visited.\n')
        
        write_ics_domain_len = str(len(ics_domain_dic)) + ' subdomain of isc.uci.edu found\n'
        #file.write(write_ics_domain_len)
        print(write_ics_domain_len)
        
        for domain in ics_domain_sorted:
            #file.write(str(domain[0]) + ' seen ' + str(domain[1]) + ' times \n')
            print(str(domain[0]) + ' seen ' + str(domain[1]) + ' times \n')
            
        file.close()
        print('===================DONE====================')

def is_valid_domain(url):
    netloc = urlparse(url).netloc
    netloc = netloc.lower()
    print(netloc)
    print(bool(".ics.uci.edu" in netloc))
    #print(bool(re.search("ics.uci.edu", netloc)))
    #print(bool(re.search(".ics.uci.edu", netloc)))
    
    #return bool(re.search("cs.uci.edu", netloc)) or bool(re.search("ics.uci.edu", netloc)) or bool(re.search("informatics.uci.edu", netloc)) or bool(re.search("stat.uci.edu", netloc))


#is_valid_domain("http://www.ics.uci.edu/community/news/view_news?id=2224/grad/funding/index/about/search/search_graduate_all.php/community/alumni")#is_valid_domain("www.cs.uci.edu")
#is_valid_domain("http://www.economics.uci.edu/community/news/view_news?id=2224/grad/funding/index/about/search/search_graduate_all.php/community/alumni")
get_report()