import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import validators
from stop_words import get_stop_words
from urllib import robotparser
import shelve
from urllib.parse import urljoin
import hashlib
from simhash import Simhash
import threading

lock_simhash = threading.Lock()
lock_largest_page = threading.Lock()
lock_word_count = threading.Lock()

stop_words = set(get_stop_words('en'))

db_word_count = None
db_largest_page = None
db_simhash = None

def open_shelves():
    global db_word_count
    db_word_count = shelve.open("wordCount.shelve", writeback=True)

    global db_largest_page
    db_largest_page = shelve.open("largest_page.shelve", writeback=True)

    global db_simhash
    db_simhash = shelve.open("hash_values.shelve", writeback=True)

def close_shelves():
    db_word_count.close()
    db_largest_page.close()
    db_simhash.close()

def tokenizer(page_text_content):
    tokens = []
    
    cur_word = ""
    for ch in page_text_content: #read line character by character
        if ch in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890': #check if character is in english alphabet or a number
            cur_word += ch.lower() #convert that ch to lower case and add it to the cur_word
        elif cur_word in stop_words:
            cur_word = ""        
        elif len(cur_word) > 1: #we do not want single charecters. for example James's would give us "James" and "s" if we dont do this 
            tokens.append(cur_word) # add cur word to token list 
            cur_word = "" #reset cur_word
        else:
            cur_word = ''
            
    if len(cur_word) > 1 and cur_word not in stop_words: #if cur_word is not empty, we need to add it to the list bc we do not wanna skip the last word unadded
        tokens.append(cur_word)
    return tokens

def count_tokens(tokens):
    with lock_word_count:
        for token in tokens:
            if not token:       # empty somehow
                continue
            if token in db_word_count:
                db_word_count[token] += 1
            else:
                db_word_count[token] = 1
        #db_word_count.sync()

def is_longest_page(url, num_tokens):
    with lock_largest_page:
        if 'largest_site' in db_largest_page:
            if db_largest_page['largest_site'][1] < num_tokens:
                db_largest_page['largest_site'] = (url, num_tokens)
                #db_largest_page.sync()
        else:
            db_largest_page['largest_site'] = (url, num_tokens)
            #db_largest_page.sync()

def check_crawl_persmission(url):           # can we crawl according to robots.txt
    rp = robotparser.RobotFileParser()
    rp.set_url(urljoin(url, '/robots.txt'))
    rp.read()
    return rp.can_fetch('*', url)

def is_absolute_url(url):
    return 'www.' in url or 'http' in url or (len(url) >= 4 and url[:2] == '//') #some abosolute urls start with "//" for example "//swiki.ics.uci.edu/doku.php"

def is_valid_domain(netloc):
    netloc = netloc.lower()
    return bool(re.search("cs.uci.edu", netloc)) or bool(re.search("ics.uci.edu", netloc)) or bool(re.search("informatics.uci.edu", netloc)) or bool(re.search("stat.uci.edu", netloc))

def scraper(url, resp):
    '''
    This function needs to return a list of urls that are scraped from the response. 
    These urls will be added to the Frontier and retrieved from the cache. 
    These urls have to be filtered so that urls that do not have to be downloaded are not added to the frontier.
    '''
    if resp.status != 200 or not resp.raw_response or not resp.raw_response.content or len(url) > 170 or not check_crawl_persmission(url): # return [] if err or lenght of the url greater than 150, then its most likely a trap.
        return []   
    
    links = extract_next_links(url, resp)
    res = [link for link in links if is_valid(link)]
    return res 

def soup_and_soupText(resp):
    soup = BeautifulSoup(resp.raw_response.content, 'html.parser') #get the html content from the response
    return (soup, soup.get_text())
    
def is_trap(simhash_fp):  # fp for fingerprint
    with lock_simhash:
        if "hash_values" in db_simhash:
            for other_fp in db_simhash['hash_values']:
                similarity = other_fp.distance(simhash_fp)    # gives values 0-64, 0 meaning exact match, 64 meaning completely different
                if similarity <= 12.8:     # anything more than 20% similarity
                    return True            # it is a trap
        else:
            db_simhash['hash_values'] = set()
            db_simhash['hash_values'].add(simhash_fp)
            #db_simhash.sync()
    # db.close()
    return False


def extract_next_links(url, resp):
    '''
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content'''
    
    soup, text_content = soup_and_soupText(resp)
    if not text_content: #if there is no content in the site, we dont want to crawl it. 
        return []        
    links = soup.find_all('a', href=True) #all the links from the html content
    text_content = soup.get_text()
    
    finger_print = Simhash(text_content)    # use simhash to create simhash fingerprint
    
    if is_trap(finger_print):
        return []
    
    tokens = tokenizer(text_content)
    count_tokens(tokens)                       # update token counts of all pages visited so far in wordCount.shelve
    is_longest_page(url, len(tokens))          # update largest_page.shelve
                
    urls = []
    for link in links:
        cur_link = link['href']
        if 'mailto:' in cur_link:
            continue
        if '#' in cur_link: #if fragment found, remove the fragment part
            cur_link= cur_link[:cur_link.index('#')]
        
        if is_absolute_url(cur_link):
            if '//' == cur_link[0:2]: # add http if missing
                cur_link = 'http:'+cur_link
            urls.append(cur_link) #http is not missing, url is absolute absolute
        else:
            urls.append(url+cur_link) #relative link, combine cur_link with url
            
    return urls
    
def is_valid(url):
    """
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    Notes:
        -- domains: .ics.uci.edu, .cs.uci.edu, .informatics.uci.edu, .stat.uci.edu 
            Question: do we filter out all except ics.uci.edu? (github README says this)
    """
    try:
        if not validators.url(url):
            return False
        parsed_url = urlparse(url)        # https://docs.python.org/3/library/urllib.parse.html 
        
        if not(parsed_url.scheme == 'http' or parsed_url.scheme == 'https'):
            return False
        if not is_valid_domain(parsed_url.netloc) or "www.ics.uci.edu/community/news/view_news/" in url:   
            return False
        return not re.match(
            r".*.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed_url.path.lower())

    except TypeError:
        print ("TypeError for ", parsed_url)
        raise
