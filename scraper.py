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

stop_words = set(get_stop_words('en'))
'''
check the classes to avoid global variables
'''
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
    with shelve.open("wordCount.shelve") as db:
        for token in tokens:
            if not token:
                continue
            if token in db:
                db[token] += 1
            else:
                db[token] = 1
    db.close()

def is_longest_page(url, tokens):
    with shelve.open("largest_page.shelve") as db: #largest_site = (url, len)
        if 'largest_site' in db:
            if db['largest_site'][1] < len(tokens):
                db['largest_site'] = (url,len(tokens))
        else:
            db['largest_site'] = (url,len(tokens))
    db.close()


def check_crawl_persmission(url):
    rp = robotparser.RobotFileParser()
    rp.set_url(urljoin(url, '/robots.txt'))
    rp.read()
    return rp.can_fetch('*', url)

def is_absolute_url(url):
    return 'www.' in url or 'http' in url or (len(url) >= 4 and url[:2] == '//') #some abosolute urls start with "//" for example "//swiki.ics.uci.edu/doku.php"

def is_valid_domain(netloc):
    netloc = netloc.lower()
    return bool(re.search("cs.uci.edu", netloc)) or bool(re.search("ics.uci.edu", netloc)) or bool(re.search("informatics.uci.edu", netloc)) or bool(re.search("stat.uci.edu", netloc))

''' Hashing functions start here'''
def threeGramHashNumber(three_gram):
    hashValue = 0
    for string in three_gram:
        for character in string:
            hashValue += ord(character)*31
    return hashValue % 999
    

def getFP(tokens):
    allHashValues = []
    mod4HashValues = []
    grams = get_3grams(tokens)
    
    for item in grams:
        hashNumber = threeGramHashNumber(item)
        
        #only use some of the hashes (hashes with 0%4) to save memory
        if hashNumber % 4 == 0:
            mod4HashValues.append(hashNumber)

    if len(mod4HashValues) == 0:
        return allHashValues
    else:
        return mod4HashValues

def areSimilar(set1, set2):
    #jaccard similarity, see lecture 9.5 page 13
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    similarity = len(intersection) / len(union)
    if(similarity>0.9):
        return True
    else:
        return False
    


#function to convert list of tokens into a list of n-grams (n=3)
def get_3grams(tokens):
    if len(tokens) == 1:
        tokens.append("2ndWordBuffer")
    if len(tokens) == 2:
        tokens.append("3rdWordBuffer")
    ngrams = []
        # create 3-grams using a sliding window of size 3
    for i in range(len(tokens) - 2):
        ngram = tokens[i:i+3]
        ngrams.append(" ".join(ngram))
    return ngrams
'''hashing functions end here'''

def calculate_page_fingerprint(text_content):
    # first web scrape a website for all text in body tags
    # create fingerprint hash using all the text
    hash_method = hashlib.md5()
    text_content_bytes = text_content.encode('utf-8')  # encode string as bytes
    hash_method.update(text_content_bytes)
    return hash_method.hexdigest()
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
    
def is_trap(finger_print):
    with shelve.open("hash_values.shelve") as db:
        if "hash_values" in db:
            for other_finger_print in db['hash_values']:
                if areSimilar(finger_print,other_finger_print):
                    db['hash_values'].append(finger_print)
                    db.close()
                    return True
        else:
            db['hash_values'] = []
            
        db['hash_values'].append(finger_print)
    db.close()
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
    tokens = tokenizer(text_content)
    finger_print = getFP(tokens)
    
    if is_trap(finger_print):
        return []
    
    count_tokens(tokens)
    is_longest_page(url, tokens)
                
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
        if not is_valid_domain(parsed_url.netloc):   
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
