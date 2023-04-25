import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import validators
from stop_words import get_stop_words
import urllib.robotparser as urobot
import shelve
import os

stop_words = set(get_stop_words('en'))
tokenized_sites = {} # site_url : [] #list of tokens found in that site => for question 2 and 3
ics_subdomains = set() #for question 5
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
            
    if cur_word and cur_word not in stop_words: #if cur_word is not empty, we need to add it to the list bc we do not wanna skip the last word unadded
        tokens.append(cur_word)
    return tokens

def count_tokens(tokens):
    with shelve.open("allcontent.shelve") as db:
        if 'count_all' not in db:
            db['count_all'] = {}
            
        # for token in tokens:

        #     if token not in db['count_all']:
        #         db['count_all'][token] = 1
        #         print('db[token]', db['count_all'][token])
        #     else:
        #         print('db[token]', db['count_all'][token])
        #         db[token] += 1
    db.close()
    

def find_longest_page():
    longet_page_url = ''
    longet_page_len = 0
    for site_url in tokenized_sites:
        if len(tokenized_sites[site_url]) > longet_page_len:
            longet_page_url = site_url
            longet_page_len = len(tokenized_sites[site_url])
            
    return longet_page_url


def common_words():
    counter = {}
    for site_url in tokenized_sites:
        for word in tokenized_sites[site_url]:
            if word not in counter:
                counter[word] = 1
            else:
                counter[word] += 1
                
    most_common_words = sorted(counter.items(), key=lambda el: (-el[1], el[0]), reverse=False)#Source: https://stackoverflow.com/questions/1915564/python-convert-a-dictionary-to-a-sorted-list-by-value-instead-of-key

    return most_common_words[0:50]
        
def check_crawl_persmission(url):
    rp = urobot.RobotFileParser()
    rp.set_url(url + "/robots.txt")
    rp.read()
    return rp.can_fetch("*", url)

def scraper(url, resp):
    '''
    This function needs to return a list of urls that are scraped from the response. 
    These urls will be added to the Frontier and retrieved from the cache. 
    These urls have to be filtered so that urls that do not have to be downloaded are not added to the frontier.
    '''
    if resp.status != 200  or len(url) > 150 or not check_crawl_persmission(url): # return [] if err or lenght of the url greater than 150, then its most likely a trap.
        return []   
    
    links = extract_next_links(url, resp)
    res = [link for link in links if is_valid(link)]
    return res 


def is_absolute_url(url):
    return 'www.' in url or 'http' in url or (len(url) >= 4 and url[:2] == '//') #some abosolute urls start with "//" for example "//swiki.ics.uci.edu/doku.php"

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
    
    soup = BeautifulSoup(resp.raw_response.content, 'html.parser') #get the html content from the response
    links = soup.find_all('a', href=True) #all the links from the html content
    text_content = soup.get_text()
    
    if not text_content: #if there is no content in the site, we dont want to crawl it. 
        return []        
    # tokenized_sites[url] = tokenizer(text_content)
    
    tokens = tokenizer(text_content)
    count_tokens(tokens)
    # Access the data in the shelve file
                
    urls = []
    # print('URL: ', url)
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
    
def is_valid_domain(netloc):
    netloc = netloc.lower()
    return bool(re.search("cs.uci.edu", netloc)) or bool(re.search("ics.uci.edu", netloc)) or bool(re.search("informatics.uci.edu", netloc)) or bool(re.search("stat.uci.edu", netloc))
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
