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
''' 
    tokenizer: Takes the page_text_content returned by BeautifulSoup (as a string) and parses this text into tokens.
    - Tokens are a list of strings who's length that is greater than 1.
'''
    tokens = []
    
    cur_word = ""
    for ch in page_text_content: #read line character by character
        if ch in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890': #check if character is in english alphabet or a number
            cur_word += ch.lower() #convert that ch to lower case and add it to the cur_word
        elif cur_word in stop_words:
            cur_word = ""
        # if it is not a stop_word && is not alphanumeric && the length is greater than one, then we can append it to the list
        elif len(cur_word) > 1: #we do not want single charecters. for example James's would give us "James" and "s" if we dont do this 
            tokens.append(cur_word) # add cur word to token list 
            cur_word = "" #reset cur_word
        else:
            cur_word = ''
    
    # if cur_word is not empty, we need to add it to the list bc we do not wanna skip the last word unadded
    if len(cur_word) > 1 and cur_word not in stop_words: 
        tokens.append(cur_word)
    return tokens


def count_tokens(tokens):
'''
    count_tokens: for each token in tokens, add it to the count of words for keeping track of the 50 most common words
    - Open wordCount.shelve, then incriment each counter if it exists
    - if it does not exist, then create the item in the dictionary and set its count equal to one
'''
    with shelve.open("wordCount.shelve") as db:
        for token in tokens:
            if not token:
                continue
            if token in db:
                db[token] += 1
            else:
                db[token] = 1
    db.sync()
    db.close()


def is_longest_page(url, tokens):
'''
    is_longest_page: updates the longest page by checking it against the # of tokens in the current url
    - check the length of the list of tokens from the current largest count in largest_page.shelve
    - if the cureent page's toekns are larger, update the current largest site to (url,len(tokens))
'''
    with shelve.open("largest_page.shelve") as db: #largest_site = (url, len)
        if 'largest_site' in db:
            if db['largest_site'][1] < len(tokens):
                db['largest_site'] = (url,len(tokens))
        else:
            db['largest_site'] = (url,len(tokens))
    db.sync()
    db.close()



def check_crawl_persmission(url):
'''
    check_crawl_permission:
    - checks the robot.txt file with robotparser
    - rp.can_fetch: sees if all cralwers can crawl that url, returns a bool (true if can crawl, false if not)
    - also catches if there was an error reading robots.txt, if there was, return False
'''
    try:
        rp = robotparser.RobotFileParser()
        rp.set_url(urljoin(url, '/robots.txt')) # this might be err
        rp.read()
        return rp.can_fetch('*', url)   
    except:
        return False



def is_absolute_url(url):
'''
    is_absolute_url:
    - checks if the url is an absolute url
'''
    return 'www.' in url or 'http' in url or (len(url) >= 4 and url[:2] == '//') #some abosolute urls start with "//" for example "//swiki.ics.uci.edu/doku.php"

# checks to make sure that each url is within the valid domains we can search
# note: we add the dot (".") at the beginning to make sure domains like "economics" doesn't get added
def is_valid_domain(netloc):
'''
    is_valid_domain:
    - checks to make sure that each url is within the valid domains we can search
    - note: we add the dot (".") at the beginning to make sure domains like "economics" doesn't get added
'''
    netloc = netloc.lower()
    return bool(".cs.uci.edu" in netloc) or bool(".ics.uci.edu" in netloc) or bool(".informatics.uci.edu" in netloc) or bool(".stat.uci.edu" in netloc)


# def calculate_page_fingerprint(text_content):
#     # first web scrape a website for all text in body tags
#     # create fingerprint hash using all the text
#     hash_method = hashlib.md5()
#     text_content_bytes = text_content.encode('utf-8')  # encode string as bytes
#     hash_method.update(text_content_bytes)
#     return hash_method.hexdigest()


def scraper(url, resp):
    '''
    scraper: returns a list of urls that are scraped from the response. 
    - these urls will be added to the Frontier and retrieved from the cache. 
    - These urls have to be filtered so that urls that do not have to be downloaded are not added to the frontier.
        - list of links is then passed ot is_valid(link) to make sure it is valid before adding it to res
    '''
    if resp.status != 200 or not resp.raw_response or not resp.raw_response.content or len(url) > 170 or not check_crawl_persmission(url): # return [] if err or lenght of the url greater than 150, then its most likely a trap.
        return []   
    
    links = extract_next_links(url, resp)
    res = [link for link in links if is_valid(link)]
    return res 


def soup_and_soupText(resp):
'''
    soup_and_soupText: gets the html content from the response and returns the soup object & the page text content
'''
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser') #get the html content from the response
        return (soup, soup.get_text()) #return the soup object and the page text content
    except:
        return (None, '') #if there is an exception, catch it and return none and empty string as tuple 



def is_trap(text_content):
'''
    is_trap: if it is a trap, return true, else false
    - opens hash_values.shelve & compares the fingerprints from the file to the current fingerprint
    - if they are similar according to our threshold, return true
'''
    db = shelve.open("hash_values.shelve", writeback=True)
    finger_print = Simhash(text_content)
    if "hash_values" in db: # if there are hash values
        for other_fingerprint in db['hash_values']:  # loop through each one
            other_fingerprint = other_fingerprint[0]            
            similarity = finger_print.distance(other_fingerprint) # see if they are similar
            if similarity <= 12: # 0 = same, 64 = different ran 17, 30, 12, 15, 13 (13 gave 11k) (never finished 12 thinking it got into a trap. it may or may not be true)
                db.close()
                return True #if they are similar, return True
    else:
        db['hash_values'] = [] # make the hash value "holder" (holds all hash values in a list)

    db['hash_values'].append([finger_print]) # append the first hash value as a string
    db.sync()
    db.close()
    return False



# Extracts text content and the links from the current page
# then tokenizes the text_content, count the tokens, and update the longest page
def extract_next_links(url, resp):# url = url of the page we are scrapping. Resp = the object we get from http request
    '''
    # extract_next_links: extracts the links from a page, formats them correctly, and returns them in a list
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
    
    
    if is_trap(text_content):
        return []
    
    #get tokens
    tokens = tokenizer(text_content)
    #count tokens
    count_tokens(tokens)
    #update longest page to see if this page is the longest page
    is_longest_page(url, tokens)
    

    # for every link that you found on the page, do the following before addding it to
    # the list of URLS
    urls = []
    for link in links:
        cur_link = link['href']
        #if its a mail link (a link to email someone), skip this iteration
        if 'mailto:' in cur_link:
            continue
        #if fragment found, remove the fragment part
        if '#' in cur_link: 
            cur_link= cur_link[:cur_link.index('#')]
        #if the url is absolute, see if the "http" is missing, if it is, add it
        if is_absolute_url(cur_link):
            if '//' == cur_link[0:2]: # add http if missing
                cur_link = 'http:'+cur_link
            urls.append(cur_link) #http is not missing, url is absolute absolute
        #if it is not an absolute url, add the current link to the url that you originally passed to the function
        else:
            urls.append(url+cur_link) #relative link, combine cur_link with url
            
    return urls


def is_valid(url):
    """
    is_valid: checks to makes ure the current URL passed is valid
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    Notes:
        -- valid domains: .ics.uci.edu, .cs.uci.edu, .informatics.uci.edu, .stat.uci.edu 
    """
    try:
        if not validators.url(url):
            return False
        parsed_url = urlparse(url)        # https://docs.python.org/3/library/urllib.parse.html 
        # the parsed url does not contain a propper "http" / "https"
        if not(parsed_url.scheme == 'http' or parsed_url.scheme == 'https'):
            return False
        # checks to make sure that each url is within the valid domains we can search
        if not is_valid_domain(parsed_url.netloc):   
            return False
        #makes sure the link does not end in the following
        return not re.match(
            r".*.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|xml|mov|rdf|mpeg|ram|json|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|py|rss|eps|atom|n3|tex|ttl|ppt|pptx|ppsx|doc|docx|xls|xlsx|names"
            + r"|data|dat|bat|img|odc|nrg|cmd|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|owl|nt|gpx|vmdk|xmp|raw|aac|svg|cnf|m4a|mpg|tgz|sha1"
            + r"|cr2|rw2|dng|arw|kmz|kml|thmx|nef|mso|flv|arff|rtf|jar|csv"
            + r"|rm|dbf|tmp|temp|old|archive|log|bak|sqlite|backup|tab|sql|accdb|mdb|ods|tsv|smil|wmv|swf|wma|zip|rar|gz)$", parsed_url.path.lower())

    except TypeError:
        print ("TypeError for ", parsed_url)
        raise
