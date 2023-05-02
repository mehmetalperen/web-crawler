import re
import os
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import validators
from stop_words import get_stop_words
from urllib import robotparser
import shelve
from urllib.parse import urljoin
import threading

lock_simhash = threading.Lock()
lock_largest_page = threading.Lock()
lock_word_count = threading.Lock()
lock_common_traps = threading.Lock()

stop_words = set(get_stop_words('en'))

db_word_count = None
db_largest_page = None
db_hash = None
db_common_traps = None

def open_shelves():
    global db_word_count
    db_word_count = shelve.open("wordCount.shelve", writeback=True)

    global db_largest_page
    db_largest_page = shelve.open("largest_page.shelve", writeback=True)

    global db_hash
    db_hash = shelve.open("hash_values.shelve", writeback=True)
    
    global db_common_traps
    db_common_traps = shelve.open("commonTraps.shelve", writeback=True)

def close_shelves():
    db_word_count.close()
    db_largest_page.close()
    db_hash.close()
    db_common_traps.close()

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

def check_crawl_persmission(url):      # can we crawl according to robots.txt
    try:
        rp = robotparser.RobotFileParser()
        rp.set_url(urljoin(url, '/robots.txt'))
        rp.read()
        return rp.can_fetch('*', url)
    except:
        return False

def is_absolute_url(url):
    return 'www.' in url or 'http' in url or (len(url) >= 4 and url[:2] == '//') #some abosolute urls start with "//" for example "//swiki.ics.uci.edu/doku.php"

def is_valid_domain(netloc): # makes sure that it is within the 4 domains & is not a repeated link
    
    netloc = netloc.lower()
    
    if (".cs.uci.edu" in netloc) or (".ics.uci.edu" in netloc) or (".informatics.uci.edu" in netloc) or (".stat.uci.edu" in netloc): # READ THIS !!!!
        return True
    elif ("/cs.uci.edu" in netloc) or ("/ics.uci.edu" in netloc) or ("/informatics.uci.edu" in netloc) or ("/stat.uci.edu" in netloc):
        return True
    else:
        return False
    #return bool(re.search("cs.uci.edu", netloc)) or bool(re.search("ics.uci.edu", netloc)) or bool(re.search("informatics.uci.edu", netloc)) or bool(re.search("stat.uci.edu", netloc))

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
        if hashNumber % 7 == 0:
            mod4HashValues.append(hashNumber)

        allHashValues.append(hashNumber)

    if len(mod4HashValues) == 0:
        return allHashValues
    else:
        return mod4HashValues

def areSimilar(list1, list2):
    size1 = len(list1)
    size2 = len(list2)
    dict1 = {}
    dict2 = {}

    for item in list1: # create a dictionary with the # of occurences of each key
        if item in dict1:
            dict1[item] += 1 
        else:
            dict1[item] = 1

    for item in list2: # create a dictionary with the # of occurences of each key
        if item in dict2:
            dict2[item] += 1
        else:
            dict2[item] = 1
    
    count = 0
    for key in dict1:
        if key in dict2:
            count += min(dict1[key], dict2[key]) # calculate the number of keys that they have in common
    
    percentSimu = count / max(size1, size2)
    # print(percentSimu)
    if percentSimu > 0.70:
        return True
    else:
        return False


#function to convert list of tokens into a list of n-grams (n=3)
def get_3grams(tokens):
    if len(tokens) == 1:
        tokens.append("2ndwordbufferjust")
    if len(tokens) == 2:
        tokens.append("3rdwordbuffer")
    ngrams = []
        # create 3-grams using a sliding window of size 3
    returnGrams =[]
    for i in range(len(tokens) - 2):
        ngrams.append(tokens[i])
        ngrams.append(tokens[i+1])
        ngrams.append(tokens[i+2])
        returnGrams.append(ngrams)
        ngrams = []
    return returnGrams
'''hashing functions end here'''


def scraper(url, resp):
    '''
    This function needs to return a list of urls that are scraped from the response. 
    These urls will be added to the Frontier and retrieved from the cache. 
    These urls have to be filtered so that urls that do not have to be downloaded are not added to the frontier.
    '''
    # return [] if...
        # resp status is not valid (200)
        # resp.raw_response is an empty response (same for raw_response.content)
        # err or length of the url greater than 170, then its most likely a trap.
        # url len is unreasonably long
        # robot.txt does not give us permission to crawl
    if resp.status != 200 or not resp.raw_response or not resp.raw_response.content or len(url) > 170 or not check_crawl_persmission(url): 
        return []   
    
    links = extract_next_links(url, resp) # extract next links now that the url is shown to be valid
    res = [link for link in links if is_valid(link)] # loop through each link in links to see if its valid, if it is add it resp
    return res 

def soup_and_soupText(resp):
    # get the html content from the response
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        return (soup, soup.get_text())
    except:
        return (None, "")
    
def is_trap(finger_print):
    #db_common_traps = shelve.open("commonTraps.shelve", writeback=True)
    #db_hash = shelve.open("hash_values.shelve", writeback=True)

    with lock_common_traps:
        if "commonTraps" in db_common_traps: # if there are hash values
            # print("commonTraps...")
            for other_finger_print in db_common_traps['commonTraps']:  # loop through each one
                if areSimilar(finger_print,other_finger_print): # see if they are similar
                    # print("commonTraps trap!!")
                    return True #if they are similar, return True
        else:
            # print("making common traps...")
            db_common_traps['commonTraps'] = [] # make the hash value "holder" (holds all hash values in a list)
    

        with lock_simhash:
            if "hash_values" in db_hash: # if there are hash values
                print("stored hash traps...")
                for other_finger_print in db_hash['hash_values']:  # loop through each one
                    if areSimilar(finger_print,other_finger_print): # see if they are similar
                        if "commonTraps" in db_common_traps: # ensures the "commonTraps" exists
                            db_common_traps["commonTraps"].append(finger_print)

                        print("Hash_values trap  !!, commonTraps length:", len(db_common_traps["commonTraps"]))
                        return True #if they are similar, return True
            else:
                db_hash['hash_values'] = [] # make the hash value "holder" (holds all hash values in a list)


            # print("Hash value len: ", len(db_hash['hash_values']))
            db_hash['hash_values'].append(finger_print) # append the first hash value
    # if(len(db_hash['hash_values']) > 400): # MAY NEED TO MODIFY, HERE WE ONLY STORE 500 PAGES TO CHECK FOR TRAPS
    #     db_hash.sync()
    #     db_hash.close()
    #     os.remove("hash_values.shelve")
    #     db_hash = shelve.open("hash_values.shelve", writeback=True)
    #     db_hash['hash_values'] = []

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

    text_content = soup.get_text() # get text from soup
    tokens = tokenizer(text_content) # tokenize the text
    if len(tokens) == 0:
        # print("insignificant text content (no tokens)")
        return []

    finger_print = getFP(tokens) # get the fingerprint from the tokens
    if len(finger_print) == 0:
        # print("too small, skip page")
        return [] # too small to check for a trap, probably too small to have useful urls
    
    if is_trap(finger_print): # compares fingerprints with each other
        return []
    
    count_tokens(tokens) # count the tokens and store the count in wordCount.shelve (for most common words)
    is_longest_page(url, len(tokens)) # find the longest page
                
    urls = []
    for link in links:
        cur_link = link['href'] # get link from href
        if 'mailto:' in cur_link: #skip cur_link bc it's not a url
            continue
        if '#' in cur_link: #if fragment found, remove the fragment part
            cur_link= cur_link[:cur_link.index('#')]
        if 'view_news?id=' in cur_link:
            cur_link = cur_link[:(cur_link.index("view_news?id=") + 17)]
        if is_absolute_url(cur_link):
            if '//' == cur_link[0:2]: # add http if missing
                cur_link = 'http:'+cur_link
            urls.append(cur_link) # http is not missing, url is absolute absolute
        else:
            urls.append(url+cur_link) # relative link, combine cur_link with url
            
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
        if not is_valid_domain(parsed_url.netloc) in url:   
            return False
        return not re.match(#make sure we are detecting unwanted types. we were missing ppsx previously. check for more that we are missing
            r".*.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|ppsx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed_url.path.lower())

    except TypeError:
        print ("TypeError for ", parsed_url)
        raise
