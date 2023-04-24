import re
import nltk
nltk.download('stopwords')
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import validators

# recives string of text and returns a map with the 
# english nonStopwords and their frequency
# based on code from Cole Hajek's CS 121 assignment 1
def tokenizer(string):
    from nltk.corpus import stopwords
    sWords = set(stopwords.words('english'))        #set up set of all english stop words

    tokens = []
    #set regex "pattern" to accept all strings of alphanumeric characters
    #+O(n) time for the regex
    pattern = r'[0-9a-zA-Z_]+'
    tokens = re.findall(pattern, string, flags=re.I)

    counter = {}
    for item in tokens:
        lowerString = item.lower()
        if item in sWords:          #if its a stopWord, don't add it to the counter.
            continue
        elif lowerString in counter:
            counter[lowerString] += 1
        else:
            counter[lowerString] = 1
    return counter      
    

def scraper(url, resp):
    '''
    This function needs to return a list of urls that are scraped from the response. 
    These urls will be added to the Frontier and retrieved from the cache. 
    These urls have to be filtered so that urls that do not have to be downloaded are not added to the frontier.
    '''
    print('===========================TESTING START=======================') #FEEL FREE TO REMOVE THIS. 
    print('URL:', url)#FEEL FREE TO REMOVE THIS. 
    print('RESP: ', resp)#FEEL FREE TO REMOVE THIS. 
    links = extract_next_links(url, resp)
    print('links: ', links)#FEEL FREE TO REMOVE THIS. 
    res = [link for link in links if is_valid(link)]
    print('--------------------------------------------------------------')
    print('RES: ', res)
    print('===========================TESTING DONE=======================')#FEEL FREE TO REMOVE THIS. 
    return res 


def is_absolute_url(url):
    return 'www.' in url or 'http' in url or (len(url) >= 4 and url[:2] == '//') #some abosolute urls start with "//" for example "//swiki.ics.uci.edu/doku.php"

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.status != 200: #if err, then return empty list
        return []
    
    soup = BeautifulSoup(resp.raw_response.content, 'html.parser') #get the html content from the response
    links = soup.find_all('a', href=True) #all the links from the html content
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
    
def is_valid_domain(netloc):
    netloc = netloc.lower()
    return bool(re.search("cs.uci.edu", netloc)) or bool(re.search("ics.uci.edu", netloc)) or bool(re.search("informatics.uci.edu", netloc)) or bool(re.search("stat.uci.edu", netloc))
def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    """
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
