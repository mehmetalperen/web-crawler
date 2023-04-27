import re
import validators

from stop_words import get_stop_words


stop_words = set(get_stop_words('en'))

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


myStr = '''Hello thesis this is going to be
a great day own ownership Jame's
full of fun and funny up you're
the chicken ate the hen
attitude is great'''
tokens = tokenizer(myStr)
print(tokens)
print(stop_words)