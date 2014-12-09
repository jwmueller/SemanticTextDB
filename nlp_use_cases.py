# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# ##Determining sentiment of Kate Middleton in London as she marries into royalty.

# <codecell>

import csv
from textblob import TextBlob

# <codecell>

def mycsv_reader(csv_reader): 
  while True: 
    try: 
      yield next(csv_reader) 
    except csv.Error: 
      print "missed value"
      pass
    continue 

# <codecell>

redditReadPath = "C:/git/SemanticTextDB/example_code/twitter.csv"
count = 0
posCount = 0
negCount = 0
with open(redditReadPath, 'rU') as csvfile:
    reader = mycsv_reader(csv.reader(csvfile))
    for row in reader:
        count = count + 1
        try:           
            twitterID = row.pop(0)
            location = row.pop(0)
            username = row.pop(-1)
            tweet = ', '.join(row)
        except:
            #print "Error occurred at item", count, "skipping insertion."
            continue

        #Perform textblob stuff here
        #--------------------------------------------------        
        NLPObject = TextBlob(unicode(tweet, 'utf-8'))
        if NLPObject.sentiment.polarity <= -.8 and 'wedding' in tweet:
            negCount = negCount + 1
            #print tweet
            #print "Sentiment Score:", NLPObject.sentiment.polarity
            #print
        if NLPObject.sentiment.polarity >= .8 and 'wedding' in tweet:
            posCount = posCount + 1
            #print tweet
            #print "Sentiment Score:", NLPObject.sentiment.polarity
            #print
        #--------------------------------------------------
        #print count
        #print "London support of Kate Middleton is at least:", 1.0*posCount/negCount, "supporters for every 1 non-supporter." 
print count
print "London support of Kate Middleton is at least:", 1.0*posCount/negCount, "supporters for every 1 non-supporter." 

# <markdowncell>

# ##Reading in documents and printing their summary

# <codecell>

# -*- coding: utf8 -*-
import os

from __future__ import absolute_import
from __future__ import division, print_function

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer as Summarizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words

#Add the following two lines if you have the following error (NLTK not installed):
#Resource 'tokenizers/punkt/english.pickle' not found.  Please
#use the NLTK Downloader to obtain the resource:
#import nltk
#nltk.download() #Opens a windows GUI installer for NLTK


LANGUAGE = "english"
SENTENCES_COUNT = 1

readPath = "C:/git/SemanticTextDB/example_code/all_US_Law_Codes/"
count = 0

if __name__ == "__main__":
    for filename in os.listdir(readPath):
        count = count + 1
        parser = PlaintextParser.from_file(readPath + filename, Tokenizer(LANGUAGE))
        stemmer = Stemmer(LANGUAGE)

        summarizer = Summarizer(stemmer)
        summarizer.stop_words = get_stop_words(LANGUAGE)
           
        summary = ". ".join([str(sentence) for sentence in summarizer(parser.document, SENTENCES_COUNT)])
        print(summary)
        print("\n")
            
        if count > 3:
            break

