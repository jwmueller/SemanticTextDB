# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

from textblob import TextBlob

# <codecell>

import csv 

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
import csv
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

