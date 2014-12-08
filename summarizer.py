# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

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

# <codecell>

import codecs
redditReadPath = "C:/git/SemanticTextDB/example_code/twitter.csv"
outPath= "C:/git/SemanticTextDB/example_code/twitter_utf8.csv"
with codecs.open(redditReadPath, 'rU', 'UTF-16') as infile:
    with open(outPath + '.utf8', 'wb') as outfile:
        for line in infile:
            outfile.write(line.encode('utf8'))

# <codecell>


