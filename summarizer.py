# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

from __future__ import absolute_import
from __future__ import division, print_function

import os

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer as Summarizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words   

# <codecell>

def summarize(string, summary_length = 1, language = "english"):
    string = string.lower() if string.isupper() else string
    parser = PlaintextParser.from_string(string, Tokenizer(language))
    stemmer = Stemmer(language)
    summarizer = Summarizer(stemmer)
    summarizer.stop_words = get_stop_words(language)

    return ". ".join([str(sentence) for sentence in summarizer(parser.document, summary_length)]) 

