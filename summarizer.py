# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import os

from __future__ import absolute_import
from __future__ import division, print_function

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer as Summarizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words   

# <codecell>

def summarize(string, summary_length = 1, language = "english"):
    parser = PlaintextParser.from_string(string, Tokenizer(language))
    stemmer = Stemmer(language)
    summarizer = Summarizer(stemmer)
    summarizer.stop_words = get_stop_words(language)

    return ". ".join([str(sentence) for sentence in summarizer(parser.document, summary_length)]) 

# <codecell>

def selectWithSummaries():
    """
    Performs a select statement on docTable but returns summaries along with each result for viewing.
    """
    #implement with Jonas
    return None

