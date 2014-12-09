# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# ##SentimentAnalysis version for SemanticTextDB

# <codecell>

from textblob import TextBlob
from textblob import Word

def sentimentAnalysis(string): 
    NLPObject = TextBlob(unicode(string, 'utf-8'))
    return NLPObject.sentiment.polarity

# <codecell>

def findPositiveDocs():
    return None #implement with Jonas

# <codecell>

def findNegativeDocs():
    return None #implement with Jonas

# <markdowncell>

# ##Other NLP wrapper functions

# <codecell>

def noun_phrases(string):
    nlp = TextBlob(unicode(string, 'utf-8'))
    return nlp.noun_phrases

# <codecell>

def correct_spelling(string):
    nlp = TextBlob(unicode(string, 'utf-8'))
    return nlp.correct()

# <codecell>

def frequency(string, word, case_sensitive = False):
    nlp = TextBlob(unicode(string, 'utf-8'))
    return nlp.words.count(word, case_sensitive)

# <codecell>

def word_counts(string):
    nlp = TextBlob(unicode(string, 'utf-8'))
    return nlp.word_counts

# <codecell>

def translate(string, to_language):
    # Note langauges can be found here:
    # https://cloud.google.com/translate/v2/using_rest#language-params
    nlp = TextBlob(unicode(string, 'utf-8'))
    return nlp.translate(from_lang=from_language, to=to_language)

# <codecell>

def detectLangauge(string):
    # Note langauges can be found here:
    # https://cloud.google.com/translate/v2/using_rest#language-params
    nlp = TextBlob(unicode(string, 'utf-8'))
    return nlp.detect_language()

