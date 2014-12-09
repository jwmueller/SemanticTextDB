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
    nlp = TextBlob(string)
    return nlp.noun_phrases

# <codecell>

def correct_spelling(string):
    nlp = TextBlob(string)
    return nlp.correct()

# <codecell>

def frequency(string, word, case_sensitive = False):
    nlp = TextBlob(string)
    return nlp.words.count(word, case_sensitive)

# <codecell>

def word_counts(string):
    nlp = TextBlob(string)
    return nlp.word_counts

# <codecell>

def translate(string, to_language):
    # Note langauges can be found here:
    # https://cloud.google.com/translate/v2/using_rest#language-params
    nlp = TextBlob(string)
    return nlp.translate(from_lang=from_language, to=to_language)

# <codecell>

def detectLangauge(string):
    # Note langauges can be found here:
    # https://cloud.google.com/translate/v2/using_rest#language-params
    nlp = TextBlob(string)
    return nlp.detect_language()

# <codecell>

nlp = TextBlob("Jonas is an amazing friend and I think he is great!")
nlp.word_counts

# <codecell>

sentimentAnalysis("Jonas is an amazing friend and I think he is great!")

# <codecell>

sentimentAnalysis("Jonas is an amazing friend! and I think he is great!!!!!!!!")

# <codecell>

sentimentAnalysis("Jonas is an amazing friend!! and I think he is great!")

# <codecell>

sentimentAnalysis("Jonas is an amazing friend!!! and I think he is great!")

# <codecell>

sentimentAnalysis("Fuck you!!!!")

# <codecell>

sentimentAnalysis("Fuck yes!!!!")

# <codecell>

sentimentAnalysis("i love my shitty life!!!!")

# <codecell>

sentimentAnalysis("i love my fucking life!!!!")

# <codecell>


