# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# ##Import system modules. Import psycopg2 (requires install). This is the python interface driver for the postgresql database.

# <codecell>

import sys, os, time, csv
import psycopg2

# <markdowncell>

# ##Add the directory of our SemanticTextDB library to your absolute path to support import dependencies in our library files.

# <codecell>

mypath = os.path.dirname(os.path.abspath("__file__"))
repositoryLocation = "C:/git/SemanticTextDB"
sys.path.insert(0, os.path.normpath(os.path.join(mypath, 'C:/git/SemanticTextDB')))

import SemanticTextDB as stdb

# <markdowncell>

# ##Create a connection to the database withe the psycopg2.connect() method. Create a cursor to execute operations. Lastly create the SemanticTextDB object. This object is the primary object you will use to interact with, query, perform statistical modelling queries, and interact with the features available with our library.

# <codecell>

conn = psycopg2.connect(database="benchmark", user="Curtis Northcutt", host="18.251.7.99", password="coldnips")
cur = conn.cursor()

# Now create a new SemanticTextDB object based on the underlying DB's state:
my_stdb = stdb.SemanticTextDB(conn, cur)

# <markdowncell>

# ##How to create our document tables in a postgres database

# <codecell>

# Returns a list of all tables in underyling DB:
my_stdb.allTables()

#Returns a list of only document tables in the DB:m
my_stdb.allDocTables()

# <markdowncell>

# ##Delete all tables

# <codecell>

# PRINTS ALL TABLES IN DB:
def allTables(cur):
    cur.execute("select table_name from information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")
    return [x[0] for x in cur.fetchall()]

# Shows at most 5 results from table:
def seeTable(cur, name):
    cur.execute("select * from " + name + ";")
    x = cur.fetchall()
    if len(x) < 5:
        return x
    else:
        return x[:4]


# Deletes all tables from DB:	
def clearDB(cur):
    x = allTables(cur)
    for t in x:
        cur.execute("DROP TABLE " + t + ";")
        cur.execute("COMMIT;")

# <codecell>

clearDB(cur)

# <markdowncell>

# ## Loading in all U.S. Code Laws (over 56,000 laws) fully formatted and substantial length

# <codecell>

cur.execute("END;")
cur.execute("ABORT;")

import warnings
warnings.filterwarnings("ignore")

import re
readPath = "C:/git/SemanticTextDB/example_code/all_US_Law_Codes/"
author = "United States Government"
base_time = time.time()

NUM_INTERTIMES = 1
NUM_TRIALS = 1
NUM_LAWS = 1001

law_trials = []
for i in range(NUM_TRIALS):
    
    clearDB(cur)

    # Delete the document table (iff you want to replace table with same name):
    if 'laws' in my_stdb.document_tables.keys(): #check that the table exists before deleting it
        my_stdb.dropDocTable("laws")

    # Creates a document table (and associated machine-generated tables):
    my_stdb.createDocTable("laws", ['lawTitleNumber text', 'lawSectionNumber text', 'lawName text'],
                       summary = 1, topics = (10,20), entities = None, 
                       sentiment = True, count_words = False, length_count = False, 
                       vs_representations = None, max_word_length = 200,
                       update_increment = 50, new_transaction = False)
    
    intertimes = []
    count = 0
    totaltime = 0
    for filename in os.listdir(readPath):
        count = count + 1
        f = open(readPath + filename,'r')
        fileAsString = f.read()
        result = re.search(r'[0-9].*?\n', fileAsString)
        if result == None:
            continue
            #print fileAsString
        else:
            result = re.sub(r'\n', '', result.group())
            lawTitleNum = filename.split('_')[0]
            lawSectionNum = (re.search(r'.*?\.', result)).group()[:-1]
            lawName = (re.search(r'\..*', result)).group()[2:]

            if lawName == "" or re.search(r'\..*', result).group()[1] != " ": #discard - wrong format
                count = count - 1
                continue
            else:
                #stringToInsert = unicode(fileAsString, 'utf-8')
                fileAsString = re.sub(r'-', ' ', fileAsString)
                fileAsString = re.sub(r'[^a-z ]', '', fileAsString)
                fileAsString = re.sub(r' +', ' ', fileAsString)
                
                start_time = time.time()
                my_stdb.insertDoc(fileAsString, "laws", [lawTitleNum, lawSectionNum, lawName])
                totaltime = totaltime + (time.time() - start_time)
                if count % (NUM_LAWS / NUM_INTERTIMES) == 0:
                    print count
                    intertimes.append(totaltime)
        if count >= NUM_LAWS:
            break
    time_elapsed = time.time() - base_time
    print 'Trial Completed:', i
    print "Estimated Minutes Left:", (time_elapsed * NUM_TRIALS / (i + 1.0) - time_elapsed) / 60.0
    print "Time Elapsed:", time_elapsed
    law_trials.append(intertimes)
    

# <codecell>

my_stdb.persistModels('laws')

# <codecell>

import csv

with open("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\laws_1k_insert_only_NEW_TRANSACTION_TRUE.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerows(law_trials)

# <markdowncell>

# ## Loading up to 2.5 million twitter posts into our database

# <codecell>

def exception_proof_csv_reader(csv_reader):
    '''Wraps a try-except clause around the csv reader to
    prevent throwing of exceptions which will stop reading.'''
    while True: 
      try: 
          yield next(csv_reader) 
      except csv.Error: 
          pass
      continue 

# <codecell>

cur.execute("END;")
cur.execute("ABORT;")

import re
import warnings
warnings.filterwarnings("ignore")

twitterReadPath = "C:/git/SemanticTextDB/example_code/twitter.csv"
base_time = time.time()

NUM_INTERTIMES = 1
NUM_TRIALS = 1
NUM_TWEETS = 5001

twitter_trials = []
for i in range(NUM_TRIALS):
    
    clearDB(cur)
    
    # Delete the document table (iff you want to replace table with same name):
    if 'twitter' in my_stdb.document_tables.keys(): #check that the table exists before deleting it
        my_stdb.dropDocTable("twitter")

    # Creates a document table (and associated machine-generated tables):
    my_stdb.createDocTable("twitter", ['twitterId text', 'location text', 'username text'],
                       summary = 1, topics = (10,20), entities = None, 
                       sentiment = True, count_words = False, length_count = False, 
                       vs_representations = None, max_word_length = 200,
                       update_increment = 50, new_transaction = True)
    
    intertimes = []
    count = 0
    totaltime = 0
    with open(twitterReadPath, 'rU') as csvfile:
        reader = exception_proof_csv_reader(csv.reader(csvfile, delimiter=','))
        for row in reader:
            count = count + 1
            try:           
                twitterID = row.pop(0)
                location = row.pop(0)
                username = row.pop(-1)
                tweet = ', '.join(row)
            except:
                #print "Error occurred at item", count, "skipping insertion."
                count = count - 1
                continue
            
            start_time = time.time()
            
            tweet = re.sub(r'-', ' ', tweet)
            tweet = re.sub(r'[^a-z ]', '', tweet)
            tweet = re.sub(r' +', ' ', tweet)

            start_time = time.time()
            my_stdb.insertDoc(tweet, "twitter", [twitterID, location, username])
            
            totaltime = totaltime + (time.time() - start_time)
            
            if count % (NUM_TWEETS / NUM_INTERTIMES) == 0:
                intertimes.append(totaltime)                    
                print count
            if count >= NUM_TWEETS:
                break
    time_elapsed = time.time() - base_time
    print 'Trial Completed:', i
    print "Estimated Minutes Left:", (time_elapsed * NUM_TRIALS / (i + 1.0) - time_elapsed) / 60.0
    print "Time Elapsed:", time_elapsed     
    twitter_trials.append(intertimes)

# <codecell>

twitter_trials

# <codecell>

import csv

with open("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\twitter_5k_insert_only_NEW_TRANSACTION_TRUE.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerows(twitter_trials)

# <markdowncell>

# ##Benchmarking fetches

# <codecell>

import NLPfunctions as nlpf

NUM_INTERTIMES = 10
NUM_TRIALS = 2
NUM_ITEMS = 5001

trials = []
results = []
for trial in range(NUM_TRIALS):
    intertimes = []
    for level in range(NUM_ITEMS / NUM_INTERTIMES, NUM_ITEMS, NUM_ITEMS / NUM_INTERTIMES):
        start_time = time.time()
        cur.execute("SELECT * FROM twitter NATURAL JOIN twitter_topicprops_10 LIMIT " + str(level))
        results = cur.fetchall()       
        intertimes.append(time.time() - start_time)
        print trial, level
    trials.append(intertimes)

# <codecell>

import csv

with open("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\twitter_5k_read_all_algorithms.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerows(trials)

# <codecell>

import pickle
pickle.dump(results, open("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\twitter_50k.p", "wb"))

# <codecell>

import NLPfunctions as nlpf
import pandas as pd
import summarizer as s
import runOnDocuments as lda

data = pd.read_pickle("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\twitter_50k.p")

NUM_INTERTIMES = 10
NUM_TRIALS = 2
NUM_ITEMS = 001

lda_trials = []
summary_trials = []
sentiment_trials = []

base_time = time.time()

for trial in range(NUM_TRIALS):
    lda_intertimes = []
    summary_intertimes = []
    sentiment_intertimes = []
    for level in range(NUM_ITEMS / NUM_INTERTIMES, NUM_ITEMS, NUM_ITEMS / NUM_INTERTIMES):
        
        data_this_level = data[:level]
        data_this_level = [item[0] for item in data_this_level]#Remove tuple wrapping strings
        
        start_time = time.time()        
        ldaResults = []        
        ldaResults.append(lda.runLDA(data_this_level))
        lda_intertimes.append(time.time() - start_time)
        
        summaryResults = []
        sentimentResults = []        
        
        sentiment_total_time = 0
        summary_total_time = 0
        
        for item in data_this_level:            
            start_time = time.time()
            sentimentResults.append(nlpf.sentimentAnalysis(item))
            sentiment_total_time = sentiment_total_time + (time.time() - start_time)
            
            start_time = time.time()
            summaryResults.append(s.summarize(item))
            summary_total_time = summary_total_time + (time.time() - start_time)
        
        summary_intertimes = [summary_total_time]
        sentiment_intertimes = [sentiment_total_time] 
        
        print trial, level
        
    lda_trials.append(lda_intertimes)
    summary_trials.append(summary_intertimes)
    sentiment_trials.append(sentiment_intertimes)
    time_elapsed = time.time() - base_time
    print 'Trial Completed:', trial + 1
    print "Estimated Minutes Left:", (time_elapsed * NUM_TRIALS / (trial + 1.0) - time_elapsed) / 60.0
    print "Time Elapsed:", time_elapsed

# <markdowncell>

# ##The code below is the same as above, just with lda removed.

# <codecell>

import NLPfunctions as nlpf
import pandas as pd
import summarizer as s
import runOnDocuments as lda

data = pd.read_pickle("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\laws_10k.p")

NUM_INTERTIMES = 10
NUM_TRIALS = 2
NUM_ITEMS = 1001

summary_trials = []
sentiment_trials = []

base_time = time.time()

for trial in range(NUM_TRIALS):
    summary_intertimes = []
    sentiment_intertimes = []
    for level in range(NUM_ITEMS / NUM_INTERTIMES, NUM_ITEMS, NUM_ITEMS / NUM_INTERTIMES):
        
        data_this_level = data[:level]
        data_this_level = [item[0] for item in data_this_level]#Remove tuple wrapping strings
                
        summaryResults = []
        sentimentResults = []        
        
        sentiment_total_time = 0
        summary_total_time = 0
        
        for item in data_this_level:            
            start_time = time.time()
            sentimentResults.append(nlpf.sentimentAnalysis(item))
            sentiment_total_time = sentiment_total_time + (time.time() - start_time)
            
            start_time = time.time()
            summaryResults.append(s.summarize(item))
            summary_total_time = summary_total_time + (time.time() - start_time)
        
        summary_intertimes.append(summary_total_time)
        sentiment_intertimes.append(sentiment_total_time) 
        
        print trial, level
        
    summary_trials.append(summary_intertimes)
    sentiment_trials.append(sentiment_intertimes)
    time_elapsed = time.time() - base_time
    print 'Trial Completed:', trial + 1
    print "Estimated Minutes Left:", (time_elapsed * NUM_TRIALS / (trial + 1.0) - time_elapsed) / 60.0
    print "Time Elapsed:", time_elapsed

# <codecell>

import csv

#with open("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\laws_1k_read_lda_algorithm_on_client.csv", "wb") as f:
#    writer = csv.writer(f)
#    writer.writerows(lda_trials)
 
with open("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\laws_1k_read_summary_algorithm_on_client.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerows(summary_trials)
    
with open("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\laws_1k_read_sentiment_algorithm_on_client.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerows(sentiment_trials)
    

# <codecell>

summary_trials

# <codecell>

reload(lda)

# <markdowncell>

# ##Now that there is data in the database, here is an example query using pyscopg2

# <codecell>

#Single items can be found by their unique id (deterministically the order each tuple was inserted)
cur.execute("select content from twitter_text where id = 65 or id = 66;")
result = cur.fetchall()
result

# <codecell>

statement = "SELECT COUNT(*) FROM twitter_text WHERE content LIKE '%wedding%'"
posCount = my_stdb.semanticSelect('twitter_text', statement, 'positive_only', 0.8)
negCount = my_stdb.semanticSelect('twitter_text', statement, 'negative_only', -0.8)
if negCount != 0:
    ratio = posCount / (1.0 * negCount)
    print "Ratio of support to non-support of Kate Middleton:", ratio
else:
    print "Supporter count:", posCount

# <codecell>

statement = "select content from twitter_text where id < 500;"
my_stdb.semanticSelect('twitter_text', statement, 'positive_only', 0.8)

# <codecell>

statement = "select count(*) from twitter_text where id < 150;"
my_stdb.semanticSelect('twitter_text', statement, 'positive_only', 0.8)

# <codecell>

statement = "select * from laws where id < 5;"
my_stdb.semanticSelect('laws', statement, 'view_summaries', 1)

# <codecell>

import NLPfunctions as nlpf
nlpf.correct_spelling("au ll main ayunan tante,  to london ah? of @zachzachra: Seat sekolahannya si @Indrarbk")

# <codecell>

statement = "select content from twitter_text where id < 10;"
my_stdb.semanticSelect('twitter_text', statement, 'correct_spelling')

# <codecell>

t = time.time()
cur.execute("select * from laws")
#result = cur.fetchall()
print "entire thing:", time.time() - t

# <codecell>

t = time.time()
cur.execute("select * from laws limit 2")
#result = cur.fetchall()
print "limit 2 things only:", time.time() - t

# <codecell>

#If you ever end a transaction while its running, the following two commands be necessary to reset for next transaction.
cur.execute("END;")
cur.execute("ABORT;")

# <markdowncell>

# ##Using our machine-generated tables. Everytime you insert a document, machine generated tables are created. One of these tables is called "tablename_text." This table contains the actual text for each document you've inserted. For example the twitter table and twitter_text table can be joined by id. Thus, you can efficiently find documents of interest in the twitter table, then view the text in the twitter_text table. This allows for efficient manipulation of tables containing many documents without handling all of the text for each document simultaneously. 

# <codecell>

cur.execute("select * from twitter_text limit 5;")
cur.fetchall()

