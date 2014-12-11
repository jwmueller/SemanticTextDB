# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import sys, os, time, csv
import psycopg2

mypath = os.path.dirname(os.path.abspath("__file__"))
repositoryLocation = "C:/git/SemanticTextDB"
sys.path.insert(0, os.path.normpath(os.path.join(mypath, 'C:/git/SemanticTextDB')))

import SemanticTextDB as stdb

conn = psycopg2.connect(database="benchmark", user="Curtis Northcutt", host="18.251.7.99", password="coldnips")
cur = conn.cursor()

# Now create a new SemanticTextDB object based on the underlying DB's state:
my_stdb = stdb.SemanticTextDB(conn, cur)

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

cur.execute("END;")
cur.execute("ABORT;")

# <codecell>

import warnings
warnings.filterwarnings("ignore")

import re
readPath = "C:/git/SemanticTextDB/example_code/all_US_Law_Codes/"
author = "United States Government"
base_time = time.time()

NUM_INTERTIMES = 10
NUM_TRIALS = 5
NUM_LAWS = 1001

law_trials = []
for i in range(NUM_TRIALS):
    
    clearDB(cur)

    # Delete the document table (iff you want to replace table with same name):
    if 'laws' in my_stdb.document_tables.keys(): #check that the table exists before deleting it
        my_stdb.dropDocTable("laws")

    # Creates a document table (and associated machine-generated tables):
    my_stdb.createDocTable("laws", ['lawTitleNumber text', 'lawSectionNumber text', 'lawName text'],
                       summary = None, topics = (10,20), entities = None, 
                       sentiment = False, count_words = False, length_count = False, 
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

import csv

with open("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\laws_1k_BATCH50_insert_lda.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerows(law_trials)

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

NUM_INTERTIMES = 10
NUM_TRIALS = 5
NUM_TWEETS = 5001

twitter_trials = []
for i in range(NUM_TRIALS):
    
    clearDB(cur)
    
    # Delete the document table (iff you want to replace table with same name):
    if 'twitter' in my_stdb.document_tables.keys(): #check that the table exists before deleting it
        my_stdb.dropDocTable("twitter")

    # Creates a document table (and associated machine-generated tables):
    my_stdb.createDocTable("twitter", ['twitterId text', 'location text', 'username text'],
                       summary = None, topics = (10,20), entities = None, 
                       sentiment = False, count_words = False, length_count = False, 
                       vs_representations = None, max_word_length = 200,
                       update_increment = 250, new_transaction = False)
    
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

import csv

with open("C:\\git\\SemanticTextDB\\example_code\\benchmark_results\\twitter_5k_BATCH250_insert_lda.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerows(twitter_trials)

# <codecell>

cur.execute("DROP TABLE twitter_topicmodels")

# <codecell>

my_stdb.allTables()

# <codecell>

twitter_trials

# <codecell>


