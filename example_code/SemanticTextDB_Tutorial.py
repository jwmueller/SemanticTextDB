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

conn = psycopg2.connect(database="semanticdb", user="Curtis Northcutt", host="18.251.7.99", password="coldnips")
cur = conn.cursor()

# <codecell>

# Now create a new SemanticTextDB object based on the underlying DB's state:
my_stdb = stdb.SemanticTextDB(conn, cur)

# <markdowncell>

# ##How to create our document tables in a postgres database

# <codecell>

# Returns a list of all tables in underyling DB:
my_stdb.allTables()

#Returns a list of only document tables in the DB:
my_stdb.allDocTables()

# Delete the document table (iff you want to replace table with same name):
if 'laws' in my_stdb.document_tables.keys(): #check that the table exists before deleting it
    my_stdb.dropDocTable("laws")
    
# Creates a document table (and associated machine-generated tables):
my_stdb.createDocTable("laws", ['lawTitleNumber text', 'lawSectionNumber text', 'lawName text'],
                   summary = None, topics = None, entities = None, 
                   sentiment = True, count_words = False, length_count = False, 
                   vs_representations = None, max_word_length = 200,
                   update_increment = 1, new_transaction = False)

# <markdowncell>

# ## Loading in all U.S. Code Laws (over 56,000 laws) fully formatted and substantial length
# ### This is how you insert a document into the database using our SemanticTextDB library (see last line)

# <codecell>

import re
readPath = "C:/git/SemanticTextDB/example_code/all_US_Law_Codes/"
author = "United States Government"

count = 0
for filename in os.listdir(readPath): #We pre-parsed each law as a txt file.
    count = count + 1
    f = open(readPath + filename,'r')
    fileAsString = f.read()
    result = re.search(r'[0-9].*?\n', fileAsString) #grabs first line of law.
    if result == None:
        print fileAsString
        continue #if first line of law is not properly formatted, txt file is not a law.
    else:
        #Parse lawtitle, number, section, and name from txt file.
        result = re.sub(r'\n', '', result.group())
        lawTitleNum = filename.split('_')[0]
        lawSectionNum = (re.search(r'.*?\.', result)).group()[:-1]
        lawName = (re.search(r'\..*', result)).group()[2:]
        
        if lawName == "" or re.search(r'\..*', result).group()[1] != " ":
            print result #Incorrectly formatted
        else:
            #This is how you insert a document into the database using our SemanticTextDB library
            my_stdb.insertDoc(fileAsString, "laws", [lawTitleNum, lawSectionNum, lawName])
    if count > 1000:
        break

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

# Delete the document table (iff you want to replace table with same name):
if 'twitter' in my_stdb.document_tables.keys(): #check that the table exists before deleting it
    my_stdb.dropDocTable("twitter")
    
# Creates a document table (and associated machine-generated tables):
my_stdb.createDocTable("twitter", ['twitterId text', 'location text', 'username text'],
                   summary = None, topics = None, entities = None, 
                   sentiment = False, count_words = False, length_count = False, 
                   vs_representations = None, max_word_length = 200,
                   update_increment = 1, new_transaction = False)

twitterReadPath = "C:/git/SemanticTextDB/example_code/twitter.csv"
count = 0
NUM_TWEETS_TO_INSERT = 10000

with open(twitterReadPath, 'rU') as csvfile: #all tweets in single csv file
    reader = exception_proof_csv_reader(csv.reader(csvfile, delimiter=','))
    for row in reader:
        count = count + 1
        try:
            #Grab columns of csv to insert as tuple fields
            twitterID = row.pop(0)
            location = row.pop(0)
            username = row.pop(-1)
            tweet = ', '.join(row)
        except:
            #print "Error occurred at item", count, "skipping insertion."
            count = count - 1
            continue
        #This is how you insert a document into the database using our SemanticTextDB library
        my_stdb.insertDoc(tweet, "twitter", [twitterID, location, username])
        
        #Since the csv is extremely large, set a max number of documents to insert.
        if count >= NUM_TWEETS_TO_INSERT:
            break
        
        if count % 500 == 0:
            print count

# <markdowncell>

# ##Give client the power to use all our NLP algorithms within the usual SELECT framework even if they choose not to store these columns.
# ##e.g. finding negative comments on twitter

# <codecell>

statement = "select content from twitter_text;"
my_stdb.semanticSelect('twitter_text', statement, 'negative_only', -.8)

# <markdowncell>

# ##If user chose let the machine curate sentiment during inset (potentially batched), you can just search that column

# <codecell>

cur.execute("select content from laws natural join laws_text where auto_sentiment > .5")
results = [item[0]for item in cur.fetchall()]
count = 0
for item in results:
    count = count + 1
    print item
    if count > 20:
        break

# <markdowncell>

# ##Now that there is data in the database, here are some example queries using pyscopg2

# <codecell>

#Single items can be found by their unique id (deterministically the order each tuple was inserted)
cur.execute("select * from twitter where id = 65;")
cur.fetchall()

#If you ever end a transaction while its running, the following two commands be necessary to reset for next transaction.
cur.execute("END;")
cur.execute("ABORT;")

# <markdowncell>

# ##Using our machine-generated tables. Everytime you insert a document, machine generated tables are created. One of these tables is called "tablename_text." This table contains the actual text for each document you've inserted. For example the twitter table and twitter_text table can be joined by id. Thus, you can efficiently find documents of interest in the twitter table, then view the text in the twitter_text table. This allows for efficient manipulation of tables containing many documents without handling all of the text for each document for non-text queries (like selecting author). 

# <codecell>

cur.execute("select * from twitter_text limit 5;")
cur.fetchall()

# <markdowncell>

# ## Determine political candidate support (or royalty favorability in the case of Kate Middleton)

# <codecell>

statement = "SELECT COUNT(*) FROM twitter_text WHERE content LIKE '%wedding%'"
posCount = my_stdb.semanticSelect('twitter_text', statement, 'positive_only', 0.8)
negCount = my_stdb.semanticSelect('twitter_text', statement, 'negative_only', -0.8)
if negCount != 0:
    ratio = posCount / (1.0 * negCount)
    print "Ratio of support to non-support of Kate Middleton:", ratio
else:
    print "Supporter count:", posCount

# <markdowncell>

# ## Actually be able to view long documents when you query by using the summary feature in semanticSelect. View a document summary so you can actually view the data live, instead of shuffling through extremely long documents.

# <codecell>

statement = "select * from laws where id < 10;"
my_stdb.semanticSelect('laws', statement, 'view_summaries', 1)

# <codecell>


