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
import DocumentTableInfo

# <markdowncell>

# ##Create a connection to the database withe the psycopg2.connect() method. Create a cursor to execute operations. Lastly create the SemanticTextDB object. This object is the primary object you will use to interact with, query, perform statistical modelling queries, and interact with the features available with our library.

# <codecell>

conn = psycopg2.connect(database="semanticdb", user="Curtis Northcutt", host="18.251.7.99", password="coldnips")
cur = conn.cursor()

# Now create a new SemanticTextDB object based on the underlying DB's state:
stdb = stdb.SemanticTextDB(conn, cur)

# <markdowncell>

# ##How to create our document tables in a postgres database

# <codecell>

# Returns a list of all tables in underyling DB:
stdb.allTables()

#Returns a list of only document tables in the DB:
stdb.document_tables.keys()

# Delete the document table (iff you want to replace table with same name):
if 'laws' in stdb.document_tables.keys(): #check that the table exists before deleting it
    stdb.dropDocTable("laws")
    
# Creates a document table (and associated machine-generated tables):
stdb.createDocTable("laws", ['lawTitleNumber text', 'lawSectionNumber text', 'lawName text'])

# <markdowncell>

# ## Loading in all U.S. Code Laws (over 56,000 laws) fully formatted and substantial length
# ### The time keeping variables are for benchmarking purposes and can be ignored

# <codecell>

import re
readPath = "C:/git/SemanticTextDB/example_code/all_US_Law_Codes/"
author = "United States Government"

count = 0
for filename in os.listdir(readPath):
    count = count + 1
    f = open(readPath + filename,'r')
    fileAsString = f.read()
    result = re.search(r'[0-9].*?\n', fileAsString)
    if result == None:
        print fileAsString
    else:
        result = re.sub(r'\n', '', result.group())
        lawTitleNum = filename.split('_')[0]
        lawSectionNum = (re.search(r'.*?\.', result)).group()[:-1]
        lawName = (re.search(r'\..*', result)).group()[2:]
        
        if lawName == "" or re.search(r'\..*', result).group()[1] != " ":
            print result #Incorrectly formatted
        else:
            stdb.insertDoc(fileAsString, "laws", [lawTitleNum, lawSectionNum, lawName])

# <markdowncell>

# ## Loading up to 2.5 million twitter posts into our database
# ### The time keeping variables are for benchmarking purposes and can be ignored

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
if 'twitter' in stdb.document_tables.keys(): #check that the table exists before deleting it
    stdb.dropDocTable("twitter")
    
# Creates a document table (and associated machine-generated tables):
stdb.createDocTable("twitter", ['twitterId text', 'location text', 'username text'])

redditReadPath = "C:/git/SemanticTextDB/example_code/twitter.csv"
count = 0
NUM_TWEETS_TO_INSERT = 1000000

with open(redditReadPath, 'rU') as csvfile:
    reader = exception_proof_csv_reader(csv.reader(csvfile, delimiter=','))
    for row in reader:
        break
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
        stdb.insertDoc(tweet, "twitter", [twitterID, location, username])
        if count >= NUM_TWEETS_TO_INSERT:
            break

# <codecell>

cur.execute("END;")
cur.execute("ABORT;")

# <codecell>

stdb.allTables()

# <codecell>

cur.execute("select * from twitter_text limit 5;")
cur.fetchall()

# <codecell>

cur.execute("select * from cgntestdoctable1_text where id = 65;")
cur.fetchall()

# <codecell>

import datetime
a= datetime.time()

# <codecell>

datetime.time() - a

# <codecell>


