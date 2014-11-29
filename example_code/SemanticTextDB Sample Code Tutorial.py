# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import os
import psycopg2
conn = psycopg2.connect(database="semanticdb", user="Curtis Northcutt", host="18.251.7.99", password="coldnips")
cur = conn.cursor()

# <codecell>

classPath = "C:/git/SemanticTextDB/SemanticTextDB.py"
dtablePath = "C:/git/SemanticTextDB/DocumentTableInfo.py"
# Now read in the classes into Python:
execfile(classPath)
execfile(dtablePath)

# <codecell>

# Now create a new SemanticTextDB object based on the underlying DB's state:
stdb = SemanticTextDB(conn, cur)

# List all tables in underyling DB:
stdb.allTables()

# Delete the document table (iff you want to replace table with same name):
stdb.dropDocTable("cgntestdoctable1")
# Creates a document table (and associated machine-generated tables):
stdb.createDocTable("cgntestdoctable1", ['lawTitleNumber text', 'lawSectionNumber text', 'lawName text'])

# Insert (there is also a method for bulk inserting many documents in a single Transaction):
#stdb.insertDoc("cool text", "cgntestdoctable1")

# View tables:
#cur.execute("select * from cgntestdoctable1;")
#cur.fetchall()
#cur.execute("select * from cgntestdoctable1_text;")
#cur.fetchall()


# <codecell>

stdb.allTables()

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
        print count,
        result = re.sub(r'\n', '', result.group())
        lawTitleNum = filename.split('_')[0]
        lawSectionNum = (re.search(r'.*?\.', result)).group()[:-1]
        lawName = (re.search(r'\..*', result)).group()[2:]
        
        if lawName == "" or re.search(r'\..*', result).group()[1] != " ":
            print result #Incorrectly formatted
        else:
            stdb.insertDoc(fileAsString, "cgntestdoctable1", [lawTitleNum, lawSectionNum, lawName])

# <codecell>

stdb.allTables()

# <codecell>

cur.execute("select count(*) from cgntestdoctable1;")
cur.fetchall()

# <codecell>

cur.execute("select * from cgntestdoctable1_text where id = 65;")
cur.fetchall()

# <codecell>

3+3

# <codecell>

"hello"[8:]

# <codecell>

 w

