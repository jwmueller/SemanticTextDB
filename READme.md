==========================================
SemanticTextDb - When NLP meets databases.
==========================================

A database for document-storage/retrieval with automated curation
and structure discovery, so that documents may be efficiently organized 
and queried not only based on human-labeled attributes/metadata, but also using 
a variety of optional automatically-inferred latent features including: 
semantics, topics, sentiment, eloquence, and entities of interest. 

Inference of these properties is done using various statistical models and 
NLP algorithms stored and run inside the database.

Installation
------------

You will need Python 3.2 and pip installed.

See next two sections for server and client installation.

Server (where postgresql database is running) Installation
-----------------------------------------------------------------
The postgresql server requires:
1. Python 3 installed
2. PL/Python installed. This is installed as follows in postgresql on the server:

`CREATE OR REPLACE LANGUAGE plypython3u;`

You can also just run this (from the client) in python using the psycopg2 library as follows:

`cur = conn.cursor() #where conn is the psycopg2.connect() connection to the database`
`cur.execute("CREATE OR REPLACE LANGUAGE plypthon3u;")`

Other library dependencies:

1. numpy (pip install numpy)

    `$ [sudo] pip install numpy`

2. scipy (pip install scipy)

    `$ [sudo] pip install scipy`

3. pyscopg2 which can be installed with pip as follows:

    `$ [sudo] pip install psycopg2`


Client Installation - clients use the SemanticTextDB library built on psycopg2 python interface driver.
---------------------------------------------------------------

Clients using SemanticTextDB requires:

1. psycopg2

    `$ [sudo] pip install psycopg2`

2. NLTK - download within python terminal. A GUI will pop-up. Click download.

    `import nltk`

    `nltk.download()`

3. textblob (and its dependencies)

    `$ [sudo] pip install -U textblob`

4. sumy (and its dependencies)

    `$ [sudo] pip install sumy`


Using SemanticTextDB
--------------------

Simply clone the repo and refer to SemanticTextDB_Tutorial.py for documentation.

With respect to viewing the tutorial, we STRONGLY recommend using iPython Notebook for viewing the SemanticTextDB_Tutorial.py.
Use SemanticTextDB_Tutorial.ipynb when viewing in ipython notebook. The experience is highly enhanced.