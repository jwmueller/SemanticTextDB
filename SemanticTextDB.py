## Functions for SemanticTextDB library which interact with Postgres server:

import psycopg2

class SemanticTextDB:
	
	def __init__(self, conn, cursor, name = None):
		self.conn = conn
		self.cursor = cursor
		self.name = name
		self.document_tables = {}; # dict of DocumentTable objects
	
	def setConn(self,conn):
		self.conn = conn
	
	def setCursor(self, cursor):
		self.cursor = cursor
	
	def allTables(self):
		"""
		:returns: a list of all the tables in the Postgres DB specified by cursor
		"""
		self.cursor.execute("select table_name from information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
		all_tables = self.cursor.fetchall()
		return [t[0] for t in all_tables]
	
	def createDocTable(self, name, user_columns = [], summary = None,
	 				   topics = None, entities = None,
     				   sentiment = None, word_count = True, length_count = True):
		"""
		Creates a new document table
		:param name: The string-name of the Postgres table housing documents in this table.
		If a table by this name already exists, throws error.
		:param user_columns: List of <colName colType> strings (in Postgres format) specifying additional
		user-defined columns that should exist in the document-table.
		None of the given colNames should be any of: 'auto_length_count', 'auto_sentiment', 'auto_summary', 'auto_insert_time'
		:param length_count: Boolean specifying whether this table should automatically 
		count total number of words in documents.
		:param sentiment: Integer specifying the number of significant digits to store
		regarding the sentiment value for each document, if None, then automatic
		sentiment analysis is NOT performed, if = 0, then no limit on precision is used (wastes space)
		
		"""
		checkNameValidity(name)
		initializeDocTable(self, name, user_columns, summary,
						   sentiment, length_count)
		# create associated text table:
		self.cursor.execute("CREATE TABLE " + name + 
							"_text (id serial PRIMARY KEY, content text, " +
							"foreign key (id) references " + 
							name + " (id) );" 
		
		if topics != None:
			topicmodel = 0 #TODO
		else:
			topicmodel = None
			
		if entities != None:
			entitymodel = 0 #TODO
		else entitymodel = None
		
		if count_words:
			# create associated Word-counts table:
			self.cursor.execute("CREATE TABLE " + name + 
							"_word_counts (word varchar(200), count integer);"
			# TODO: Need to create index on word_counts word column vs. without index
			
		dt = DocumentTable(name, user_columns,
						   summaries, topics, entities,
						   sentiment,count_words, topicmodel, 
						   entitymodel)
		self.document_tables['name'] = dt
		return dt


	def deleteDocTable(self, name):
		if table_name not in self.document_tables:
			raise LookupError(table_name + " is an unknown table")
		else:
			doctable = self.document_tables[name]
			doctable.delete()
			del self.document_tables[name]
			print "deleted document table: " + name
		

	def insertDoc(self, id, text, table_name):
		if table_name not in self.document_tables:
			raise LookupError(table_name + " is an unknown table")
		"""
		Inserts a document into the DB and upates models
		:param id: an integer which cannot be an existing document id in the DB
		:param text: the text of the document (string)
		"""
		return 0

	
	def initializeDocTable(self, name, user_columns, summary, sentiment, length_count):
		"""
		Actually creates documents table in Postgres DB.
		Helper function for createDocTable(), not meant to be used on its own.
		"""
		command = "CREATE TABLE " + name + " (id serial PRIMARY KEY, "
		for col in user_columns:
			colName = col.split()
			if (len(colName) != 2) or 
			   (colName in ['auto_length_count','auto_sentiment','auto_summary','insert_time']):
				raise ValueError("column specification: " + col + 
							     " is incorrectly formatted/named")
			command = command + col + ", "
		
		if count_length:
			command = command + "auto_length_count integer"
		
		if (sentiment != None):
			if sentiment == 0:
				command = command + ", auto_sentiment numeric"
			else if isinstance(sentiment, (int,long)):
				command = command + ", auto_sentiment numeric(" + sentiment + ")"
			else:
				raise ValueError("sentiment " + sentiment +" is invalid.  Must be None or integer")
		if 
		
		while (command[-1] in [' ', ',']):
			command = command[:-1]
		command = command + ");"
		self.cursor.execute(command)

		
	def checkNameValidity(self, name):
		""" raises exception if the given name for a table or the 
		machine-generated names built upon this name 
		are already used in the PostgresDB
		"""
		if (name in self.document_tables) or (name in self.allTables()):
			raise ValueError(name + " is already a table")
		new_name = name + "_text"
		if (new_name in self.document_tables) or (new_name in self.allTables()):
			raise ValueError(new_name + " is already a table")
		new_name = name + "_word_counts"
		if (new_name in self.document_tables) or (new_name in self.allTables()):
			raise ValueError(new_name + " is already a table")
		# TODO : still need to check names of topic model / entity resolution tables.
