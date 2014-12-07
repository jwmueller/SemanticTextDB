## Functions for SemanticTextDB library which interact with Postgres server:

import psycopg2
import re
import StoredProcedures

class SemanticTextDB:
	
	"""
	SemanticTextDB offers a Python interface with a Postgres database ("the underlying DB")
	which relies on commands in the psycopg2 Python package to modify/query tables within 
	the database.  SemanticTextDB methods are solely designed for
	storage and analysis of text documents, so other types of data the user wishes
	to store in the same underlying DB (i.e. non-document-related schemas) 
	should be created and interacted-with in the usual psycopg fashion,
	via the cursor class.  See http://initd.org/psycopg/docs/ for more information.
	
	SemanticTextDB allows the user to create a special Postgres table to store 
	documents and relevant meta-data, the DocumentTable.  As it is simply a Postgres
	table, a DocumentTable may be queried in the usual pyscopg fashion, 
	but both insertion/deletion of documents + meta-data and creation/removal of a
	DocumentTable should ONLY be done via SemanticTextDB functions rather than manually.
	
	When a DocumentTable called "X" is created, a Postgres table of the same name is
	initialized in the underlying DB.  The tuples in this table contain a unique ID 
	assigned to each document as well as other user-specified meta-data columns such as
	author, title, publication date, etc.  Depending on user-selected options, 
	SemanticTextDB is capable of automatically generating useful analytic representations 
	corresponding to each DocumentTable, which are stored as machine-generated fields in 
	tables or entirely new machine-generated Postgres schemas.  While these tables may be 
	queried and their values-changed using psycopg (to improve model-parameters),
	insertion/deletion from these machine-managed schemas is not allowed, and instead, 
	users should use the options provided in SemanticTextDB to turn on/off 
	different modeling capabilities.
	
	In the underlying DB, the names of the automatically machine-managed schemas 
	corresponding to DocumentTable X will be:
	
	X_text : A machine-generated Tuples in this table contain a foreign-key 
	document ID "id" pointing to table d 
	and a "content" field which contains the actual text of the document.  
	SemanticTextDB provides functions to nicely format this text for reading. 
	This table should NOT be modified, if document-modification is desired:
	first delete the document using the provided deleteDoc function,
	manually change the document's text, and then reinsert the document
	using the provided insertDoc function.
	TODO
	
	X_useropts : A machine-generated table with a single tuple listing the 
	various settings specified by the user regarding which models should be 
	trained, parameter-settings, etc.
	
	X_wordinfo : A machine-generated table containing counts of each word 
	over all documents in the DocumentTable and other word-specific 
	information such as a neural-network based vector-representation. TODO
	
	X_entities : A machine-generated table in which each tuple contains 
	fields: "entity_name" (transformed to adhere to a common spelling), 
	the ID of the document referencing, the number of 
	occurences in the document, the relative importance of the entity to 
	the document. Since the same entity may be mentioned in multiple 
	documents, multiple tuples may share the same entity_name. TODO
	
	X_topics_K : A machine-generated table containing K fields, corresponding 
	to the topics (distributions over words) of a fitted topic model with K 
	(an integer) topics.
	 
	X_doc_topics_K : A table in which each tuple contains the
	proportions of each of the K (an integer) topics in one of the documents
	corresponding to the fitted topic model with K topics.
	  
	The names of each of the above schemas may also be accessed as fields 
	of the DocumentTable python object.  All of the machine-generated-schema
	updates are wrapped in a transaction, so updates to all models either
	entirely succeed or fail if the DB server crashes. 
	"""
	
	# these modifiers appended to a given document table name produce
	# the names of all possible machine-generated schemas associated 
	# with the given document table:
	table_name_modifiers = ['_text', '_useropts', '_wordinfo', 
							'_entities', ''] # TODO add topic modeling tables.
	
	def __init__(self, conn, cursor):
		self.conn = conn
		self.cursor = cursor
		# find user-options tables in underyling DB:
		pattern = re.compile(r"^.*_useropts$")
		matches = [pattern.match(t) for t in self.allTables()]
		self.opt_tables = []
		for m in matches:
			if (m is not None):
				self.opt_tables.append(m.group())
		# find document tables in underyling DB:
		self.document_tables = {} # dict of names of document-tables in underlying DB
								  # mapping to DocumentTable objects
		for opt_table in self.opt_tables:
			doctable_name = opt_table[:-9]
			self.document_tables[doctable_name] = self.initializeDocumentTableInfo(doctable_name)
		self.cursor.execute("CREATE OR REPLACE LANGUAGE plpython3u;")
		self.createStoredProcedures()
		print "Document tables in this database: ",
		print self.document_tables.keys()
	
	def allTables(self):
		"""
		:returns: a list of all the tables in the Postgres DB specified by cursor
		"""
		self.cursor.execute("SELECT table_name from information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
		all_tables = self.cursor.fetchall()
		return [t[0] for t in all_tables]
	
	def allDocTables(self):
		"""
		:returns: a list of all the document tables in the underlying Postgres DB
		"""
		return self.document_tables.keys()
	
	def tableLength(self, name):
		"""
		:returns: the number of tuples in the given table or 
		None if table does not exist in underlying DB.
		"""
		self.cursor.execute("SELECT count(*) from " + name)
		results = self.cursor.fetchone()
		if len(results) > 0:
			return int(results[0])
		else:
			return None
	
	def displayText(self, name, id, options = None):
		"""
		Displays text in this document formatted for readability.
		:param name: the name of the document
		:param id: the id of the document in the document table.
		:param options: options regarding the output format.
		"""
		if name not in self.document_tables:
			raise LookupError(name + " is an unknown document table")
		self.cursor.execute("SELECT content from " + name + "_text where id = " +
							str(id) + ";")
		res = self.cursor.fetchall()
		if (len(res) == 1) and (len(res[0]) == 1):
			print res[0][0]  # TODO : implement pop=out window
		else:
			print "Text for id " + str(id) + " in document-table " + name + " not found."
	
	def createDocTable(self, name, user_columns = [], summary = 1,
	 				   topics = (5,10), entities = 1,
     				   sentiment = 1, count_words = True, length_count = True,
     				   vs_representations = 1, max_word_length = 200,
     				   update_increment = 1, new_transaction = True):
		""" Creates a new document table
		:param name: The string-name of the Postgres table housing documents in this table.
		If a table by this name already exists, throws error.
		:param user_columns: List of "colName colType" strings (in Postgres format) specifying additional
		user-defined columns that should exist in the document-table.
		None of the given colNames should be any of: 
		'auto_length_count', 'auto_sentiment', 'auto_summary', 'auto_insert_time'
		:param summary: Integer specifying a parameter-related to the summarization
		algorithm (TODO)
		:param topics: Tuple of two integers specifying minimum and maximum number 
		of topics to track (TODO)
		:param entities: Integer specifying a parameter-related to the entity resolution
		algorithm (TODO)
		:param length_count: Boolean specifying whether this table should automatically 
		count total number of words in documents.
		:param sentiment: Integer specifying a parameter-related to the sentiment analysis 
		algorithm (TODO)
		:param vs_representations: Integer specifying the dimension of the
		vectors which are used to represent each word in the neural-network word 
		embedding.
		:param max_word_length: Integer specifying the maximum word-length 
		(number of characters) to store in the *_wordinfo table.
		:param update_increment: Integer x which specifies that models are
		updated every X document-inserts.
		:param new_transaction: Boolean specifying whether a new transaction
		should be created for the operations in this function
		(set to True if this table-creation is not part of a larger Transaction).
		"""
		self.checkNameValidity(name)
		if new_transaction:
			self.cursor.execute("BEGIN;") # Start new transaction
		self._initializeDocTable(name, user_columns, summary,
						   sentiment, length_count)
		# create associated text table:
		self.cursor.execute("CREATE TABLE " + name + 
							"_text (id serial PRIMARY KEY, content text);")
		# , " +
		#					"foreign key (id) references " + 
		#					name + "(id) 
		# TODO : ^ is extra code to make id a foreign key, remove!
		opt_table_name = self._initializeOptionsTable(name, summary, topics,
							   entities, sentiment, count_words, length_count,
							   vs_representations, max_word_length,
							   update_increment)
		
		if topics != None:
			topic_model = 0 #TODO
		else:
			topic_model = None
			
		if entities != None:
			entity_model = 0 #TODO
		else:
			entity_model = None
		 
		if vs_representations != None:
			word_embedding_model = 0 #TODO
		else:
			word_embedding_model = None
		
		self._initializeWordinfoTable(name, count_words, vs_representations,
								max_word_length)
		if new_transaction:
			self.cursor.execute("COMMIT;") # End transaction
		self.opt_tables.append(opt_table_name)
		self.document_tables[name] = self.initializeDocumentTableInfo(name) # TODO replace with DocumentTable object
		print "created document table: " + name
	
	def dropDocTable(self, name, new_transaction = True):
		""" Removes a document table from underlying DB.
		:param name: Specifies the document table (must be a valid document table).
		:param new_transaction: Boolean specifying whether a new transaction
		should be created for the operations in this function
		(set to True if this table-deletion is not part of a larger Transaction).
		"""
		if name not in self.document_tables:
			raise LookupError(name + " is an unknown document table")
		else:
			if new_transaction:
				self.cursor.execute("BEGIN;") # Start new transaction
			for s in SemanticTextDB.table_name_modifiers:
				new_name = name + s
				self.cursor.execute("DROP TABLE IF EXISTS " + new_name + ";")
			if new_transaction:
				self.cursor.execute("COMMIT;") # End transaction
			del self.document_tables[name]
			self.opt_tables.remove(name+"_useropts")
			print "deleted document table: " + name
	
	def insertDoc(self, text, table_name, user_column_vals = [], id = None,
				  new_transaction = True):
		""" Inserts a document into the DB and automatically updates
		models if necessary.
		:param text: the text of the document (string)
		:param table_name: the name of the document-table in which this 
		document should be inserted.
		:param user_column_vals: A list whose elements are the values 
		corresponding to user-specified fields established during the 
		creation of the document-table.
		:param id: an integer which cannot be an existing document id in the DB.
		Generally id = None should be used to allow the document table to 
		self-manage the assigment of ids to documents.
		:param new_transaction: Boolean specifying whether a new transaction
		should be created for the operations in this function
		(set to True if this insert is not part of a larger Transaction).
		"""
		if table_name not in self.document_tables:
			raise LookupError(table_name + " is an unknown document table")
		if new_transaction:
			self.cursor.execute("BEGIN;")
		# Insert into document table
		command = "INSERT INTO " + table_name + " VALUES ("
		if id is None:
			command += "DEFAULT"
		else:
			command += str(id)
		for val in user_column_vals:
			if isinstance(val, basestring):
				# wrap in quotes for Postgres string and format safely
				# Note this is NOT safe against SQL injection,
				# so untrusted user-strings should always be sanitized before calling 
				# insertDoc. Here $zxqy9$ is a (highly-unlikely-naturally-occuring)
				# quote-delimiter which is not allowed to appear in the contents of the text.  
				command = command + ", " + "$zxqy9$" + val + "$zxqy9$"
			else:
				command = command + ", " + str(val)
		command += ", clock_timestamp()"
		if self.document_tables[table_name].length_count_option:
			command += ", NULL" # TODO: Implement length-counting
		if self.document_tables[table_name].sentiment_option is not None:
			command += ", NULL" # TODO: Implement sentiment-analysis
		if self.document_tables[table_name].summary_option is not None:
			command += ", NULL" # TODO: Implement summarization
		command += ");"
		self.cursor.execute(command)
		# Insert text:
		command = "INSERT INTO " + table_name + "_text VALUES ("
		if id is None:
			command += "DEFAULT"
		else:
			command += str(id)
		# wrap in quotes for Postgres string and format safely
		# Note this is NOT safe against SQL injection,
		# so untrusted texts should always be sanitized before calling 
		# insertDoc. Here $zxqy9$ is a (highly-unlikely-naturally-occuring)
		# quote-delimiter which is not allowed to appear in the contents of the text.  
		command = command + ", " + "$zxqy9$" + text + "$zxqy9$);"
		self.cursor.execute(command)
		# TODO check whether models should be updated and if so, do it.
		if new_transaction:
			self.cursor.execute("COMMIT;")


	def deleteDoc(self, id, table_name):
		if table_name not in self.document_tables:
			raise LookupError(table_name + " is an unknown table")
		"""
		Delects a document from the DB and upates models
		:param id: An integer which cannot be an existing document id in the DB
		"""
		return 0 # TODO
	
	def insertDocuments(self, texts, table_name, user_column_vals_list,
	 					ids = None, new_transaction = True):
		""" Inserts an entire set of documents into the DB in a single 
		Transaction and automatically updates models if necessary.
		:param texts: A list of strings, each the text of one document
		:param table_name: The name of the document-table into which these 
		documents should be inserted.
		:param user_column_vals: A list whose elements are the values 
		corresponding to user-specified fields established during the 
		creation of the document-table. Non-Pythonic SQL-specific
		data-types may be inserted by simply passing a string with the 
		SQL-formatted value.  For example a timestamp may be inserted using the
		string: "TIMESTAMP '2001-01-12 15:24:39'"
		:param id: An integer which cannot be an existing document id in the DB.
		Generally id = None should be used to allow the document table to 
		self-manage the assigment of ids to documents.
		:param new_transaction: Boolean specifying whether a new transaction
		should be created for the operations in this function
		(set to True if these inserts are not part of a larger Transaction).
		"""
		message = "inserting documents into " + table_name + "..."
		print message,
		if new_transaction:
			self.cursor.execute("BEGIN;")
		if ids is None:
			ids = [None] * len(texts)
		if not (len(texts) == len(user_column_vals_list) == len(ids)):
			raise ValueError("texts, user_column_vals_list, and ids must be of same length")
		for i in range(len(texts)):
			insertDoc(texts[i], table_name, user_column_vals_list[i],
					  ids[i], new_transaction = False)
		if new_transaction:
			self.cursor.execute("COMMIT;")
		print "done"
	
	def _initializeDocTable(self, name, user_columns, summary, 
							sentiment, length_count):
		"""
		Actually creates documents table in underlying Postgres DB.
		Helper function for createDocTable(), not meant to be used on its own.
		"""
		command = "CREATE TABLE " + name + " (id serial PRIMARY KEY, "
		for col in user_columns:
			colName = col.split()
			if ((len(colName) != 2) or 
			   (colName in ['auto_length_count','auto_sentiment','auto_summary','insert_time'])):
				raise ValueError("column specification: " + col + 
							     " is incorrectly formatted/named")
			command = command + col + ", "
		command = command + "auto_insert_time timestamp"
		if length_count:
			command = command + ", auto_length_count integer"
		
		if sentiment is not None:
				command = command + ", auto_sentiment numeric"
		if summary is not None:
			command = command + ", auto_summary text"
		command = command + ");"
		self.cursor.execute(command)
		# TODO: insert-time tracking needs to be maintained.
	
	def _initializeOptionsTable(self, name, summary, topics, entities, sentiment,
							   count_words, length_count, vs_representations,
							   max_word_length, update_increment):
		"""
		Creates *_useropts table in underlying DB associated
		with a Document Table, which contains a single row, in which
		the fields are the user-specified settings.
		Helper function for createDocTable(), not meant to be used on its own.
		"""
		opt_table_name = name + "_useropts"
		buildcommand = "CREATE TABLE " + opt_table_name + " (" + \
				  "summary integer, topics integer[2], entities integer, sentiment integer, " + \
				  "count_words boolean, length_count boolean, vs_representations integer, " + \
				  "max_word_length integer, update_increment integer, " + \
				  "num_inserts_since_update integer);"
				  # TODO: fix the types of these columns once algorithms are implemented.
		self.cursor.execute(buildcommand)
		command = "INSERT INTO " + opt_table_name + " VALUES ("
		if isinstance(summary, int):
			command = command + str(summary) + ", "
		elif summary is None:
			command += "NULL, "
		else:
			raise ValueError("summary must be integer")
		if (topics is not None) and (len(topics) == 2) and \
		 isinstance(topics[0], int) and isinstance(topics[1], int):
			command = command + "'{" + str(topics[0]) + ", " + str(topics[1]) + "}', "
		elif topics is None:
			command += "NULL, "
		else:
			raise ValueError("topics must be tuple of two integers")
		if isinstance(entities, int):
			command = command + str(entities) + ", "
		elif entities is None:
			command += "NULL, "
		else:
			raise ValueError("entities must be integer")
		if isinstance(sentiment, int):
			command = command + str(sentiment) + ", "
		elif sentiment is None:
			command += "NULL, "
		else:
			raise ValueError("sentiment must be integer")
		if isinstance(count_words, bool):
			if count_words:
				command += "TRUE, "
			else:
				command += "FALSE, "
		elif count_words is None:
			command += "NULL, "
		else:
			raise ValueError("count_words must be boolean")
		if isinstance(length_count, bool):
			if length_count:
				command += "TRUE, "
			else:
				command += "FALSE, "
		elif length_count is None:
			command += "NULL, "
		else:
			raise ValueError("length_count must be boolean")
		if isinstance(vs_representations, int):
			command = command + str(vs_representations) + ", "
		elif vs_representations is None:
			command += "NULL, "
		else:
			raise ValueError("vs_representations must be integer")
		if isinstance(max_word_length, int):
			command = command + str(max_word_length) + ", "
		elif max_word_length is None:
			command += "NULL, "
		else:
			raise ValueError("max_word_length must be integer")
		if isinstance(update_increment, int):
			command = command + str(update_increment) + ", "
		elif update_increment is None:
			command += "NULL, "
		else:
			raise ValueError("update_increment must be integer")
		command = command + str(update_increment - 1) + ");" # for num_inserts_since_update
		self.cursor.execute(command)
		return opt_table_name
	
	def _initializeWordinfoTable(self, name, count_words, vs_representations,
								max_word_length):
		"""
		Actually creates *_wordinfo table in underlying DB associated
		with a Document Table, only if either the count_words or vs_representations
		options are specified.
		Helper function for createDocTable(), not meant to be used on its own.
		"""
		if count_words:
			if (vs_representations is not None):
				self.cursor.execute("CREATE TABLE " + name + \
							"_wordinfo (word varchar(" + str(max_word_length) + \
							"), count integer, vector numeric[" + \
							str(vs_representations) + "]);")
			else:
				self.cursor.execute("CREATE TABLE " + name + \
					"_wordinfo (word varchar(" + str(max_word_length) + \
									"), count integer);")
		elif vs_representations is not None:
			self.cursor.execute("CREATE TABLE " + name + \
							"_wordinfo (word varchar(" + str(max_word_length) + \
							"), vector numeric[" + str(vs_representations) + "]);")
			# TODO: Need to create index on wordinfo word column vs. without index
	
	def checkNameValidity(self, name):
		""" raises exception if the given name for a table or the 
		machine-generated names built upon this name 
		are already used in the PostgresDB
		"""
		for s in SemanticTextDB.table_name_modifiers:
			new_name = name + s
			if (new_name in self.document_tables) or (new_name in self.allTables()):
				raise ValueError(new_name + " is already a table")
		
		if (len(re.findall(".*_useropts$", name)) > 0):
			raise ValueError(name + " cannot end with '_useropts'")
	
	def initializeDocumentTableInfo(self, name):
		"""Creates a DocumentTableInfo object from the existing <name>_useropts table.
		:param name: The corresponding document table in underlying DB.
		:returns: A DocumentTableInfo object with the correct settings.
		"""
		if name + "_useropts" not in self.opt_tables:
			raise LookupError(name + " does not have a corresponding user-options table")
		self.cursor.execute("SELECT * FROM " + name + "_useropts" + ";")
		colnames = [desc[0] for desc in self.cursor.description]
		opts = self.cursor.fetchone()
		dt = DocumentTableInfo(name, 
				summary = opts[colnames.index("summary")],
				topics = opts[colnames.index("topics")],
				entities = opts[colnames.index("entities")],
				sentiment = opts[colnames.index("sentiment")],
				count_words = opts[colnames.index("count_words")],
				length_count = opts[colnames.index("length_count")],
				vs_representations = opts[colnames.index("vs_representations")],
				max_word_length = opts[colnames.index("max_word_length")],
				update_increment = opts[colnames.index("update_increment")]
			 )
		return dt
	
	def createStoredProcedures(self):
		""" 
		Creates a set of procedures which are stored in the DB
		"""
		[self.cursor.execute(func) for func in StoredProcedures.listStoredProcedures()]
	
	""" The functions below this point call stored procedures within the database
		and are not intended for most users. """
	
	def pymax(a, b):
		""" A procedure stored in the underlying DB.
		:returns: the maximum of A and B
		"""
		self.cursor.callproc("pymax", [a, b])
		result = cursor.fetchone()
		if result is None:
			return None
		else:
			return result[0]
