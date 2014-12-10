## Functions for SemanticTextDB library which interact with Postgres server:

import psycopg2
import re
import StoredProcedures
import DocumentTableInfo as dti
import nltk
import summarizer
import NLPfunctions as nlpf

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
	(an integer) topics. An placehold index in this table for a future word which has not yet
	appeared is temporarily assigned the placeholder word "---"
	
	X_topicprops_K : A table in which each tuple contains the
	proportions of each of the K (an integer) topics in one of the documents
	corresponding to the fitted topic model with K topics.
	
	X_topicmodels : A machine-generated table containing the parameters used for each of the
	topic models trained over the documents in DocumentTable X. Each row in this
	table contains the specific parameters for a single LDA model.
	
	X_topicmodel_sharedparam : A machine-generated table containing the parameters
	which are common to all the LDA models for DocTable X.
	  
	The names of each of the above schemas may also be accessed as fields 
	of the DocumentTable python object.  All of the machine-generated-schema
	updates are wrapped in a transaction, so updates to all models either
	entirely succeed or fail if the DB server crashes.  If the user wishes to switch 
	modeling options after the creation of the DocumentTable, they may modify 
	the appropriate tables at anytime. However, changing the properties of Python objects 
	will not persist such a change on disk, so option-table-modification is required.
	A set of accessor functions to facilitate permanent post-creation DocumentTable
	options-setting is still in the works. 
	"""
	
	# these modifiers appended to a given document table name produce
	# the names of all possible machine-generated schemas associated 
	# with the given document table:
	table_name_modifiers = ['_text', '_useropts', '_wordinfo', '_topicprops_', '_topics_'
							'_topicmodels', '_topicmodel_sharedparam', '_entities', ''] # TODO add necessary table-name-modifiers.
	
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
		# self.cursor.execute("CREATE OR REPLACE LANGUAGE plpython3u;")
		self.createStoredProcedures()
		print "Document tables in this database: ",
		print self.document_tables.keys()
	
	
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
	
	
	def displayTopics(self, doctable, K = None, top_words = 10):
		""" Displays the top words under the topics of the LDA model with K topics.
		By default, K is set to None, which means the topics of the model with the best
		variational bound are returned. This function searches the Global Dictionary object
		so it is capable of reflecting model updates that have not yet been persisted
		to disk.
		"""
		if K is None:
			K = self.findBestK(doctable)
		self.cursor.callproc("gettopics", [doctable, K, top_words])
		for i in range(K):
			res = self.cursor.fetchone()
			words = res[1]
			prominences = res[2]
			print "    Topic " + str(res[0]) + ":"
			for j in range(top_words):
				if words[j] == "":
					print ('<placeholder>  -----  ' + str(float(prominences[j])))
				else:
					print (words[j] + "  -----  " + str(float(prominences[j])))
			print ""
			print ""
	
	
	def queryByTopic(self, doctable, K = None):
		return None
	
	
	def createDocTable(self, name, user_columns = [], summary = 1,
	 				   topics = (10,20), entities = None,
     				   sentiment = True, count_words = False, length_count = False,
     				   vs_representations = None, max_word_length = 200,
     				   update_increment = 1, new_transaction = True):
		""" Creates a new document table
		:param name: The string-name of the Postgres table housing documents in this table.
		If a table by this name already exists, throws error.
		:param user_columns: List of "colName colType" strings (in Postgres format) specifying additional
		user-defined columns that should exist in the document-table.
		None of the given colNames should be any of: 
		'auto_length_count', 'auto_sentiment', 'auto_summary', 'auto_insert_time'
		:param summary: Integer specifying the length (in number of sentences) of the summaries produced 
		:param topics: Tuple of two integers specifying minimum and maximum number 
		of topics to track (minimum must be >1). Set to 'None' to turn off automatic summarization.
		If not None, then the DocumentTable will contain a column named 'auto_summary' containing
		summaries of each document. 
		:param entities: Integer specifying a parameter-related to the entity resolution
		algorithm (TODO: not implemented yet)
		:param length_count: Boolean specifying whether this table should automatically 
		count total number of words in documents (TODO: not implemented yet).
		:param sentiment: Boolean specifying whether or not to automatically compute  
		and store sentiment-values for each document. If = True, then the DocumentTable
		will contain a column named 'auto_sentiment' containing the sentiment of each document. 
		:param vs_representations: Integer specifying the dimension of the
		vectors which are used to represent each word in the neural-network word 
		representation (TODO: not implemented yet).
		:param max_word_length: Integer specifying the maximum word-length 
		(number of characters) to store in the *_wordinfo table.
		:param update_increment: Integer x which specifies that models are
		updated every X document-inserts.
		:param new_transaction: Boolean specifying whether a new transaction
		should be created for the operations in this function
		(set to True if this table-creation is not part of a larger Transaction).
		"""
		self.checkNameValidity(name)
		# check input:
		if  (topics is not None) and ((len(topics) < 1) or (len(topics) > 2)):
			raise ValueError("topics parameter must be of form: (min_K, max_K) or simply K")
		elif (topics is not None) and (len(topics) == 1):
			topics = (topics, topics)
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
		if topics is not None:
			self.initializeTopicModel(name, min_K = topics[0], max_K = topics[1], new_transaction = False)
		
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
				  new_transaction = True, persist_updates = False):
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
		:param persist_updates: Should the online parameter updates to the large models
		be persisted to disk? For faster insertion , only persist to disk at the end of 	
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
		if self.isOptionOn('sentiment', table_name):
			command += ", " + str(nlpf.sentimentAnalysis(text))
		if self.isOptionOn('summary', table_name):
			command += ", " + "$zxqy9$" + summarizer.summarize(text, 
						summary_length = self.getOption('summary', table_name)) + "$zxqy9$" 
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
		
		# Check whether models should be updated and if so, perform the necessary updates:
		self.updateModels(table_name, text)
		if new_transaction:
			self.cursor.execute("COMMIT;")
		
	
	def insertDocuments(self, texts, table_name, user_column_vals_list,
	 					ids = None, new_transaction = True, progress_update = 100):
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
		:param progress_update: if None nothing is printed, otherwise a progress update
		is printed every (progress_update)-th insert of a document. 
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
			if (progress_update is not None) and (i % progress_update == 0):
				print("inserted document #" + str(i))
		if new_transaction: # persist model updates into table form and commit transaction:
			
			self.cursor.execute("COMMIT;")
			
		print "done"
	
	
	def updateModels(self, doctable, last_text):
		"""
		Checks which models (if any) need to be updated after a document-insert 
		and runs the necessary algorithms.
		:param last_text: The text of the just-inserted document
		:param doctable: The name of the document table it was inserted into.
		"""
		last_update_id = self.document_tables[doctable].last_update_id
		update_increment = self.getOption("update_increment", doctable)
			# Note: the above last-update value is not on disk so in the event of transaction 
			# being aborted before the model-updates are persisted, we revert to the value 
			# stored in the Options table.
		topics_on = self.isOptionOn("topics", doctable)
		if topics_on: # Find the texts we need to include in update and put them in Global Dictionary:
			# TODO also consider additional models.
			self.cursor.callproc("textstoupdate", [doctable, last_update_id, update_increment])
			new_update_id = self.cursor.fetchone()[0]
			update_required = new_update_id > last_update_id
		else:
			update_required = False	
		if topics_on and update_required:
			self.cursor.callproc("countwords", [doctable])
			self.cursor.callproc("traintopicmodels", [doctable])
		# TODO: need methods for updating additional models.
		if update_required:
			self.document_tables[doctable].last_update_id = new_update_id
	
	
	def persistModels(self, doctable, new_transaction = True):
		"""
		Pushes the updates to all models associated with the given DocumentTable
		from RAM to Disk, so that they will persist in the underlying Database.
		This method should generally be called at the end of a bulk insert of documents,
		just before the Transaction commits.
		:param new_transaction: should this persist operation be wrapped in a new Transaction?
		"""
		if new_transaction:
			self.cursor.execute("BEGIN;")
		last_update_id = self.getLastUpdateID(doctable)
		if self.isOptionOn('topics', doctable): # Persist Topic Model:
			self.cursor.callproc("persisttm", [doctable, last_update_id])
		# TODO: persist changes to other models!
		# Change the _useropts Table's last_update_id value if necessary:
		if last_update_id > self.getOption("last_update_id", doctable):
			command = "UPDATE " + doctable + "_useropts SET last_update_id = " + str(last_update_id)
			self.cursor.execute(command)
		if new_transaction:
			self.cursor.execute("COMMIT;")
	
	
	def findBestK(self, doctable):
		""" Returns the best number of topics K according to the variational bound
		which approximates held-out perplexity
		"""
		bounds = self.getVariationalBounds(doctable)
		index = bounds.index(min(bounds))
		return (index + self.getOption('topics', doctable)[0])
	
	
	def getVariationalBounds(self, doctable):
		""" Returns the the variational bounds for each of the different LDA models fit
		to the documents stored in the specified DocumentTable
		(ordered by increasing number of topics in the models).
		"""
		self.cursor.callproc("getbounds",[doctable])
		res = self.cursor.fetchone()[0]
		#[float(b) for b in res]
		return res
	
	
	def deleteDoc(self, id, table_name):
		if table_name not in self.document_tables:
			raise LookupError(table_name + " is an unknown table")
		"""
		Deletes a document from the DB
		:param id: An integer which must be an existing document id in the DB
		"""
		return 0 # TODO
	
	
	def initializeTopicModel(self, doctable, min_K = 5, max_K = 50, D = 1e6, W = 1e4, alphas = None,
							 etas = None, tau0 = 1024., kappa = 0.7,
							 additional_stopwords = [], new_transaction = True):
		""" Creates a set of (max_K - min_K + 1) topic models with topic-numbers ranging from min_K to max_K (inclusive).
		Most parameter names/descriptions taken from Hoffman et al.
		Please change the above defaults as desired.
		:param doctable: The document table storing the corpus for these topic models.
		:param min_K: Number of topics in smallest topic model.
		:param max_K: Number of topics in largest topic model.
        
        :param D: Total number of documents in the population. For a fixed corpus,
           this is the size of the corpus. In the truly online setting, this
           can be an estimate of the maximum number of documents that
           could ever be seen. D can be extremely large and set independently of memory restrictions,
           as D goes to infinity this algorithm converges to the empirical Bayes estimates.
        :param W: Total number of words in the population.  Once the online algorithm has seen
        W distinct words in the corpus, it will treat any new words as stopwords, so W should be 
        set conservatively large (although larger W will decrease the efficiency of the algorithm
        and W must be small enough that W x K numpy matrix fits into RAM of database server).
        :param alphas: List of hyperparameters, the kth specifying a prior on weight vectors theta
        for the kth topic model.
        :param etas: List of hyperparameter, the kth specifying a prior on weight vectors beta
        for the kth topic model.
        :param tau0: A (positive) learning parameter that downweights early iterations
        :param kappa: Learning rate: exponential decay rate---should be between
             (0.5, 1.0] to guarantee asymptotic convergence.
		"""
		if alphas is None: # set default alpha values:
			alphas = [1./k for k in range(min_K, max_K+1)]
		if etas is None: # set default eta values:
			etas = [1./k for k in range(min_K, max_K+1)]
		Ks = range(min_K, max_K+1)
		if ((max_K < min_K) or (len(alphas) != len(range(min_K, max_K+1))) or (len(etas) != len(alphas)) or
			(kappa > 1) or (kappa < 0) or (tau0 <= 0) or (min_K < 2)):
			raise ValueError("invalid topic modeling parameters")
		if new_transaction:
			self.cursor.execute("BEGIN;") # Start new transaction
		stopwords = nltk.corpus.stopwords.words('english') + additional_stopwords
		stopword_string = ' '.join(stopwords)
		# Create table to store parameters for each topic model:
		self.cursor.execute("CREATE TABLE " + doctable + "_topicmodels (num_topics integer PRIMARY KEY, " + \
						"alpha numeric, eta numeric, score numeric);")
		for i in range(0,len(Ks)):
			self.cursor.execute("INSERT INTO " + doctable + "_topicmodels VALUES (" + \
				str(Ks[i]) + ", " + str(alphas[i]) + ", " + str(etas[i]) + ", " + str(0) + ");")
		# Create table to store parameters shared between all topic models:
		self.cursor.execute("CREATE TABLE " + doctable + "_topicmodel_sharedparam (d bigint, " + \
			"tau0 numeric, kappa numeric, w bigint, updatect bigint, stopwords text);")
		# Note this is NOT safe against SQL injection,
		# so untrusted additional_stopwords input should always be sanitized before calling 
		# insertDoc. Here $zxqy9$ is a (highly-unlikely-naturally-occuring)
		# quote-delimiter which is not allowed to appear in the contents of the text. 
		self.cursor.execute("INSERT INTO " + doctable + "_topicmodel_sharedparam VALUES (" + \
		 str(D) + ", " + str(tau0) + ", " + str(kappa) + ", " + str(W) + ", " + str(0) + ", " + \
		 "$zxqy9$" + stopword_string + "$zxqy9$);")
		for i in range(0, len(Ks)): # initialize topic-matrices
			self.cursor.callproc('initializelambda', [doctable, int(Ks[i]), int(D), int(W), alphas[i], etas[i], tau0, kappa])
			# Initialize topic-proportions-in-documents table for the kth topic model:
			command = "CREATE TABLE " + doctable + "_topicprops_" + str(Ks[i]) + " (id integer"
			for j in range(1,Ks[i]+1):
				command = command + ", " + "topic_" + str(j) + " numeric"
			command += ");"
			self.cursor.execute(command)
		if new_transaction:
			self.cursor.execute("COMMIT;") # End transaction
	
	
	def trainTopicModel(doctable):
		""" Calls stored procdure to update the LDA topic-parameters stored in the DB
			:param texts: a list of strings containing the contents of each document
			:param doctable: the document-table associated with the topic model of interest
		"""
		last_update_id = self.document_tables[doctable].last_update_id
		self.cursor.callproc("trainTM", [doctable, last_update_id])
	
	
	def allTables(self):
		"""
		:returns: a list of all the tables in the Postgres DB specified by cursor
		"""
		self.cursor.execute("SELECT table_name from information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
		all_tables = self.cursor.fetchall()
		return [t[0] for t in all_tables]
	
	
	def colNames(self, table):
		"""
		:returns: a list of all the column names in the specified table.
		"""
		self.cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" + table +"';")
		res = self.cursor.fetchall()
		return [c[0] for c in res]
	
	
	def seeTable(self, name, N = 5):
		"""
		Returns at most N rows from table with the given name.
		"""
		self.cursor.execute("SELECT * FROM " + name + ";")
		x = cur.fetchall()
		if len(x) < 5:
			return x
		else:
			return x[:4]
	
	
	def getOption(self, option, doctable):
		""" 
		:returns: the setting of the specificed option for the given doctable
		Note: option must be a string specifying a column in the <doctable>_useropts table.
		"""
		self.cursor.execute("SELECT " + option + " FROM " + doctable + "_useropts;")
		res = self.cursor.fetchone()
		if len(res) == 1:
			return res[0]
		else:
			print("unknown option/table specified")
	
	
	def isOptionOn(self, option, doctable):
		""" 
		:returns: the true if the specificed option is set to 'on' for the given doctable
		Note: option must be a string specifying a column in the <doctable>_useropts table.
		'on' may mean different things for different options.
		""" 
		opt_setting = self.getOption(option, doctable)
		if (option in ['topics', 'summary']): 
			if (opt_setting is not None):
				return True
			else:
				return False
		elif (option in ['sentiment', 'count_words']):
			return opt_setting
		else:
			raise ValueError(option + " is an unknown option")
		# TODO: implement this for other options
	
	
	def setOption(self, option, doctable, new_value):
			""" Sets the specified option in the given doctable.
				:param new_value: the desired setting of the option, must be of the right type.
			"""
			if option == 'topics':
				new_value_string = "'{" + str(new_value[0]) + ", " + str(new_value[1]) + "}'"
			else:
				new_value_string = str(new_value)
			self.cursor.execute("UPDATE " + doctable + "_useropts SET " + option + " = " + new_value_string + ";")
	
	
	def getLastUpdateID(self, doctable):
		""" Returns the index of the last document included in the model update.
			Note: not all of these changes may yet be persisted to disk.
		"""
		return self.document_tables[doctable].last_update_id
	
	
	def startTransaction(self):
		"""Begins a new transaction"""
		self.cursor.execute("BEGIN;")
	
	
	def endTransaction(self, persist = False, doctable = None):
		"""Commits the currently running transaction
			:param persist: If set True, this method will persist models
			:param doctable: The DocumentTable associated with the models to be 
			persisted.
		"""
		if persist:
			self.persistModels(doctable)
		self.cursor.execute("COMMIT;")
	
	
	def checkNameValidity(self, name):
		""" Raises exception if the given name for a table or the 
		machine-generated names built upon this name 
		are already used in the PostgresDB
		"""
		for s in SemanticTextDB.table_name_modifiers:
			new_name = name + s
			if (new_name in self.document_tables) or (new_name in self.allTables()):
				raise ValueError(new_name + " is already a table")
		
		if (len(re.findall(".*_useropts$", name)) > 0):
			raise ValueError(name + " cannot end with '_useropts'")
	
	
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
		
		if sentiment:
				command = command + ", auto_sentiment numeric"
		if summary is not None:
			command = command + ", auto_summary text"
		command = command + ");"
		self.cursor.execute(command)
	
	
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
				  "summary integer, topics integer[2], entities integer, sentiment boolean, " + \
				  "count_words boolean, length_count boolean, vs_representations integer, " + \
				  "max_word_length integer, update_increment integer, " + \
				  "last_update_id integer);"
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
		if isinstance(sentiment, bool):
			if sentiment:
				command += "TRUE, "
			else:
				command += "FALSE, "
		else:
			raise ValueError("sentiment must be boolean")
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
		command = command + str(0) + ");" # for last_update_id.
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
			# TODO: create index on wordinfo word column vs. without index
	
	
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
		dt = dti.DocumentTableInfo(name, 
				summary = opts[colnames.index("summary")],
				topics = opts[colnames.index("topics")],
				entities = opts[colnames.index("entities")],
				sentiment = opts[colnames.index("sentiment")],
				count_words = opts[colnames.index("count_words")],
				length_count = opts[colnames.index("length_count")],
				vs_representations = opts[colnames.index("vs_representations")],
				max_word_length = opts[colnames.index("max_word_length")],
				update_increment = opts[colnames.index("update_increment")],
				last_update_id = opts[colnames.index("last_update_id")]
			 )
		return dt
	
	
	def createStoredProcedures(self):
		""" 
		Creates a set of procedures which are stored in the DB
		"""
		# Defining a Return type for the gettopics stored procedure:
		topic_info_def = \
""" CREATE TYPE topic_info AS (
		topic_num integer,
		topic_words text[],
		word_prominence numeric[]
	);
"""
		self.cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'topic_info')")
		if not self.cursor.fetchone()[0]:
			self.cursor.execute(topic_info_def)
		[self.cursor.execute(func) for func in StoredProcedures.listStoredProcedures()]

	def semanticSelect(self, table_name, statement, feature, feature_param = None):
		"""
		semanticSelect is a powerful function which augments the traditional SQL SELECT statment
		when users are running SELECT queries over DOCUMENT tables in SemanticTextDB.
		Users can pass in a traditional SQL SELECT statement as a string using the statement variable
		and pass in a string naming the feature they wish to augment SELECT with. 
		feature options available are:
		1. positive_only - returns tuples corresponding to happy (high sentiment analysis) documents
		2. negative_only - returns tuples corresponding to unhappy (low sentiment analysis) documents
		3. view_summaries - returns the summaries corresponding to the output of your select statement
		4. word_frequency - returns frequency of given word in each document resulting from your query
		5. correct_spelling - returns documents with correct spelling for your query.

		:param table_name: string declaring name of table the select statment is meant to operate on.
		:param statement: a postgreSQL statement as a string (e.g. "SELECT * FROM table_name;")
		:param feature: a string naming which feature you wish to augment the select statement with.
		:param feature_param: if feature takes in a parameter, pass it in here.
		"""
		if (table_name[-5:] == "_text" and table_name[:-5] not in self.allDocTables()) or \
		   (table_name[-5:] != "_text" and table_name not in self.allDocTables()):
			raise ValueError("table_name must be a documentTable or the table_text machine generated table.")
		if (feature == 'positive_only' or feature == 'negative_only') and feature_param == None:
			raise ValueError("features 'positive_only' and 'negative_only' require a feauture_param.")
		if (feature == 'view_summaries' or feature == 'word_frequency' or feature == 'correct_spelling') and statement[0:12].upper() == "SELECT COUNT":
			raise ValueError("features 'view_summaries' and 'word_frequency' and 'correct_spelling' do not support SELECT COUNT.")
		if type(statement) != str:
			raise ValueError("statement must be a string.")
		if len(statement) < 6 or statement[0:6].upper() != "SELECT":
			raise ValueError("statement must begin with 'SELECT'.")
		if statement[0:12].upper() == "SELECT COUNT" and statement[0:15].upper() != "SELECT COUNT(*)":
			raise ValueError("semanticSelect can only handle aggregates of the form count(*)")

		#flag to specify how results are returned
		aggregate = statement[0:12].upper() == "SELECT COUNT"		
		
		table_text = table_name if table_name[-5:] == "_text" else table_name + "_text"
		returnResult = []

		if feature == 'positive_only':			
			count = 0

			if aggregate:
				self.cursor.execute("SELECT *" + statement[15:]) #REPLACES COUNT(*) WITH *
			else:
				self.cursor.execute("SELECT id, " + statement[7:])

			result = self.cursor.fetchall()
			for item in result:
				self.cursor.execute("SELECT content FROM " + table_text + " WHERE id = " + str(item[0]))
				doc = self.cursor.fetchone()[0]
				if nlpf.sentimentAnalysis(doc) >= feature_param:
					if aggregate:
						count = count + 1
					else:
						returnResult.append(tuple(list(item)[1:]))

			return (count if aggregate else returnResult)

		elif feature == 'negative_only':
			count = 0

			if aggregate:
				self.cursor.execute("SELECT *" + statement[15:]) #REPLACES COUNT(*) WITH *
			else:
				self.cursor.execute("SELECT id, " + statement[7:])

			result = self.cursor.fetchall()
			for item in result:
				self.cursor.execute("SELECT content FROM " + table_text + " WHERE id = " + str(item[0]))
				doc = self.cursor.fetchone()[0]
				if nlpf.sentimentAnalysis(doc) <= feature_param:
					if aggregate:
						count = count + 1
					else:
						returnResult.append(tuple(list(item)[1:]))

			return (count if aggregate else returnResult)
			
		elif feature == 'view_summaries':
			if feature_param == None:
				feature_param = 1
			self.cursor.execute("SELECT id, " + statement[7:])
			result = self.cursor.fetchall()

			for item in result:
				self.cursor.execute("SELECT content FROM " + table_text + " WHERE id = " + str(item[0]))
				doc = self.cursor.fetchone()[0]
				returnResult.append(summarizer.summarize(doc, feature_param))

			return returnResult

		elif feature == 'word_frequency':
			self.cursor.execute("SELECT id, " + statement[7:])
			result = self.cursor.fetchall()

			for item in result:
				self.cursor.execute("SELECT content FROM " + table_text + " WHERE id = " + str(item[0]))
				doc = self.cursor.fetchone()[0]
				returnResult.append(word_counts(doc))

			return returnResult

		elif feature == 'correct_spelling':
			self.cursor.execute("SELECT id, " + statement[7:])
			result = self.cursor.fetchall()

			for item in result:
				self.cursor.execute("SELECT content FROM " + table_text + " WHERE id = " + str(item[0]))
				doc = self.cursor.fetchone()[0]
				returnResult.append(str(nlpf.correct_spelling(doc)))

			return returnResult	




		#example of finding ratio of positive to negative support of the royal wedding.
		#statement = ('SELECT COUNT(*) FROM twitter, twitter_text '
		#			  'WHERE twitter.id = twitter_text.id '
		#			  'AND "london" in twitter_text.text '
		#			  'AND "wedding" in twitter_text.text')
		#posCount = semanticSelect(statement, 'positive_only', 0.8)
		#negCount = semanticSelect(statement, 'negative_only', -0.8)
		#ratio = posCount / (1.0 * negCount)