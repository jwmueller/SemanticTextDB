class DocumentTableInfo:
	
	"""
	DocumentTableInfo is simply wrapper which keeps track of some statistics/settings
	about a document table.  This class has no access to the underlying DB.
	"""
	
	def __init__(self, name, summary, topics, entities, sentiment,
				 count_words, length_count, vs_representations,
				 max_word_length, update_increment):
		self.name = name # name of the documents-table associated with this object.
		self.topicmodel = None
		self.entitymodel = None
		self.wordembedding_neuralnet = None
		
		# options used in this Document Table:
		self.summary_option = summary
		self.topics_option = topics
		self.entities_option = entities
		self.sentiment_option = sentiment
		self.count_words_option = count_words
		self.length_count_option = length_count
		self.vs_representations = vs_representations
		self.max_word_length = max_word_length
		self.update_increment = update_increment
		self.num_inserts_since_update = update_increment - 1
	
	def findRelatedTables(self):
		""" 
		Returns a list of all Postgres table-names related to this DocumentTable
		"""
		table_list = [self.name, self.texts]
		if self.word_counts != None:
			table_list = table_list.append(self.word_counts)
		return table_list
	
	def displayText(self, id, options = None):
		"""
		Displays text in this document formatted for readability.
		:param id: the id of the document in the document table.
		:param options: options regarding the output format.
		"""
		print "Not implemented" # TODO
	
	def printOptions(self):
		""" Displays the options used to generate this DocumentTable """
		print "summaries:" + self.summaries_option
		print "topics:" + self.topics_option 
		print "entities:" + self.entities_option
		print "sentiment:" + self.sentiment_option
		print "count_words:" + self.count_words_option

	