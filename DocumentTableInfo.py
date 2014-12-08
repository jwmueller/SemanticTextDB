class DocumentTableInfo:
	
	"""
	DocumentTableInfo is simply wrapper which keeps track of some statistics/settings
	about a document table.  This class has no access to the underlying DB.
	"""
	
	def __init__(self, name, summary, topics, entities, sentiment,
				 count_words, length_count, vs_representations,
				 max_word_length, update_increment, last_update_id):
		self.name = name # name of the documents-table associated with this object.
		self.topicmodel = None
		self.entitymodel = None
		
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
		self.last_update_id = last_update_id
	
	def findRelatedTables(self):
		""" 
		Returns a list of all Postgres table-names related to this DocumentTable
		"""
		table_list = [self.name, self.texts]
		if self.word_counts != None:
			table_list = table_list.append(self.word_counts)
		return table_list
	
	def printOptions(self):
		""" Displays the options used to generate this DocumentTable """
		print "summaries:" + self.summary_option
		print "topics:" + self.topics_option 
		print "entities:" + self.entities_option
		print "sentiment:" + self.sentiment_option
		print "count_words:" + self.count_words_option

	