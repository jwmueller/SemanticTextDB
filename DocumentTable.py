class DocumentTable:
	def __init__(self, name, text_table, user_columns, summaries,
	 		     topics, entities, sentiment, count_words, 
	 		     topicmodel, entitymodel):
		self.name = name # name of the documents-table associated with this object.
		self.texts = name + "_text" 
		self.topicmodel = topicmodel
		self.entitymodel = entitymodel
		if count_words:
			self.word_counts = name + "_word_counts"
		else:
			self.word_counts = None
		
		# options used in this Document Table:
		self.summaries_option = summaries
		self.topics_option = topics
		self.entities_option = entities
		self.sentiment_option = sentiment
		self.count_words_option = count_words
		
	
	
	def findRelatedTables(self):
		""" 
		Returns a list of all Postgres table-names related to this DocumentTable
		"""
		table_list = [self.name, self.texts]
		if self.word_counts != None:
			table_list = table_list.append(self.word_counts)
		return table_list
	
	def delete(self):
		"""
		deletes all tables associated with this DocumentTable
		"""
		table_list = self.findRelatedTables()
			# FIXME: delete table from database 
	
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

		
		
		