# Script runs Online LDA on a list of documents:
import onlineLDA
import numpy

def runLDA(docs, batchsize = 100, D = 1e6, Ks = range(10, 20 + 1), W = 1e5):

	# Run Topic modeling:
	oldas = [] # list of topic models
	bounds = [0] * len(Ks) # variational estimates of held-out perplexity for each model.
	for j in range(len(Ks)):
		K = Ks[j]
		oldas.append(onlineLDA.OnlineLDA(K, D, 1./K, 1./K, 1024., 0.7, W))

	docset = []
	for i in range(len(docs)):
		docset.append(docs[i])
		if (len(docset) ==  batchsize): # update topic models:
			for j in range(len(Ks)):
				olda = oldas[j]
				(gamma, bound) = olda.update_lambda(docset)
				(wordids, wordcts) = olda.parse_doc_list(docset)
	        	bounds[j] = bound * len(docset) / (D * sum(map(sum, wordcts)))
			docset = [] # reset docset.
			#print '%d:  rho_t of first model = %f,  held-out perplexity of first model = %f' % \
	            #(i, oldas[0]._rhot, numpy.exp(-bounds[0]))

	j = 2
	oldas[j].printTopics(N = 10)


