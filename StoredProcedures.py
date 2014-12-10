"""
This file contains functions which are NOT meant for use in Python but
are stored in the database and may be accessed via the provided methods 
in the SemanticTextDB class
""" 

def listStoredProcedures(): # A wrapper function which lists all the stored procedures in this file.
	
	# Initializes topic matrix for k topics and w (maximum-possible) words:
	initializelambda_def = \
"""CREATE OR REPLACE FUNCTION initializelambda (doctable text, k integer, d bigint, w bigint, alpha numeric, eta numeric, tau0 numeric, kappa numeric) RETURNS void
AS $$
if 'numpy' in GD:
	numpy = GD['numpy']
else:
	import numpy
	GD['numpy'] = numpy
if 'gammaln' in GD:
	gammaln = GD['gammaln']
else:
	from scipy.special import gammaln
	GD['gammaln'] = gammaln
if 'psi' in GD:	
	psi = GD['psi']
else:
	from scipy.special import psi
	GD['psi'] = psi
lambda_K = 1*numpy.random.gamma(100., 1./100., (k, w))
GD[doctable + '_lambda_'+str(k)] = lambda_K
if (len(lambda_K.shape) == 1):
	Elogbeta_K = psi(lambda_K) - psi(numpy.sum(lambda_K))
else:
	Elogbeta_K = psi(lambda_K) - psi(numpy.sum(lambda_K, 1))[:, numpy.newaxis]
GD[doctable + '_Elogbeta_'+str(k)] = Elogbeta_K
GD[doctable + '_expElogbeta_'+str(k)] = numpy.exp(Elogbeta_K)
GD[doctable + '_updatect'] = 0
GD[doctable + '_vocab'] = dict()
table_name = doctable + "_topics_" + str(k)
command = "CREATE TABLE " + table_name + " (word_id SERIAL PRIMARY KEY, word text"
for j in range(1, k+1):
	command = command + ", topic_" + str(j) + " numeric"
command += ");"
plpy.execute(command)
for i in range(0, w):
	lambda_i = lambda_K[:,i]
	command = "INSERT INTO " + table_name + " VALUES (DEFAULT, $zxqy9$---$zxqy9$"
	for j in range(0, k):
		command = command + ', ' + str(lambda_i[j])
	command += ");"
	plpy.execute(command)
$$ LANGUAGE plpython3u;
"""
	
	# Determines which texts must be updated and puts them in a list in  
	# the Global Dictionary along with their corresponding document-ids.
	textstoupdate_def = \
""" CREATE OR REPLACE FUNCTION textstoupdate (doctable text, last_update_id bigint, update_increment integer) RETURNS bigint
AS $$
res = plpy.execute("SELECT last_value FROM " + doctable + "_id_seq;")
new_id = res[0]['last_value']
if (new_id - last_update_id) < update_increment:
	return last_update_id
command = 'SELECT id, content FROM ' + doctable + '_text WHERE id > ' + str(last_update_id) + ';'
res = plpy.execute(command)
if len(res) < update_increment:
	return last_update_id
update_texts = [row['content'] for row in res]
GD[doctable + 'update_texts'] = update_texts
update_ids = [row['id'] for row in res]
GD[doctable + 'update_ids'] = update_ids
return new_id
$$ LANGUAGE plpython3u;
"""
	
	# Counts the words in texts and puts the word-counts dicts in a list 
	# in the Global Dictionary.
	# Note: the textstoupdate method must be run prior to calling this function.
	countwords_def = \
""" CREATE OR REPLACE FUNCTION countwords (doctable text) RETURNS void
AS $$
if 're' in GD:
	re = GD['re']
else:
	import re
	GD['re'] = re
textlist_key = doctable + 'update_texts'
if textlist_key not in GD:
	plpy.error("Must first call textstoupdate() before parseupdatetexts()")
textlist = GD[textlist_key]
wordcounts = list()
for doc_text in textlist:
	doc_text = doc_text.lower()
	doc_text = re.sub(r'-', ' ', doc_text)
	doc_text = re.sub(r'[^a-z ]', '', doc_text)
	doc_text = re.sub(r' +', ' ', doc_text)
	words = doc_text.split()
	counts = dict()
	for word in words:
		if (word not in counts):
			counts[word] = 0
		counts[word] += 1
	wordcounts.append(counts)
GD[doctable + '_wordcounts'] = wordcounts
$$ LANGUAGE plpython3u;
"""
	
	# Updates all topic models using Online LDA (variational algorithm) 
	# and stores the updates in the GD object.
	# Note: the countwords method must be run prior to calling this function.
	traintopicmodels_def = \
"""CREATE OR REPLACE FUNCTION traintopicmodels (doctable text) RETURNS void
AS $$
if 'numpy' in GD:
	numpy = GD['numpy']
else:
	import numpy
	GD['numpy'] = numpy
if 'gammaln' in GD:
	gammaln = GD['gammaln']
else:
	from scipy.special import gammaln
	GD['gammaln'] = gammaln
if 'psi' in GD:	
	psi = GD['psi']
else:
	from scipy.special import psi
	GD['psi'] = psi
wordcounts_key = doctable + '_wordcounts'
if wordcounts_key not in GD:
	plpy.error("Must first call countwords() before traintopicmodels()")
wordcounts = GD[wordcounts_key]
if (doctable + 'update_ids') not in GD:
	plpy.error("Must first call textstoupdate() before traintopicmodels()")
update_ids = GD[doctable + 'update_ids']
reread_param = False
if (doctable + '_d') not in GD:
	reread_param = True
	params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodel_sharedparam;')
	GD[doctable + '_d'] = params[0]['d']
d = GD[doctable + '_d']
if (doctable + '_tau0') not in GD:
	if not reread_param:
		params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodel_sharedparam;')
		reread_param = True
	GD[doctable + '_tau0'] = params[0]['tau0']
tau0 = GD[doctable + '_tau0']
if (doctable + '_kappa') not in GD:
	if not reread_param:
		params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodel_sharedparam;')
		reread_param = True
	GD[doctable + '_kappa'] = params[0]['kappa']
kappa = GD[doctable + '_kappa']
if (doctable + '_w') not in GD:
	if not reread_param:
		params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodel_sharedparam;')
		reread_param = True
	GD[doctable + '_w'] = params[0]['w']
w = GD[doctable + '_w']
if (doctable + '_updatect') not in GD:
	if not reread_param:
		params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodel_sharedparam;')
		reread_param = True
	GD[doctable + '_updatect'] = params[0]['updatect']
updatect = GD[doctable + '_updatect']
if (doctable + '_stopwords') not in GD:
	if not reread_param:
		params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodel_sharedparam;')
		reread_param = True
	GD[doctable + '_stopwords'] = params[0]['stopwords']
stopwords = GD[doctable + '_stopwords']
if (doctable + '_topic_opts') not in GD:
	res = plpy.execute('SELECT topics FROM ' + doctable + '_useropts;')
	topic_opts = res[0]['topics']
	GD[doctable + '_topic_opts'] = topic_opts
else:
	topic_opts = GD[doctable + '_topic_opts']
min_K = topic_opts[0]
max_K = topic_opts[1]
rhot = pow(tau0 + updatect, - kappa)
meanchangethresh = 0.001
if (doctable + '_vocab') not in GD:
	vocab = dict()
	res = plpy.execute('SELECT * FROM ' + doctable + '_topics_' + str(min_K) + ' ORDER BY word_id ASC;')
	index = 0
	topic_word = res[index]['word']
	while ((index < len(res)) and (topic_word != '---')):
		vocab[topic_word] = index
		index += 1
		topic_word = res[index]['word']
else:
	vocab = GD[doctable + '_vocab']
word_indices = []
word_cts = []
num_words = 0
for c in range(len(wordcounts)):
	doc_wordcount = wordcounts[c]
	word_indices_doc = []
	word_cts_doc = []
	for word in doc_wordcount.keys():
		if word not in stopwords:
			if word in vocab:
				word_indices_doc.append(vocab[word])
				word_cts_doc.append(doc_wordcount[word])
			elif (len(vocab) < w):
				next_index = len(vocab)
				vocab[word] = next_index
				word_indices_doc.append(next_index)
				word_cts_doc.append(doc_wordcount[word])
			num_words += doc_wordcount[word]
	if len(word_indices_doc) > 0:
		word_indices.append(word_indices_doc)
		word_cts.append(word_cts_doc)
	#else:
	# update_ids.pop(c)
batchD = len(word_indices)
if batchD == 0:
	return
reread_param = False
scores = []
for k in range(min_K, max_K+1):
	if (doctable + '_alpha_'+str(k)) not in GD:
		if (not reread_param):
			params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodels;')
			reread_param = True
		GD[doctable + '_alpha_'+str(k)] = params[k-min_K]['alpha']
	alpha = GD[doctable + '_alpha_'+str(k)]
	if (doctable + '_eta_'+str(k)) not in GD:
		if (not reread_param):
			params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodels;')
			reread_param = True
		GD[doctable + '_eta_'+str(k)] = params[k-min_K]['eta']
	eta = GD[doctable + '_eta_'+str(k)]
	if (doctable + '_lambda_'+str(k)) not in GD:
		lambda_k = numpy.zeros((k, w))
		res = plpy.execute('SELECT * FROM ' + doctable + '_topics_' + str(k) + ' ORDER BY word_id ASC;')
		for i in range(0, w):
			res_i = res[i]
			for j in range(0, k):
				lambda_k[j,i] = res_i['topic_' + str(j+1)]
	else:
		lambda_k = GD[doctable + '_lambda_'+str(k)]
	if (doctable + '_Elogbeta_'+str(k)) not in GD:
		if (len(lambda_k.shape) == 1):
			Elogbeta = psi(lambda_k) - psi(numpy.sum(lambda_k))
		else:
			Elogbeta = psi(lambda_k) - psi(numpy.sum(lambda_k, 1))[:, numpy.newaxis]
	else:
		Elogbeta = GD[doctable + '_Elogbeta_'+str(k)]
	if (doctable + '_expElogbeta_'+str(k)) not in GD:
		expElogbeta = numpy.exp(Elogbeta)
	else:
		expElogbeta = GD[doctable + '_expElogbeta_'+str(k)]
	gamma = 1*numpy.random.gamma(100., 1./100., (batchD, k))
	if (len(gamma.shape) == 1):
		Elogtheta = psi(gamma) - psi(numpy.sum(gamma))
	else:
		Elogtheta = psi(gamma) - psi(numpy.sum(gamma, 1))[:, numpy.newaxis]
	expElogtheta = numpy.exp(Elogtheta)
	sstats = numpy.zeros(lambda_k.shape)
	for dd in range(0, batchD):
		ids = word_indices[dd]
		cts = word_cts[dd]
		gammad = gamma[dd, :]
		Elogthetad = Elogtheta[dd, :]
		expElogthetad = expElogtheta[dd, :]
		expElogbetad = expElogbeta[:, ids]
		phinorm = numpy.dot(expElogthetad, expElogbetad) + 1e-100
		for it in range(0, 100):
			lastgamma = gammad
			gammad = alpha + expElogthetad * numpy.dot(cts / phinorm, expElogbetad.T)
			if (len(gammad.shape) == 1):
				Elogthetad = psi(gammad) - psi(numpy.sum(gammad))
			else:
				Elogthetad = psi(gammad) - psi(numpy.sum(gammad, 1))[:, numpy.newaxis]
			expElogthetad = numpy.exp(Elogthetad)
			phinorm = numpy.dot(expElogthetad, expElogbetad) + 1e-100
			meanchange = numpy.mean(abs(gammad - lastgamma))
			if (meanchange < meanchangethresh):
				break
		gamma[dd, :] = gammad
		if (doctable + '_proportions_'+str(k)) not in GD:
			GD[doctable + '_proportions_'+str(k)] = dict()
		GD[doctable + '_proportions_'+str(k)][update_ids[dd]] = (gammad / float(sum(gammad)))
		sstats[:, ids] += numpy.outer(expElogthetad.T, cts/phinorm)
	sstats = sstats * expElogbeta
	score = 0
	if (len(gamma.shape) == 1):
		Elogtheta = psi(gamma) - psi(numpy.sum(gamma))
	else:
		Elogtheta = psi(gamma) - psi(numpy.sum(gamma, 1))[:, numpy.newaxis]
	expElogtheta = numpy.exp(Elogtheta)
	for dd in range(0, batchD):
		gammad = gamma[dd,:]
		ids = word_indices[dd]
		cts = numpy.array(word_cts[dd])
		phinorm = numpy.zeros(len(ids))
		for i in range(0, len(ids)):
			temp = Elogtheta[dd, :] + Elogbeta[:, ids[i]]
			tmax = max(temp)
			phinorm[i] = numpy.log(sum(numpy.exp(temp - tmax))) + tmax
		score += numpy.sum(cts * phinorm)
	score += numpy.sum((alpha - gamma)*Elogtheta)
	score += numpy.sum(gammaln(gamma) - gammaln(alpha))
	score += sum(gammaln(alpha*k) - gammaln(numpy.sum(gamma, 1)))
	score = score * d / float(batchD)
	score = score + numpy.sum((eta - lambda_k) * Elogbeta)
	score = score + numpy.sum(gammaln(lambda_k) - gammaln(eta))
	score = score + numpy.sum(gammaln(eta*w) - gammaln(numpy.sum(lambda_k, 1)))
	scores.append(score * batchD / float(d * num_words))
	new_lambda = lambda_k * (1-rhot) + rhot * (eta + d * sstats / batchD)
	if (len(new_lambda.shape) == 1):
		new_Elogbeta = psi(new_lambda) - psi(numpy.sum(new_lambda))
	else:
		new_Elogbeta = psi(new_lambda) - psi(numpy.sum(new_lambda, 1))[:, numpy.newaxis]
	new_expElogbeta = numpy.exp(new_Elogbeta)
	GD[doctable + '_lambda_'+str(k)] = new_lambda
	GD[doctable + '_Elogbeta_'+str(k)] = new_Elogbeta
	GD[doctable + '_expElogbeta_'+str(k)] = new_expElogbeta
GD[doctable + '_updatect'] = updatect + 1
GD[doctable + '_vocab'] = vocab
GD[doctable + '_scores'] = scores
$$ LANGUAGE plpython3u;
"""
	
	# Finds the variational perplexity lower-bounds for each LDA model:
	getbounds_def = \
"""CREATE OR REPLACE FUNCTION getbounds (doctable text) RETURNS numeric[]
AS $$
if 'numpy' in GD:
	numpy = GD['numpy']
else:
	import numpy
	GD['numpy'] = numpy
if (doctable + '_scores') in GD:
	return GD[doctable + '_scores']
else:
	res = plpy.execute('SELECT score FROM ' + doctable + '_topicmodels ORDER BY num_topics ASC;')
	scores = []
	for i in range(len(res)):
		scores.append(res[i]['score'])
	GD[doctable + '_scores'] = scores
	return scores
$$ LANGUAGE plpython3u;
"""
	
	# Nicely formats the top words in each topic for display:
	gettopics_def = \
"""CREATE OR REPLACE FUNCTION gettopics (doctable text, k integer, top_words integer) RETURNS SETOF topic_info 
AS $$
if 'numpy' in GD:
	numpy = GD['numpy']
else:
	import numpy
	GD['numpy'] = numpy
if (doctable + '_vocab') not in GD:
	vocab = dict()
	res = plpy.execute('SELECT * FROM ' + doctable + '_topics_' + str(k) + ' ORDER BY word_id ASC;')
	index = 0
	topic_word = res[index]['word']
	while ((index < len(res)) and (topic_word != '---')):
		vocab[topic_word] = index
		index += 1
		topic_word = res[index]['word']
	GD[doctable + '_vocab'] = vocab
else:
	vocab = GD[doctable + '_vocab']
if (doctable + '_lambda_'+str(k)) not in GD:
	if (doctable + '_w') not in GD:
		params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodel_sharedparam;')
		GD[doctable + '_w'] = params[0]['w']
	w = GD[doctable + '_w']
	lambda_k = numpy.zeros((k, w))
	res = plpy.execute('SELECT * FROM ' + doctable + '_topics_' + str(k) + ' ORDER BY word_id ASC;')
	for i in range(0, w):
		res_i = res[i]
		for j in range(0, k):
			lambda_k[j,i] = res_i['topic_' + str(j+1)]
	GD[doctable + '_lambda_'+str(k)] = lambda_k
else:
	lambda_k = GD[doctable + '_lambda_'+str(k)]
topic_infos = []
for i in range(0, k):
	top_indices = numpy.argsort(lambda_k[i,:])[::-1][:top_words]
	topic_i_words = [''] * top_words
	for word in vocab:
		if vocab[word] in top_indices:
			index = int(numpy.where(top_indices == vocab[word])[0])
			topic_i_words[index] = word
	i_word_prominence = lambda_k[i,top_indices]
	topic_infos.append([i+1, topic_i_words, i_word_prominence])
return tuple(topic_infos)
$$ LANGUAGE plpython3u;
"""
	
	# Pushes the dynamic parameters of topic-models associated with the given doctable to disk:
	persisttm_def = \
"""CREATE OR REPLACE FUNCTION persisttm (doctable text, last_update_id bigint) RETURNS void
AS $$
if 'numpy' in GD:
	numpy = GD['numpy']
else:
	import numpy
	GD['numpy'] = numpy
if (doctable + '_topic_opts') not in GD:
	res = plpy.execute('SELECT topics FROM ' + doctable + '_useropts;')
	topic_opts = res[0]['topics']
	GD[doctable + '_topic_opts'] = topic_opts
else:
	topic_opts = GD[doctable + '_topic_opts']
min_K = topic_opts[0]
max_K = topic_opts[1]
if (doctable + '_updatect') in GD:
	updatect = GD[doctable + '_updatect']
	params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodel_sharedparam;')
	if params[0]['updatect'] == updatect:
		return
	command = 'UPDATE ' +  doctable + '_topicmodel_sharedparam SET updatect = ' + str(updatect) + ';'
	plpy.execute(command)
if (doctable + '_scores') in GD:
	scores = GD[doctable + '_scores']
for k in range(min_K, max_K+1):
	if (doctable + '_scores') in GD:
		command = 'UPDATE ' + doctable + '_topicmodels SET score = ' + str(scores[k - min_K]) + ' WHERE num_topics = ' + str(k) + ';' 
		plpy.execute(command)
if (doctable + '_lambda_'+str(max_K)) in GD:
	(k, w) = GD[doctable + '_lambda_'+str(max_K)].shape
else:
	params = plpy.execute('SELECT * FROM ' + doctable + '_topicmodel_sharedparam;')
	w = params[0]['w']
if (doctable + '_vocab') in GD:
	vocab = GD[doctable + '_vocab']
else:
	vocab = dict()
	res = plpy.execute('SELECT * FROM ' + doctable + '_topics_' + str(k) + ' ORDER BY word_id ASC;')
	index = 0
	topic_word = res[index]['word']
	while ((index < len(res)) and (topic_word != '---')):
		vocab[topic_word] = index
		index += 1
		topic_word = res[index]['word']
words = ['---'] * w
for word in vocab:
	if word is not '---':
		words[vocab[word]] = word
for k in range(min_K, max_K+1):
	if (doctable + '_lambda_'+str(k)) in GD:
		lambda_k = GD[doctable + '_lambda_'+str(k)]
		command = 'TRUNCATE TABLE ' +  doctable + '_topics_' + str(k) + ';'
		plpy.execute(command)
		for i in range(0, w):
			lambda_k_i = lambda_k[:,i]
			command = "INSERT INTO " + doctable + '_topics_' + str(k) + ' VALUES ('+ str(i+1) + ', ' + '$zxqy9$' + words[i] + '$zxqy9$'
			for j in range(0,k):
				command = command + ', ' + str(lambda_k_i[j])
			command += ');'
			plpy.execute(command)
if (doctable + 'update_ids') in GD:
	update_ids = GD[doctable + 'update_ids']
else:
	res = plpy.execute('SELECT id FROM ' + doctable + ' WHERE id > ' + str(last_update_id) + ' ORDER BY id ASC;')
	update_ids = [r['id'] for r in res]
for k in range(min_K, max_K+1):
	if (doctable + '_proportions_'+str(k)) in GD:
		proportions_k_dict = GD[doctable + '_proportions_'+str(k)]
		update_ids = sorted(proportions_k_dict.keys())
		for i in range(0, len(update_ids)):
			proportions_k_i = proportions_k_dict[update_ids[i]]
			command = 'INSERT INTO ' + doctable + '_topicprops_' + str(k) + ' VALUES (' + str(update_ids[i])
			for j in range(0, k):
				command = command + ', ' + str(proportions_k_i[j])
			command += ');'
			plpy.execute(command)
			GD[doctable + '_proportions_'+str(k)] = dict()
			GD[doctable + 'update_ids'] = []
			GD[doctable + 'update_texts'] = []
$$ LANGUAGE plpython3u;
"""
	
	stored_prodecures_list =[initializelambda_def, textstoupdate_def, countwords_def, traintopicmodels_def, getbounds_def, gettopics_def, persisttm_def] # TODO add rest of procedures
	return stored_prodecures_list
