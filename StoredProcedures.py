"""
This file contains functions which are NOT meant for use in Python but
are stored in the database and may be accessed via the provided methods 
in the SemanticTextDB class
""" 

def listStoredProcedures():
	
	pymax_def = \
"""CREATE OR REPLACE FUNCTION pymax (a integer, b integer) RETURNS integer 
AS $$
  if a > b:
    return a
  return b
$$ LANGUAGE plpython3u;
"""
	
	stored_prodecures_list =[pymax_def] # TODO add rest of procedures
	return stored_prodecures_list
