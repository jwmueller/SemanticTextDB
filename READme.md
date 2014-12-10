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
Make sure you have Python_ 2.6+/3.2+ and `pip <https://crate.io/packages/pip/>`_
(`Windows <http://docs.python-guide.org/en/latest/starting/install/win/>`_,
`Linux <http://docs.python-guide.org/en/latest/starting/install/linux/>`_) installed.
Run simply (preferred way):

.. code-block:: bash

    $ [sudo] pip install sumy


Or for the fresh version:

.. code-block:: bash

    $ [sudo] pip install git+git://github.com/miso-belica/sumy.git


Or if you have to:

.. code-block:: bash

    $ wget https://github.com/miso-belica/sumy/archive/master.zip # download the sources
    $ unzip master.zip # extract the downloaded file
    $ cd sumy-master/
    $ [sudo] python setup.py install # install the package