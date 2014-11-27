# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import os
import psycopg2
conn = psycopg2.connect(database="semanticdb", user="jonasmueller", host="18.251.7.99", password="mantitties")
cur = conn.cursor()

# <codecell>


