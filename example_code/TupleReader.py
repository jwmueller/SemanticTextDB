# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import os
import re

# <codecell>

readPath = "C:/git/SemanticTextDB/example_code/txt/"
author = "United States Government"

# <codecell>

count = 0
for filename in os.listdir(readPath):
    count = count + 1
    f = open(readPath + filename,'r')
    fileAsString = f.read()
    result = (re.search(r'[0-9].*?\n', fileAsString)).group()
    result = re.sub(r'\xc2\xa7\xc2\xa7\xe2\x80\xaf', '', result)
    result = re.sub(r'\n', '', result)
    lawTitleNum = filename.split('_')[0]
    lawSectionNum = (re.search(r'.*?\.', result)).group()[:-1]
    lawName = (re.search(r'\..*', result)).group()[2:]
    print filename, lawTitleNum, lawSectionNum, lawName    
    

# <codecell>

lawNum

