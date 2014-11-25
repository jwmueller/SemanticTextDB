# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import re
import os

# <codecell>

readPath = "C:/git/SemanticTextDB/xml/"
writePath = "C:/git/SemanticTextDB/txt/"
if not os.path.exists(writePath):
    os.makedirs(writePath)

# <codecell>

for filename in os.listdir(path):
    f = open(readPath + filename,'r')
    fileAsString = f.read()
    sections = re.findall(r'<section.*?</section>', fileAsString, re.DOTALL)
    count = 0
    for section in sections:
        count = count + 1
        open(writePath + filename[:-4] + '_' + str(count) + '.txt', 'w').write(re.sub(r'<.*?>', '', section, flags = re.DOTALL))
        

