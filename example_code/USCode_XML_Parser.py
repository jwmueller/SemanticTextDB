# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import re
import os
import xml.etree.ElementTree as ET

# <codecell>

readPath = "C:/git/SemanticTextDB/example_code/xml/"
writePath = "C:/git/SemanticTextDB/example_code/all_US_Law_Codes/"
if not os.path.exists(writePath):
    os.makedirs(writePath)

# <codecell>

for filename in os.listdir(readPath):
    print 'Working on file:', filename
    #f = open(readPath + filename,'r')
    #fileAsString = f.read()
    #sections = re.findall(r'<section.*?id=.*?>.*?</section>', fileAsString, re.DOTALL)
    
    #Create ElementTree representing the XML structure of the laws
    tree = ET.parse(readPath + filename)
    root = tree.getroot()
    
    #Grab the sections containing the laws. The sections are on the fourth level of the xml
    #represented by */*/*/<sections here>. Only return sections ending in element tag section.
    #The list comprehension returns a string represnetion of the xml using ET.tostring  method.
    #sections = [ET.tostring(element, encoding='utf8', method='xml') for element in root.findall('*/*/*/') if element.tag[len(element.tag)-7:] == 'section']
    parent_map = {c:p for p in tree.iter() for c in p}
    sections = [ET.tostring(element, encoding='utf8', method='xml') for element in root.iter()
                if (element.tag[len(element.tag)-7:] == 'section') and
                (parent_map[element].tag[len(parent_map[element].tag)-7:] != 'section') and
                (element.tag[len(element.tag)-10:] != 'subsection')]
    count = 0
    for section in sections:
        count = count + 1        
        #result = re.sub(r'</heading><subsection',' \n', section)
        result = re.sub(r'</ns0:heading>','\n', section)        
        result = re.sub(r'<.*?>', '', result, flags = re.DOTALL)
        result = re.sub(r'\n{3,}', '\n\n', result, flags = re.DOTALL)
        fout = open(writePath + filename[3:-4] + '_' + str(count) + '.txt', 'w')
        fout.write(result)

