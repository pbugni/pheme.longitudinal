#!/usr/bin/env python
"""We generate reports from the database, where the OBX-5
(observation_result) is stored as XML.

Feed this script a file, and it'll "strip" the XML out, using the
delimiter defined within - typically '|'.

"""
delimiter = '|'

import sys
from xml.etree import ElementTree
from StringIO import StringIO


def strip(s):
    """Strip the first level xml tags - return delimited text

    Designed to pull the contents from HL7_Obx.observation_request,
    which has the XML Mirth generates, a la:

      <OBX.5><OBX.5.1>29</OBX.5.1></OBX.5>

    Also decodes any encoded HTML (i.e. `&gt;` becomes `>`)

    NB - only the first level contents are returned.

    :param s: a string containing the xml doc or element to strip

    """
    if not s or len(s) < 1:
        return s
    sio = StringIO(s)
    root = ElementTree.parse(sio).getroot()
    result = '|'.join([child.text for child in root if child.text])
    return result


if __name__ == '__main__':
    """Designed with the sole intent of making it easy to pipe input
    from a db query - and spit out the striped xml content as found
    in hl7_obx.observation_result

    """
    input = sys.stdin.readlines()
    for line in input:
        if line.count('<'):
            xml_start = line.index('<')
            xml_end = line.rfind('>') + 1
            output = line[0:xml_start]
            output += strip(line[xml_start:xml_end])
            output += line[xml_end:]
            print output,
        else:
            print line
