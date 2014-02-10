import unittest
from pheme.longitudinal.stripXML import strip

class TestStrip(unittest.TestCase):
    "At least one field is persisted in XML format - test strip"

    def testMultiField(self):
        "Several entities should be delimited"
        s = """<OBX.5><OBX.5.1>112283007</OBX.5.1><OBX.5.2>Escherichia coli (organism)</OBX.5.2><OBX.5.3>SN</OBX.5.3><OBX.5.4>EC</OBX.5.4><OBX.5.5>ESCHERICHIA COLI</OBX.5.5><OBX.5.6>L</OBX.5.6></OBX.5>"""
        r = strip(s)
        self.assertFalse('<' in r)
        self.assertEquals(r.count('|'), 5)

    def test5_4(self):
        s = '<OBX.5><OBX.5.1/><OBX.5.2/><OBX.5.3/>' +\
            '<OBX.5.4>CULTURE RESULT: ESCHERICHIA COLI</OBX.5.4>' +\
            '<OBX.5.5/><OBX.5.6>L</OBX.5.6></OBX.5>'
        r = strip(s)
        self.assertTrue(r.startswith("CULTURE RESULT: ESCHERICHIA COLI"))

    def testHtmlDecoding(self):
        "HTML encoding is also being done by mirth - test decoding"
        s = """<OBX.5><OBX.5.1>&gt;&gt; NO ANAEROBIC GROWTH TO DATE.</OBX.5.1></OBX.5>"""
        r = strip(s)
        self.assertTrue(r.find("&gt;") < 0)

    def testNone(self):
        "gotta handle emptys peacefully"
        self.assertEquals(strip(None), None)
        self.assertEquals(strip(''), '')
        
if '__main__' == __name__:
    unittest.main()
