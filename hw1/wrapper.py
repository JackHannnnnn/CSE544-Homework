import xml.sax
import re


class DBLPContentHandler(xml.sax.ContentHandler):
  """
  Reads the dblp.xml file and produces two output files.
        pubFile.txt = (key, pubtype) tuples
        fieldFile.txt = (key, fieldCnt, field, value) tuples
  Each file is tab-separated

  Once the program finishes,  load these two files in a relational database; run createSchema.sql
  """

  def __init__(self):
    xml.sax.ContentHandler.__init__(self)


  def startElement(self, name, attrs):
    if name == "dblp":
      DBLPContentHandler.pubFile = open('pubFile.txt', 'w')
      DBLPContentHandler.fieldFile = open('fieldFile.txt', 'w')
      DBLPContentHandler.pubList = ["article", "inproceedings", "proceedings", "book", "incollection", "phdthesis", "mastersthesis", "www"]
      DBLPContentHandler.fieldList = ["author", "editor", "title", "booktitle", "pages", "year", "address", "journal", "volume", "number", "month", "url", "ee", "cdrom", "cite", "publisher", "note", "crossref", "isbn", "series", "school", "chapter"]
      DBLPContentHandler.content = ""
    if name in DBLPContentHandler.pubList:
      DBLPContentHandler.key = attrs.getValue("key")
      DBLPContentHandler.pub = name
      DBLPContentHandler.fieldCount = 0
      DBLPContentHandler.content = ""
    if name in DBLPContentHandler.fieldList:
      DBLPContentHandler.field = name
      DBLPContentHandler.content = ""
 
  def endElement(self, name):
    if name in DBLPContentHandler.fieldList:
      DBLPContentHandler.fieldFile.write(DBLPContentHandler.key)
      DBLPContentHandler.fieldFile.write("\t")
      DBLPContentHandler.fieldFile.write(str(DBLPContentHandler.fieldCount))
      DBLPContentHandler.fieldFile.write( "\t")
      DBLPContentHandler.fieldFile.write(DBLPContentHandler.field)
      DBLPContentHandler.fieldFile.write("\t")
      DBLPContentHandler.fieldFile.write(DBLPContentHandler.content)
      DBLPContentHandler.fieldFile.write("\n")
      DBLPContentHandler.fieldCount += 1
    if name in DBLPContentHandler.pubList:
      DBLPContentHandler.pubFile.write(DBLPContentHandler.key)
      DBLPContentHandler.pubFile.write("\t")
      DBLPContentHandler.pubFile.write(DBLPContentHandler.pub)
      DBLPContentHandler.pubFile.write("\n")

  def characters(self, content):
    DBLPContentHandler.content += content.encode('utf-8').replace('\\','\\\\')

def main(sourceFileName):
  source = open(sourceFileName)
  xml.sax.parse(source, DBLPContentHandler())
 
if __name__ == "__main__":
  main("/Users/Chaofan/Downloads/CSE544/hw1/dblp.xml")
