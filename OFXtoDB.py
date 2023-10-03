#  Read a financial institution's Quicken-format file (.qfx) and produce data records that can be written to a variety
#   of formats: currently only Postgres data base and Excel is supported.  Most of a .qfx file consists of SGML/XML data
#   arranged according to an Open Financial eXchange protocol, or OFX.  The program is divided into three parts: this
#   main module is responsible for producing OFX data within a logical table and record paradigm.  The OFXWriter module
#   reads mapping specs in a very specific format, translates them into an internal data structure for efficient
#   execution, and casts (OFXTag,Value) pairs from the main module into a designated spot in one or more output records.
#   The subclasses of OFXWriter implement the actual writing of the records to the desired destination format.
#   Each Writer subclass handles a different output data format by implementing as many of the event functions
#   (__init__, OFXListStart, OFXRecStart, OFXPutData, OFXRecEnd, OFXListEnd, & OFXAllDone) through base class overrides
#   as needed to consume and write the data, although typically they only need to override __init__() and OFXListEnd and
#   leave the data accumulation up to the parent.
#
# This main module is list-oriented.  That is, it can traverse any of the eight lists defined by the OFX Standard,
#   delivering all the data within that list as well as any "nearby" data.  The consumer is responsible for defining
#   what lists they are interested in, what "nearby" means to them, which data elements they want to consume, and how
#   these elements map to their output data model.  This is resident in one definition table: OFXList->Tables defines
#   the OFX lists of interest and the physical output tables and OFXTag->Columns defines the element tags of interest
#   and how they map to physical columns within the tables.  These are both resident in a single DB table that can be
#   designated in the OFXtoDB.ini file or can even reside within the OFXtoDB.ini file.  For example, the OFX->Table
#   entry (INVSTMTRS/INVTRANLIST, Transactions) says that the list data enclosed by <INVTRANLIST> is to be mapped to the
#   Transactions logical table (which is a physical table in the Postgres Writer or a Worksheet name in the Excel
#   Writer).  In addition, all of the data elements between <INVSTMTRS> and <INVTRANLIST> are to be
#   provided within every record processed in the list (nearby data is added to the record context).  In this way, for
#   example, the Account number (ACCTID tag) can be added as part of each Transactions' primary key.  The model supports
#   multiple logical tables per OFX list, so in a Bank statement you can push summary balances to an Account table while
#   at the same time pushing debit/credit transactions to a Transactions table.  The OFX entry can be any valid XML path
#   entry (see the path documentation for xml.etree.ElementTree) as long as it ends in one of the known OFX-defined list
#   wrappers (SECLIST,INVPOSLIST,INVTRANLIST,BANKTRANLIST,BANKTRANLISTP,LOANTRANLIST,AMRTTRANLIST,CLOSING).
#
import sys
import OFXtoDataParams
import OFXGlobals
import WalkElementTree
import xml.etree.ElementTree
from ofxtools import OFXTree
from collections import namedtuple
from zoneinfo import ZoneInfo
import copy
import re
import ChooseWriter

def AddData(subTree,globalvars,listtag):    # These two functions get a lot of the list-specific logic out of the main process
    local = copy.deepcopy(globalvars)  # Walk the subTree, except for the known lists in the uppercontext
    save = [[subTree]]                 #  this gets all the uppercontext elements whose data goes in every record
    while len(save)>0:
        if len(save[-1])>0:    # save is a pushdown list of lists to remember where we still need to go
            e = save[-1][0]      # Next element
            if e.tag not in knownlists:  # don't traverse the known lists
                if e.text:  # elements have Text, aggregates do not.  However, this checks for children even with text
                    if len(save)>1 and len(save[-1])>0:
                        parent = save[-2][0].tag
                    else:
                        parent = ""
                    local.append(XMLPairs(e.tag, e.text,
                                 "{0}/{1}".format(parent, e.tag)))
                save.append(e.findall('*'))  # aggregate? If so, empty list will be appended and popped right off next
            else:
                if e.tag == listtag:     # Known exception: In the list we wish to process, elevate DTSTART & DTEND
                    for dttag in ['DTSTART', 'DTEND']:  #  to be context variables, not separate list entries
                        dt = e.find(dttag)
                        if dt is not None:
                            local.append(XMLPairs(dt.tag, dt.text, "{0}/{1}".format(e.tag,dt.tag)))
                save[-1].pop(0)   # Skip over the lists themselves - just doing upper context here.
        else:    # No more entries at this level. Pop it off and pop off the head entry just above it.
            save.pop(-1)
            if len(save)>0: save[-1].pop(0)
    return local


def ProcessListEntry(ListEntry:xml.etree.ElementTree.Element, WhatList):  # This adds two special tags not in the OFX spec - ELEMENTNAME & BUYSELL
    match WhatList:  # The BUYSELL pseudo-tag logic could also be migrated to the database, but not ELEMENTNAME logic
        case "INVTRANLIST":
            match ListEntry.tag:
                case _ if ListEntry.tag.startswith("BUY"):
                    DataWriter.OFXPutData("BUYSELL","BUY", "INVTRANLIST/BUYSELL")
                case _ if ListEntry.tag.startswith("SELL"):
                    DataWriter.OFXPutData("BUYSELL", "SELL", "INVTRANLIST/BUYSELL")
                case "INVBANKTRAN":
                    stmttrn = ListEntry.find("STMTTRN")
                    DataWriter.OFXPutData("BUYSELL","SELL" if stmttrn.find("TRNAMT").text.startswith('-') else "BUY", "INVTRANLIST/BUYSELL")
                case "INCOME" | "REINVEST":
                    DataWriter.OFXPutData("BUYSELL","INCOME", "INVTRANLIST/BUYSELL")
                case "INVEXPENSE" | "MARGININTEREST":
                    DataWriter.OFXPutData("BUYSELL","FEES", "INVTRANLIST/BUYSELL")
                case _:
                    DataWriter.OFXPutData("BUYSELL","OTHER", "INVTRANLIST/BUYSELL")
    DataWriter.OFXPutData("ELEMENTNAME",ListEntry.tag, WhatList + "/ELEMENTNAME")

#  MAIN PROGRAM STARTS HERE

parameters = OFXtoDataParams.readconfig(sys.argv)
OFXGlobals.params = parameters
OFXGlobals.TargetTZ = ZoneInfo(parameters['common']['TimeZone'])  # All timestamps will be cast to this IANA zone name.
if parameters.has_option('common', 'OFXFile'):
    OFXfile = parameters['common']['OFXFile']  # Filename can come either from commandline (argv[1]) or .ini file
else:
    sys.exit("No OFX file to process")
knownlists = [e[0].upper() for e in parameters.items('OFXListUniverse')]
FIStmt = OFXTree()       # Thanks to Chris Singley for his OFXTools.  After these two statements, file has been read in
FIStmt.parse(OFXfile)    #   and turned into a set of xml.etree.ElementTree.Elements.  Easy to walk this tree.
OFXGlobals.InThisFile = knownlists   # Chop down the lists to just those present in this file - save some processing later
for i in reversed(range(len(OFXGlobals.InThisFile))):
    if not FIStmt.find(".//"+ OFXGlobals.InThisFile[i]):
        OFXGlobals.InThisFile.pop(i)
XMLPairs = namedtuple("XMLPairs", "tag value alttag")
globalvars = []
x = FIStmt.find(".//SONRS//FI")  # As far as I can tell, FID & ORG are the only two elements of interest that are NOT...
globalvars.append(XMLPairs("FID", x.find("FID").text, "FI/FID"))  # inside the list or 'nearby' the list.  Put them in globalvars,
globalvars.append(XMLPairs("ORG", x.find("ORG").text, "FI/ORG"))  # where they will be transferred to listvars
wr = parameters['common']['Writer'] if 'Writer' in parameters['common'] else None
if wr:
    if wr in parameters['Writers']:  # See if chosen output formatter (Writer) is in the known list of Writers
        wrparams = {key:value for (key,value) in parameters.items(wr)} if wr in parameters else None
    else:
        sys.exit("{0} is not a valid output choice.  Valid writers are {1}".format(wr,','.join(parameters['Writers'])) )
else:
    wrparams=None
DataWriter = ChooseWriter.WhichWriter(wr,wrparams)
for OFXList in DataWriter:  # The Writer knows which OFX lists the user is interested in & what order to process
    searchable = './/' + OFXList
    if FIStmt.find(searchable):  # This code checks to see if there is at least one list in the file
        uppercontext = re.sub(r'(//?)?\w+$','',OFXList)  # Remove the bottom tag to create a 'nearby' context
        listtag = re.sub(r'^.*?(\w+)$',r'\1',OFXList)
        if uppercontext:
            uppercontext = './/' + uppercontext
        else:
            uppercontext = './/' + listtag
        DataWriter.OFXListStart(OFXList)
        for listcontext in FIStmt.iterfind(uppercontext):  # Some data sits in an upper context near the OFXList
            listvars = AddData(listcontext,globalvars,listtag)  # get all data outside the list proper
            for listwrapper in listcontext.iter(listtag):
                for listentry in listwrapper:
                    if listentry.tag not in ["DTSTART", "DTEND"]:
                        DataWriter.OFXRecStart()
                        ProcessListEntry(listentry, listtag)  # Each list entry tag carries information about the entry type
                        for lv in listvars:
                            DataWriter.OFXPutData(lv.tag, lv.value, lv.alttag)
                        for alldata in WalkElementTree.ElandParent(listentry):
                            if alldata[0].text is not None:
                                DataWriter.OFXPutData(alldata[0].tag, alldata[0].text, "{0}{1}".format(
                                            alldata[1].tag + "/" if alldata[1] is not None else "", alldata[0].tag))
                        DataWriter.OFXRecEnd()
        DataWriter.OFXListEnd()
DataWriter.OFXAllDone()
if len(DataWriter.Stats)>0:
    print("{0:25}{1:>10}{2:>10}".format(DataWriter.destination[0:24],'Added','Updated'))
    print("{0:25}{1:>10}{2:>10}".format(DataWriter.destination[25:49],'New','Existing'))
    for table,t in DataWriter.Stats.items():
        print("{0:25}{1:>10d}{2:>10d}".format(table,t[0],t[1]))
else:
    print("No relevant lists in the datafile.  No records were processed")
