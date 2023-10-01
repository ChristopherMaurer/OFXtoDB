#  To build a new outputter (Writer) create a new class that inherits from this class (Writer).  In it, you can override
#   as many of the below event calls as you wish, including __init__.  The easiest way to do this is to let the base
#   class take the mapping specs from OFXtoDB.ini (in its __init__() procedure) and then accumulate an entire list of
#   records in memory before writing.  That way you only need to override the OFXListEnd procedure, but you will need to
#   know how to traverse the curOFXList data structure (which is a little arcane).  If you do not use the OFXListDict
#   specification format then you must provide a custom iterator to help the main program establish an order for
#   processing the lists that does not interfere with things like referential integrity in data bases.  Then in
#   ChooseWriter, add an easily understood string mnemonic in an elif statement and call the initiator of your new
#   class, returning that object to the caller.  Finally, in OFXtoDataParams, add your new mnemonic to the NoOverride
#   overlay in the [WRITERS] section.
#
#   If you need to pass in parameters to your class initiator, add a section named as your string mnemonic in your .ini
#   file and list your parameters below it.  The parameters will be in a list of 2-tuples - name + value.  For example,
#   the Writer with code in PostgresWriter.py is triggered with the mnemonic "POSTGRES".  There is also a section called
#   [POSTGRES] in the .ini file where the 4 login parameters (host, dbname, user, & password) are supplied to the
#   Postgres object initializer to initiate a session.  You can also add default values for any of your parameters by
#   adding that section into the overridable variable.  This was done for host=localhost, dbname=postgres, and
#   user=postgres in the Postgres Writer because that's a pretty common usage in single-user Postgres.

import collections
from decimal import Decimal
from operator import itemgetter
import copy
import re
import datetime
import html
import OFXGlobals

class Writer:
    #  List of events called by the driver.  ListStart & ListEnd delimit an <INVTRANLIST>,
    #       <INVPOSLIST>, or a <SECLIST>
    def OFXListStart(self, ofxlist):
        if ofxlist in self.OFXListDict:
            self.curOFXList = self.OFXListDict[ofxlist]
            for EachTable in self.curOFXList:
                self.curOFXList[EachTable][2].clear()   # Clear out the previous accumulated record for this table, if any
                self.curOFXList[EachTable][3].clear()   # That goes for the accumulated list of PKs as well
        else:
            self.curOFXList = None
        return self.curOFXList

    def OFXListEnd(self):   # This should mostly be overridden by the sub-class to perform the actual writes.
        return

#  RecStart and RecEnd  delimit each transaction, position, or security in the list

    #   Initialize a new data record
    def OFXRecStart(self):
        if self.curOFXList is not None:
            for EachTable in self.curOFXList:
                self.curOFXList[EachTable][1] = copy.copy(
                    self.curOFXList[EachTable][0].BlankRec)  # Put a new blank record ready for data reception
        return

#   The OFX data elements are character strings that sometimes must be cast into the right format to be output to the DB
#   fmt is adapted from the Postgres typcategory from the pg_catalog.pg_types table (see query in Postgres mapping
#   iterator).  This is only called by OFXPutData.
    def __DestFmt(self,str,fmt):
        match fmt:
            case 'E' | 'S':
                return html.unescape(str)  # Data bases are not the internet - we hate &amp; &quot; and their ilk.
            case 'N':
                return Decimal(str)
            case 'B':
                if str.upper() in ['Y','YES','T','TRUE']:
                    return True
                elif str.upper() in ['N', 'NO', 'F', 'FALSE']:
                    return False
                else:
                    return None
            case 'D' | 'DATE':
                rgmatch = re.search(r"^(\d{8})(\d{6})(\.(\d+))?(\[([+-]?\d{1,2})(:(\w+))?])?$", str)
                if rgmatch:
                    if not rgmatch.group(2):
                        groomedvalue = rgmatch.group(1)+'000000.000000[{0:+05d}]'.format(int(OFXGlobals.TargetTZ.utcoffset(None).seconds/60))
                    else:
                        fractsec = int(rgmatch.group(4) if rgmatch.group(4) else '0')
                        tznum = int(rgmatch.group(6)) if rgmatch.group(6) else 0
                        tztext = rgmatch.group(8) if rgmatch.group(6) else "GMT"
                        groomedvalue = '{0}{1}.{2:06d}[{3:+05d}]'.format(rgmatch.group(1), rgmatch.group(2),
                                        fractsec, tznum*100)
                    dttimeval = datetime.datetime.strptime(groomedvalue,
                                            '%Y%m%d%H%M%S.%f[%z]').astimezone(tz=OFXGlobals.TargetTZ)
                    dttimeval = dttimeval.replace(tzinfo=None)  # after converting to desired time zone, make the timestamp naive
#  To make comparisons with EXCEL dates work right store DATE formats as date-times at midnight.
                    if fmt == 'DATE': dttimeval = datetime.datetime(dttimeval.year,dttimeval.month,dttimeval.day, 0,0,0 )
                    return dttimeval
                else:
                    return None
            case _:
                return None


    #   Put this tag into every table (zero or once) where the tag appears in the OFXElementDict
    def OFXPutData(self, tag, value, parent):
        if self.curOFXList is not None:
            for EachTable in self.curOFXList:
                datatuple = self.curOFXList[EachTable]
                if tag in datatuple[0].OFXDict:
                    el = datatuple[0].OFXDict[
                        tag]  # OFXDict dictionary values are a pair: pos = offsets into data, fmt = desired data format
                    datatuple[1][el.pos] = self.__DestFmt(value, el.fmt)
                elif parent in datatuple[0].OFXDict:
                    el = datatuple[0].OFXDict[
                        parent]  # tag not in dictionary, but try parent/tag to avoid ambiguity
                    datatuple[1][el.pos] = self.__DestFmt(value, el.fmt)
        return

    #   Append completed record(s) to an internal list of records (RecCollect).  Defer DB writes until the entire list
    #     is complete
    def OFXRecEnd(self):
        if self.curOFXList is not None:
            for EachTable in self.curOFXList:
                datatuple = self.curOFXList[
                    EachTable]  # Records are accumulated as a list (mutable), then saved as a tuple to be written later
                PKtuple = tuple(datatuple[1][PKs] for PKs in datatuple[0].PKCols.values())
                if PKtuple is None or PKtuple not in datatuple[3]:  # discard any duplicated records (you get these in SECLISTs sometimes)
                    datatuple[2].append(tuple(datatuple[1]))  #
                    datatuple[3].append(PKtuple)
        return

    def OFXAllDone(self):
        return

    def __init__(self, plist):
        if not hasattr(self, 'MapSrc'):
            self.MapSrc = MappingFromIniFile()
        self.OFXListDict = {}  # A dictionary of OFX lists we want processed (e.g. INVTRANLIST, INVPOSLIST, SECLIST)
                          #  each entry contains a list of table names to process by indexing into OFXTableDict.
        DBTableDict = {}  # Each table dictionary has a list of database columns (in order), the table name it
                          # goes to, and the OFX dictionary that defines its data elements.
        OFXElementDict = {}  # Each Element dictionary describes positionally the OFX element that goes in each table.
        TableCols = {}
        TablePKs = {}    # Need to know this for the Merge statement join
        BlankDataRecord = []
#   A couple of named tuples defined here (because names are much more readable than indexes).
        self.TableSpecs = collections.namedtuple('TableSpecs',
                                            ['Cols','PKCols','OFXDict','BlankRec'])  # The static mapping data (per table)
        self.OFXEntry = collections.namedtuple('OFXEntry',['pos','fmt']) # The value part of the OFX element map. Integer position and one-char desired format
        self.Stats = {}
        for rec in self.MapSrc:
            if re.sub(r'^.*?(\w+)$',r'\1',rec.OFXList) in OFXGlobals.InThisFile:    # Only read entries for lists known to exist in this file
                if rec.DBColumn not in TableCols:
                    BlankDataRecord.append(None)
                    TableCols[rec.DBColumn] = len(BlankDataRecord) - 1
                OFXElementDict[rec.OFXTag] = self.OFXEntry(TableCols[rec.DBColumn], rec.typcategory)  #This determines where the OFX value goes in the output tuple
                if rec.IsPK:
                    TablePKs[rec.DBColumn] = TableCols[rec.DBColumn]
                if rec.newtable:    # Last element of a DB table - connect a tuple of TableCols & OFXElementDict to the TableDict
                    DBTableDict[rec.DBTable] = [self.TableSpecs(TableCols, TablePKs, OFXElementDict, BlankDataRecord),
                                                                 BlankDataRecord, [], []]
                    TableCols = {}
                    TablePKs = {}
                    OFXElementDict = {}
                    BlankDataRecord = []
                    if rec.newlist:   #Last element of an OFX list, add the list name and the dictionary of datatables to OFXListDict
                        self.OFXListDict[rec.OFXList] = copy.deepcopy(DBTableDict)
                        DBTableDict = {}
        self.curOFXList = None
# An iterator to return all the OFX lists the writer cares about in the order it wants it processed
#   The initializer of the subclass must set up self.OFXListDict with the OFXLists as the keys.  The values can be
#   whatever is convenient to handle the record creation.
    def __iter__(self):
        if self.OFXListDict is not None:
            self.__list = list(self.OFXListDict.keys())
        return self

    def __next__(self):
        if len(self.__list) > 0:
            return self.__list.pop(0)
        raise StopIteration

class ConfigurationError(Exception):
    pass

class MappingFromIniFile():
    def __iter__(self):
        m = OFXGlobals.params.options('Mapping')
        if len(m) <= 0:
            raise ConfigurationError('No Mapping section in .INI file')
        rawMapList = []
        OFXListOrder = []
        TablenameOrder = []
        for i in m:
            mapitem = re.findall(r'[^\s,]+',i)
            if len(mapitem) != 4: raise ConfigurationError(
                "Mapping item ({0}) - should have 4 comma-separated items".format(", ".join(mapitem)))
            if OFXGlobals.params.has_section('Table:{0}'.format(mapitem[1])):
                try:
                    columnordinal = OFXGlobals.params.options('Table:{0}'.format(mapitem[1])).index(mapitem[3])
                except ValueError:
                    raise ConfigurationError("For mapping item ({0}): No column {1} in table {2}".format(
                        ", ".join(mapitem), mapitem[1], mapitem[3]))
                if mapitem[0] not in OFXListOrder: OFXListOrder.append(mapitem[0])
                if mapitem[1] not in TablenameOrder: TablenameOrder.append(mapitem[1])
                mapitem.extend([columnordinal, OFXListOrder.index(mapitem[0]), TablenameOrder.index(mapitem[1])])
                rawMapList.append(mapitem)
            else:
                raise ConfigurationError("For mapping item ({0}): No table named {1} in configuration".format(
                    ", ".join(mapitem), mapitem[1]))
        self.__maplist = sorted(rawMapList,key=itemgetter(5,6,4,2,3))
        self.MappingRecord = collections.namedtuple('MappingRecord',["OFXList", "DBTable", "OFXTag", "DBColumn", "typcategory", "IsPK", "ordinal_position","newlist","newtable"])
        return self

    def __next__(self):
        if len(self.__maplist) <= 0:
            raise StopIteration
        newtable = len(self.__maplist) == 1 or not (self.__maplist[0][0]==self.__maplist[1][0] and self.__maplist[0][1]==self.__maplist[1][1])
        newlist  = len(self.__maplist) == 1 or not  self.__maplist[0][0]==self.__maplist[1][0]
        coldefs = OFXGlobals.params.get("Table:{0}".format(self.__maplist[0][1]),self.__maplist[0][3])
        if not coldefs:
            raise ConfigurationError("Table definition missing for Table:{0} and Column:{1}".format(self.__maplist[0][1],self.__maplist[0][3]))
        cd = re.findall(r'[^\s,]+',coldefs)
        rec = self.MappingRecord(self.__maplist[0][0], self.__maplist[0][1], self.__maplist[0][2], self.__maplist[0][3],
                                 cd[0], len(cd)>1 and cd[1]=='PK', self.__maplist[0][4], newlist, newtable)
        self.__maplist.pop(0)
        return rec