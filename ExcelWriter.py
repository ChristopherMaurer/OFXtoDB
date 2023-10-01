import openpyxl
from openpyxl import load_workbook
import OFXWriter
import re

class ConfigurationError(Exception):
    pass


# Searching for duplicate primary keys is slow.  This attempts to speed it up by putting all primary keys into a
# dictionary structure, thus effectively making a hash index out of the primary key.  The dictionary key is the primary
# key of the EXCEL record and the value is the row number of the entry in the sheet, making it a direct access to the
# matching record.  The indexing is performed in Excel's initializer block with the idea of turning it into a parallel
# thread/process some day when true parallelism is possible in Python in frozen .EXEs (created with PYInstaller).
# This would let the indexing take place at the same time the main program was accumulating its data records before
# writing them out.
def IndexAllWorksheets(MapSpecs, wb):
    for OFX in MapSpecs:
        for tablename in MapSpecs[OFX]:
            if tablename in wb.sheetnames:
                ws = wb[tablename]
                if (not hasattr(ws,"PKIndexIsReady")) and ws.max_row>1:
                    ws.PKIndexIsReady = False
                    spectuple = MapSpecs[OFX][tablename][0]
                    for rows in ws.iter_rows(min_col=1, max_col=ws.max_column, min_row=1, max_row=1):
                        colhdrs = rows
                        break
                    ws.colnbrs = []
                    for OFXcol in spectuple.Cols:
                        for wscol in colhdrs:
                            if OFXcol == wscol.value:
                                ws.colnbrs.append(wscol.col_idx)
                                break
                        else:
                            raise ConfigurationError(
                                "Column {0} is not in spreadsheet for Table {1}".format(OFXcol, tablename))
                    pkcolnbrs = []
                    for PKItem in spectuple.PKCols:
                        for wscol in colhdrs:
                            if PKItem == wscol.value:
                                pkcolnbrs.append(wscol.col_idx-1)
                                break
                        else:
                            raise ConfigurationError(
                                "Column {0} is not in spreadsheet for Table {1}".format(PKItem, tablename))
                    ws.PKIndex = {}
                    rownum = 2
                    for row in ws.iter_rows(min_col=1, max_col=ws.max_column, min_row=rownum, max_row=ws.max_row):
                        PK = tuple(row[i].value for i in pkcolnbrs)
                        ws.PKIndex[PK] = rownum
                        rownum += 1
                    ws.PKIndexIsReady = True



class ExcelWBWriter(OFXWriter.Writer):
    def __init__(self, plist):
        fn = plist['ExcelFile'] if 'ExcelFile' in plist else None
        try:
            self.wb = load_workbook(filename=fn)
        except FileNotFoundError:
            self.wb = openpyxl.Workbook()
        self.wb.workbookname = fn  # save this so when we write it out again it can be under the same name
        self.destination = re.search(r"[^\\/]+$",fn).group(0)
        super().__init__(plist)
#  Now that all mapping specs are processed and the spreadsheet is in memory, Index each sheet's primary keys
        IndexAllWorksheets(self.OFXListDict, self.wb)
        self.__anychanges = False

    def OFXListEnd(self):
        if self.curOFXList is not None:
            for EachTable in self.curOFXList:
                datatuple = self.curOFXList[EachTable][0]
                if EachTable not in self.wb.sheetnames:
                    ws = self.wb.create_sheet(title=EachTable)
                    ws.append([i for i in datatuple.Cols ])
                    for rec in self.curOFXList[EachTable][2]:
                        ws.append(rec)
                    self.__anychanges = True
                    if EachTable not in self.Stats:
                        self.Stats[EachTable] = [0, 0]  # Update statistics
                    self.Stats[EachTable][0] += len(self.curOFXList[EachTable][2])
                else:
                    ws = self.wb[EachTable]
                    if EachTable not in self.Stats:
                        self.Stats[EachTable] = [0, 0]  # Get ready to update statistics
                    origmaxrow = ws.max_row
                    for datarow, thisPK in zip(self.curOFXList[EachTable][2], self.curOFXList[EachTable][3]):
                        self.__anychanges = True
                        destrow = ws.PKIndex[thisPK] if thisPK in ws.PKIndex else ws.max_row+1
                        for newvalue, destcol in zip(datarow,ws.colnbrs):
                            ws.cell(row=destrow, column=destcol).value = newvalue
                        if destrow>origmaxrow:
                            self.Stats[EachTable][0] += 1
                        else:
                            self.Stats[EachTable][1] += 1

    def OFXAllDone(self):
        if self.__anychanges: self.wb.save(self.wb.workbookname)
