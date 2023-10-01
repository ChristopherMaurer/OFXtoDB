import OFXWriter
import datetime
import re

def FieldtoStr(field, colchar, quotechar, quoterule, ExcelStrings):
    if field == None:
        st = ""
    else:
        datatype = type(field).__name__
        match datatype:
            case 'Decimal' | 'bool':
                st = "{0}".format(field)
            case 'datetime':
                st = ("{0:%Y-%m-%d}".format(field)
                      + (" {0:%H:%M:%S}".format(field) if field.time() != datetime.time(0, 0, 0) else ""))
            case 'str':
                st = field
                if ExcelStrings.upper() in ['YES', 'Y', 'TRUE', 'T', 'ENABLED']:
                    if re.search(r'^\s*(.*{0}.*|[+-]?(\d*\.\d+|\d+.?)(E\d+)?)$'.format(colchar[0]), field, re.I):
                        st = '="{0}"'.format(field.replace('"', '""'))
                    elif re.search((
                            r'^\s*(((0?[1-9]|1[0-2])([-\\\/])(0?[1-9]|[12][0-9]|3[01])(?:\4((?:19|2[0-9])?[0-9][0-9]))?)'
                            r'|((Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|(?:Nov|Dec)(?:ember)?)'
                            r'\s+(0?[1-9]|[12][0-9]|3[01])(?:[ ,]\s*((?:19|2[0-9])?[0-9][0-9]))?)'
                            r'|(((?:19|2[0-9])[0-9]{2,2})([-\\\/])(0?[1-9]|1[0-2])\13(0?[1-9]|[12][0-9]|3[01])))\s*$'),
                            field, re.I):
                        st = '="{0}"'.format(field.replace('"', '""'))
                    elif field in ['TRUE', 'FALSE']:
                        st = '="{0}"'.format(field.replace('"', '""'))
                    else:
                        st = field
                if (colchar in st) or (quoterule == 'AllStrings'):
                    st = (
                        "{1}{0}{1}".format(st.replace(quotechar, quotechar + quotechar), quotechar))
    return st

class CSVWriter(OFXWriter.Writer):
    def __init__(self, plist):
        self.savedir = plist['WriteToDirectory'] if 'WriteToDirectory' in plist else "."
        self.includeheader = plist['Headers'] if 'Headers' in plist else 'NO'
        self.quoterule = plist['WhenToQuote']  # SeparatorOnly (only as necessary) or AllStrings (every string value)
        self.ExcelStrings = plist['ExcelCompatibility']  # TRUE = write special formulas to keep strings as strings.
        self.quotechar = plist['QuoteChar'][0]
        self.colchar = plist['ColumnSeparator'][0]
        super().__init__(plist)

    def OFXListEnd(self):
        if self.curOFXList is not None:
            for EachTable in self.curOFXList:
                f = open("{0}/{1}.csv".format(self.savedir, EachTable),'w+')
                if self.includeheader in ['YES', 'Y', 'TRUE', 'T', 'ENABLED']:
                    result = []
                    for hdr in self.curOFXList[EachTable][0].Cols:
                        st = FieldtoStr(hdr, self.colchar, self.quotechar, self.quoterule, self .ExcelStrings)
                        result.append(st)
                    f.write(self.colchar[0].join(result) + "\n")
                for rec in self.curOFXList[EachTable][2]:
                    result = []
                    for field in rec:
                        st = FieldtoStr(field, self.colchar, self.quotechar, self.quoterule, self .ExcelStrings)
                        result.append(st)
                    f.write(self.colchar[0].join(result) + "\n")
                f.close()