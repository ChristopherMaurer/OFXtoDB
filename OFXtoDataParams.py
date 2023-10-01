import configparser
import platform
import os

# The config parameters are in three parts, to take advantage of how Configparser overlays identical entries.  The first
#   part are the default values that can be overridden by entries in the actual OFXtoDB.ini file.  The second layer is
#   reading what is in the actual .ini file.  The third layer are system-layer parameters that must not be altered by
#   the user.
def readconfig(argv):
    OFXFile = 'OFXFile = ' + argv[1] if len(argv)>1 else ''
    overridable = '''# These are the default parameters for the OFXtoData program.  They can be overridden in the .ini
    [common]
    TimeZone = America/New_York
    Writer = Postgres
    
    [Postgres]
    host=localhost
    port=5432
    dbname=postgres
    user=postgres
    OFXmapping = OFX_to_Tables
    
    [CSV]
    WhenToQuote = SeparatorOnly
    ExcelCompatibility = Yes
    QuoteChar = "
    ColumnSeparator = ,
    '''

    NoOverride = '''# These are internal parameters that cannot be altered/overridden.
    [common]
    {0}
    
    # Includes the list of supported Writers which must accurately reflect those in ChooseWriter.
    [Writers]
    Postgres
    Excel
    CSV
    
    [OFXListUniverse]
    SECLIST
    INVPOSLIST
    INVTRANLIST
    BANKTRANLIST
    BANKTRANLISTP
    LOANTRANLIST
    AMRTTRANLIST
    CLOSING
    '''.format(OFXFile)

    cf = configparser.ConfigParser(allow_no_value=True)
    cf.optionxform = lambda option: option      #  Preserve case on keys
    cf.read_string(overridable, 'ProgramDefaults')
    try:
        f = open('./OFXtoData.ini', 'r')
    except OSError:
        if platform.system()=="Windows": f = open(os.path.expandvars("%APPDATA%\OFXtoDB\OFXtoDB.ini"), 'r')
    cf.read_file(f, 'ExternalFile')
    f.close()
    cf.read_string(NoOverride, 'internals')
    return cf
