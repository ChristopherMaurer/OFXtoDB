import OFXWriter
import psycopg2
import psycopg2.extras


class PostgresDBWriter(OFXWriter.Writer):
    def __init__(self, plist):
        phost = plist['host'] if 'host' in plist else None
        pport = plist['port'] if 'port' in plist else None
        pdbname = plist['dbname'] if 'dbname' in plist else None
        puser = plist['user'] if 'user' in plist else None
        ppwd = plist['password'] if 'password' in plist else None
        self.__schema = plist['schema'] if 'schema' in plist else None
        mappingsource = plist['mapping'] if 'mapping' in plist else None
        mappingtable = plist['OFXmapping'] if 'OFXmapping' in plist else None
        self.DBSession = psycopg2.connect(host=phost, port=pport, dbname=pdbname, user=puser, password=ppwd)
        curs = self.DBSession.cursor()
        curs.execute("select version()")
        self.destination = curs.fetchone()[0].split(",")[0]
        curs.close()

# I recommend not using this UseIniFile option for Postgres.  The table specs would then be resident both in the data
#  base and in the .ini file, and to the extent they do not agree it's just one more source of errors.  Also, the
#  specification gatherer in Postgres is a query that is aware of foreign key relationships between tables and will
#  re-order the processing of OFXLists and tables automatically to avoid referential integrity problems.
        if mappingsource!='UseIniFile':
            self.MapSrc = MappingFromDB(self.__schema,self.DBSession,mappingtable)
        super().__init__(plist)     # Finish the mapping spec processing in the base class init using MappingFromDB
                                    #  class (below) to deliver the mapping record-by-record.

# Most of the data accumulation logic has been placed into the superclass (in OFXWriter) and only the actual data base
#   interfacing logic (query mapping specs and db Writes/Updates) is kept here.

#   Now that an entire OFX list is complete, do a 2-step write to the DB.  First, write it all to an identical TEMP
#     table in the DB. Then execute a MERGE to not duplicate any common keys.  I was on the fence as to whether to
#     delete from DTSTART to DTEND first, in case intervening corrections have removed items at the F.I.  But ultimately
#     I did not do it.
    def OFXListEnd(self):
        if self.curOFXList is not None:
            for EachTable in self.curOFXList:
                datatuple = self.curOFXList[EachTable][0]
                TempTblCreate = 'Create Temp Table "{table}_Hold" (LIKE "{schema}"."{table}")'.format(table=EachTable,schema=self.__schema)
                TempTblInsert = 'Insert Into "{table}_Hold" ({cols}) VALUES %s'.format(table=EachTable,
                                                    cols=",".join(['"{0}"'.format(i) for i in datatuple.Cols]))
# interpose a Select to distinguish Updates from Inserts in the following Merge to give better statistics
                if len(datatuple.PKCols) > 0:
                    QryMatches = 'Select count(*) From "{table}_Hold" t Inner Join "{schema}"."{table}" M'.format(table=EachTable,schema=self.__schema)
                    QryMatches +=" ON " + " AND ".join(['M."{pk}"=t."{pk}"'.format(pk=i) for i in datatuple.PKCols])
                else:
                    QryMatches = 'Select 0'
# Merge statement to go from the temp table to the permanent one.  This blows up with a SQL error for tables without
#   primary keys (or whose primary keys are not in the mapping, such as a sequence).  I think this is preferable to
#   always Inserting records, because re-runs of the same file, or running overlapping date ranges from two files will
#   produce duplicate transaction records.
                PermMerge = 'Merge Into "{schema}"."{table}" M Using "{table}_Hold" t'.format(table=EachTable,schema=self.__schema)
                PermMerge += " ON " + " AND ".join(['M."{pk}"=t."{pk}"'.format(pk=i) for i in datatuple.PKCols])
                PermMerge += ' When Matched Then Update Set '
                connector = ''
                for tbCols in datatuple.Cols:
                    if tbCols not in datatuple.PKCols:
                        PermMerge += connector + '"{0}"=t."{0}"'.format(tbCols)
                        connector = ', '
                PermMerge += ' When Not Matched Then Insert ({cols}) Values '.format(
                    cols=",".join(['"{0}"'.format(i) for i in datatuple.Cols]))
                PermMerge += '(' + ', '.join(['t."{col}"'.format(col=i) for i in datatuple.Cols]) + ')'
                DropTemp = 'Drop Table "{table}_Hold"'.format(table=EachTable)

#   Get a cursor to execute the SQL steps
                curs = self.DBSession.cursor()
                curs.execute(TempTblCreate)
                # execute_values is happiest with the records in a list of tuples, that's why they were saved that way.
                psycopg2.extras.execute_values(curs,TempTblInsert,self.curOFXList[EachTable][2])
                curs.execute(QryMatches)
                NbrUpdates = curs.fetchone()
                curs.execute(PermMerge)
                NbrTotal = curs.rowcount
                curs.execute(DropTemp)
                self.DBSession.commit()
                curs.close()
                if EachTable not in self.Stats:
                    self.Stats[EachTable]=[0,0]  # Update statistics
                self.Stats[EachTable][0] += NbrTotal-NbrUpdates[0]
                self.Stats[EachTable][1] += NbrUpdates[0]

#   Finish up by closing out the DB session & releasing server resources
    def OFXAllDone(self):
        self.DBSession.close()

# The following class is a specification (mapping) reader.  This one gets its data out of the data base itself, from a
#  mapping table whose name is passed in as maptable and various metadata elements from information_schema & pg_catalog.
#  Its sole purpose is to provide those records to the caller as an Iterator so it can build the internal mapping data.
class MappingFromDB(OFXWriter.MappingFromIniFile):
    def __init__(self,useschema,usesession,maptable):
        self.schema=useschema
        self.session = usesession
        self.maptable = maptable

    def __iter__(self):
        self.curs = self.session.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        self.curs.execute('''With Recursive dependencies AS (
        	Select table_schema,table_name, 0 as dependency_level, table_schema as foreign_schema,table_name as foreign_name, ot."OFXList" as ofxgroup
        	from information_schema.tables it Inner Join (Select Distinct "DBTable", "OFXList" From "{0}"."{1}") ot on it.table_name=ot."DBTable"
        	Where it.table_schema=\'{0}\'
          UNION ALL
        	Select d.table_schema,d.table_name, d.dependency_level+1, f.table_schema as foreign_schema, f.table_name as foreign_name, d.ofxgroup
        	From dependencies d Inner Join (
        		SELECT t.table_schema,t.table_name, c.table_schema as foreign_schema, c.table_name as foreign_table
        		FROM information_schema.table_constraints t Inner Join information_schema.constraint_column_usage c
        			  ON t.constraint_catalog=c.constraint_catalog and t.constraint_schema=c.constraint_schema and t.constraint_name=c.constraint_name
        		Where t.table_schema=\'{0}\'  and t.constraint_type=\'FOREIGN KEY\'
        		Group By t.table_schema,t.table_name, c.table_schema, c.table_name
        	  ) f ON d.foreign_schema=f.foreign_schema and d.foreign_name=f.foreign_table
        ), dependency AS (
        	Select ofxgroup,table_schema,table_name, max(table_dependency) as table_dependency,  max(list_dependency) as list_dependency
        	From (
        		Select ofxgroup,table_schema,table_name, max(dependency_level) Over (Partition By ofxgroup,table_schema,table_name) as table_dependency
        			  , max(dependency_level) Over (Partition By ofxgroup) as list_dependency
        		from dependencies
        	) x
        	Group By ofxgroup,table_schema,table_name
        )
        Select c."OFXList", c."DBTable", c."OFXTag", c."DBColumn", columns.typcategory, columns.pk as "IsPK", columns.ordinal_position
           ,Coalesce(lead(c."OFXList") Over (Order By c."OFXList",c."DBTable",ordinal_position,"OFXTag"),\'\') <> c."OFXList" as newlist
           ,Coalesce(lead(c."DBTable") Over (Partition By c."OFXList" Order By c."DBTable",ordinal_position,"OFXTag")
        	 ,\'\') <> c."DBTable" as newtable
        From "{0}"."{1}" c Inner Join
          ( Select c.*, pk.table_name is not null as PK
        	From (Select xc.*, xt.typcategory
                  from information_schema.columns xc Left Outer Join pg_catalog.pg_namespace ns on xc.udt_schema=ns.nspname
                    Left Outer Join pg_catalog.pg_type xt ON xc.udt_name=xt.typname and ns.oid=xt.typnamespace
                  Where xc.table_schema=\'{0}\'
                  Order By table_name) c
        	 Left Outer Join
        		(Select cc.table_name,cc.column_name
        		 from information_schema.table_constraints tc
        			  Inner Join information_schema.constraint_column_usage cc
        			  On cc.constraint_catalog=tc.constraint_catalog and cc.constraint_schema=tc.constraint_schema
        				 and cc.constraint_name = tc.constraint_name
        		Where tc.table_schema=\'{0}\' and tc.constraint_type=\'PRIMARY KEY\') pk
        	  On c.table_name=pk.table_name and c.column_name=pk.column_name) columns
           On columns.table_name=c."DBTable" And columns.column_name=c."DBColumn"
          Left Outer Join dependency d on c."DBTable" = d.table_name and c."OFXList"=d.ofxgroup
        Order By Coalesce(d.list_dependency,0) desc,c."OFXList",Coalesce(d.table_dependency,0) desc,c."DBTable",ordinal_position,c."OFXTag"
        '''.format(self.schema, self.maptable))
    # Building a complex, three-layered data structure to control the translation of OFX tags into (possibly multiple)
    #   DB tables as specified in dedicated tables referenced in the above query.  All tables must be in the referenced
    #   schema.  The table specified in self.maptable contains the OFX tag to database table-column mapping. It allows
    #   different OFX tags to map to the same DB column element in a table (e.g. DTTRADE and DTPOSTED both can be mapped
    #   to Tran_Date), allows the same OFX tag to map to different columns in different tables (e.g. FITID can map to
    #   each table in an OFXlist), but does not allow a single OFX tag to map to two columns in the same table
    #   (e.g. DTPOSTED to both Tran_Date and Settle_Date) due to the nature of the OFXElement dictionary.  Normally I
    #   would expect one DB table per OFX list, but your data model may vary.
    #
    #   The entire WITH clause (CTE) at the top (first 22 lines) solely creates two Order By variables to order the list
    #   processing and data base writes to avoid foreign key entanglements. If you don't use referential integrity,
    #   it does nothing useful.
    #
    # Possible enhancement: Change data structure of mapping to allow data from OFX tag to go to 2 different columns in
    # the same table, although you can mostly accomplish this on the data base side as well through triggers or
    # generated columns (Postgres-only).
    #
        return self

    def __next__(self):
        rcd = self.curs.fetchone()
        if rcd is not None: return rcd
        self.curs.close()
        raise StopIteration
