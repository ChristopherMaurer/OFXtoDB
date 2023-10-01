import PostgresWriter
import ExcelWriter
import DataDumper
import CSVWriter


def WhichWriter(choice="default", paramlist=None):
    if choice == 'Postgres':
        return PostgresWriter.PostgresDBWriter(paramlist)
    elif choice == 'Excel':
        return ExcelWriter.ExcelWBWriter(paramlist)
    elif choice == 'CSV':
        return CSVWriter.CSVWriter(paramlist)
    else:
        return DataDumper.TypeWriter()
