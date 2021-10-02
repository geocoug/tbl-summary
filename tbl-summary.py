#! /usr/bin/python
# tbl-summary.py
#
# PURPOSE:
#       Create a summary of unique values for each column in a table.
#
# I/O:
#       Input: PostgreSQL table
#       Output: XLSX table
#
# NOTES:
#       1. Columns with "number" data types are not always included in dataframe.describe. CG.
#
# AUTHORS:
#       Caleb Grant (CG), Integral Consulting Inc.
#
# HISTORY:
#                             Date                     Comments
#                             ---------- -------------------------------------------------------------------------
#                             2020-10-23          Created.  CG.
#                             2020-10-26          Added header, comments. CG.
#                   Included outfile (-f) as script argument. Not a polished function. Should review. CG.
#                   Changed filter behavior for output. Instances where max worksheet dimension
#                       is falsly represented by metadata written in cells B1:G2.
#       2020-11-30  Error parsing date values. Attempt to fix by forcing np.datetime to string. CG.
#       2020-12-07  Change handling of datetime objects. Try Y-M-D H:M:S first, then Y-M-D. If both
#                       attempt fail, write object as string. CG.
#============================================================================================
 
# Standard libraries
import os
import sys
import argparse
from getpass import getpass
 
# Third-party libraries
import psycopg2
import pandas as pd
import numpy
from openpyxl import load_workbook
from openpyxl.styles import Border, Side, Alignment, PatternFill, Font, Color
from openpyxl.utils.cell import get_column_letter, coordinate_from_string, column_index_from_string
 
 
# Command line argument parser
def CLI():
    descrip = "Extract unique values and frequency for each column in a PostgreSQL table. Output saved to your current working directory."
    parser = argparse.ArgumentParser(description=descrip)
    parser.add_argument("-v", "--host",
                            action="store", dest="host", default="env3",
                            help="Server host name. Default = env3.")
    parser.add_argument("-u", "--user",
                            action="store", dest="user", default=None, required=True,
                            help="Username for host authentication.")
    parser.add_argument("-d", "--database",
                            action="store", dest="database", default=None, required=True,
                            help="Database name")
    parser.add_argument("-s", "--schema",
                            action='store', dest="schema", default="idb",
                            help="Schema name. Default = idb.")
    parser.add_argument("-t", "--table",
                            action="store", dest="table", default=None, required=True,
                            help="Table name.")
    parser.add_argument("-f", "--filename",
                            action="store", dest="outfile", default=None,
                            help='Name of the output file (without extension). File will be saved to your current working directory.')
    return parser
 
 
# OPEN DATABASE CONNECTION
def DB_Connect():
    connection_str = "host='%s' user='%s' password='%s' dbname='%s'" %(host, user, password, database)
    conn = None
    try:
       conn = psycopg2.connect(connection_str)
       cur = conn.cursor()
       return conn, cur
    except Exception as error:
       print(error)
       sys.exit()
 
 
# CLOSE DATABASE CONNECTION
def DB_Close(conn):
    if conn is not None:
        conn.close()
 
 
# QUERY DATABASE TABLE
def pulldata():
    conn, cur = DB_Connect()
    try:
        sql = """
            SELECT * FROM {}.{}
            """.format(schema, table)
        cur.execute(sql)
        data = cur.fetchall()
        cols = [i.name for i in cur.description]
    except Exception as error:
        print(error)
    finally:
        DB_Close(conn)
 
    return cols, data
 
 
# DATA SUMMARY OBJECT
class DataSummary(object):
    def __init__(self, df):
        if not df.empty:
            self.description = df.describe(percentiles=None, include='all')
            self.dtype = df.dtypes
            self.unique_vals = None
            self.unique(df)
            self.shape = df.shape
        else:
            self.description = None
            self.dtype = None
    def unique(self, df):
        self.unique_vals = {}
        for col in df.columns:
            try:
                self.unique_vals.update({col: sorted(df[col].unique())})
            except:
                # Occurances where "<" not a valid operator (sorted func) because of data type mismatch. Usually due to null values.
                df_c = df[col]
                df_c.dropna(axis='rows', inplace=True)
                self.unique_vals.update({col: sorted(df_c.unique())})
 
 
# WRITE OUTPUT
def writeSummary(file):
    # Excel cell formatting
    font = Font(
                bold=True,
                size=12
    )
    border = Border(
                bottom=Side(border_style=None,
                            color='FF000000'
                           )
    )
    alignment = Alignment(
                horizontal='general',
                vertical='bottom',
                text_rotation=0,
                wrap_text=False,
                shrink_to_fit=False,
                indent=0
    )
    fill = PatternFill(
                patternType='solid',
                fgColor=Color('97b4c9')
    )
    border_all = Border(
                left=Side(border_style='thin', color=Color('000000')),
                right=Side(border_style='thin', color=Color('000000')),
                top=Side(border_style='thin', color=Color('000000')),
                bottom=Side(border_style='thin', color=Color('000000'))
    )
 
    # Filter pandas dataframe.describe object
    for index in desc.description.index:
        if index not in ['freq', 'unique', 'top']:
            desc.description.drop(index=index, inplace=True)
 
    # Write pandas dataframe.describe object to sheet
    with pd.ExcelWriter(file) as writer:
        desc.description.to_excel(writer, sheet_name='Data Summary')
 
    # Re-open workbook
    wb = load_workbook(file)
    ws = wb['Data Summary']
 
    # Delete first row (column names)
    ws.delete_rows(1)
    # Insert 3 blank rows at top of sheet
    for i in range(3):
        ws.insert_rows(1)
 
    col_index = 2
    headers = list(desc.unique_vals)
    # Write table column names
    for col in range(len(desc.unique_vals)):
        head = ws.cell(row=7, column=col_index)
        head.value = headers[col]
        head.font=font
        head.border=border_all
        head.fill=fill
 
        # Write unique values for each table column
        for row in range(len(desc.unique_vals[headers[col]])):
            cell = ws.cell(row=row+8, column=col_index)
            # Case for timestamp columns          
            if isinstance(desc.unique_vals[headers[col]][row], numpy.datetime64):
                if pd.isnull(numpy.datetime64(desc.unique_vals[headers[col]][row])):
                    cell.value = None
                else:
                    try:
                        cell.value = pd.to_datetime(str(desc.unique_vals[headers[col]][row])).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        try:
                            cell.value = pd.to_datetime(str(desc.unique_vals[headers[col]][row])).strftime('%Y-%m-%d')
                        except:
                            cell.value = str(desc.unique_vals[headers[col]][row])
            else:
                cell.value = str(desc.unique_vals[headers[col]][row])
 
        col_index += 1
 
    # Manually write and format to cells
    c=ws.cell(row=4, column=1)
    c.value = '# of unique values'
    c.font=font
    c.border=border
    c.alignment=alignment
 
    c=ws.cell(row=5, column=1)
    c.value = 'most frequent value'
    c.font=font
    c.border=border
    c.alignment=alignment
 
    c=ws.cell(row=6, column=1)
    c.value = 'value frequency'
    c.font=font
    c.border=border
    c.alignment=alignment
 
    c=ws.cell(row=7, column=1)
    c.value = 'column names'
    c.font=font
    c.border=border
    c.alignment=alignment
 
    ws.cell(row=1, column=2).value = 'Host'
    ws.cell(row=1, column=2).font=font
    ws.cell(row=2, column=2).value = host
 
    ws.cell(row=1, column=3).value = 'Database'
    ws.cell(row=1, column=3).font=font
    ws.cell(row=2, column=3).value = database
 
    ws.cell(row=1, column=4).value = 'Schema'
    ws.cell(row=1, column=4).font=font
    ws.cell(row=2, column=4).value = schema
 
    ws.cell(row=1, column=5).value = 'Table'
    ws.cell(row=1, column=5).font=font
    ws.cell(row=2, column=5).value = table
 
    ws.cell(row=1, column=6).value = 'Total Rows'
    ws.cell(row=1, column=6).font=font
    ws.cell(row=2, column=6).value = desc.shape[0]
 
    ws.cell(row=1, column=7).value = 'Description'
    ws.cell(row=1, column=7).font=font
    ws.cell(row=2, column=7).value = 'Showing distinct values for each column in table. Columns with no values are excluded.'
 
    # Get worksheet dimensions.
    ws_dimensions = ws.dimensions
    end_dim = ws_dimensions.split(":")[-1]
    # Check to see if column "G" is the max column
    # Metadata is written to cells B1:G2 so column G may not be the actual max dimension of the table
    end_col = column_index_from_string(coordinate_from_string(end_dim)[0])
    end_row = coordinate_from_string(end_dim)[1]
    if end_col == 7:
        tot_cols = len(headers) + 1
        target_col = get_column_letter(tot_cols)
        end_dim = "{}{}".format(target_col, end_row)
 
    # Turn on filter
    ws.auto_filter.ref = "B7:{}".format(end_dim)
    # Change column A cell width
    ws.column_dimensions['A'].width = 25
 
    wb.save(file)
 
 
# BEGIN SCRIPT
if __name__ == "__main__":
    global args
    parser = CLI()
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit(1)
 
    # Get argparse namespace variables
    host=args.host
    user=args.user
    database=args.database
    schema=args.schema
    table=args.table
 
    # Make sure file name argument is valid
    # Not a polished check, could use revision
    if args.outfile:
        outfile = args.outfile + ".xlsx"
        is_valid = False
        try:
            with open(os.path.join(os.getcwd(), outfile), 'w') as f:
                pass
            os.remove(os.path.join(os.getcwd(),outfile))
        except:
            print("Filename not valid.")
            sys.exit()
    else:
        outfile= "{}.{}-summary.xlsx".format(schema, table)
 
    # Prompt user for password
    password = getpass(prompt="Password for (host={} | user={}):".format(host, user))
 
    print('Compiling summary.')
    # Database query
    cols, data = pulldata()
    # Query results to dataframe format
    df = pd.DataFrame(data, columns=cols)
    # Drop columns that contail no records
    df.dropna(axis='columns', how='all', inplace=True)
    # Create summary
    desc = DataSummary(df)
 
    print('Writing output.')
    writeSummary(outfile)
 
    print('Done.')
