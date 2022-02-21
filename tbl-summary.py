#! /usr/bin/python
# -*- coding: utf-8 -*-
# tbl-summary.py

"""
PURPOSE
    Create a summary of unique values for each column in a Postgres table.

AUTHOR
    Caleb Grant (CG)

HISTORY
    Date          Remarks
    ----------	--------------------------------------------------
    2022-02-18    Created.  CG.
=================================================================="""

import argparse
import getpass
import os

import openpyxl
import pandas as pd
import psycopg2
from openpyxl.styles import Border, Color, Font, PatternFill, Side
from openpyxl.utils.cell import get_column_letter

__version__ = "0.1.1"
__vdate = "2022-02-18"


class Database():
    '''Base class to connect to a database and execute procedures.'''

    def __init__(self, host, db, user, password=None):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.conn = None
        self.password = self.get_password()

    def get_password(self):
        return getpass.getpass("Enter your password for %s" % self.__repr__())

    def open_db(self):
        if self.conn is not None:
            self.conn.close()
        if self.password is not None:
            try:
                self.conn = psycopg2.connect(f"""
                    host='{self.host}'
                    dbname='{self.db}'
                    user='{self.user}'
                    password='{self.password}'
                    """)
            except psycopg2.OperationalError as err:
                raise err

    def cursor(self):
        if self.conn is None:
            self.open_db()
        return self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute(self, sql):
        cur = self.cursor()
        try:
            cur.execute(sql)
        except Exception as err:
            raise err
        return cur

    def has_row(self, sql):
        cur = self.execute(sql)
        if cur.fetchone():
            return True
        else:
            return False

    def __repr__(self):
        return u"""Database(host=%s, database=%s, user=%s)""" % (
            self.host, self.db, self.user)


class DataSummary():
    '''Methods used to create attributes which summarize a data table.'''

    def __init__(self, db_inst, schema, table):
        self.db_inst = db_inst
        self.schema = schema
        self.table = table
        self.data = self.table_data()[0]
        self.total_rows = self.table_data()[1]
        self.df = pd.DataFrame(
            self.data,
            columns=self.columns()
        )

    def __repr__(self):
        return f"""DataSummary({self.db_inst},
                                schema={self.schema},
                                table={self.table})"""

    def columns(self):
        cur = self.db_inst.execute(f"""SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = '{self.schema}'
                            AND table_name = '{self.table}';""")
        return self.column_rows(cur)

    def unique(self, col):
        cur = self.db_inst.execute(f"""SELECT distinct "{col}"
                                        FROM "{self.schema}"."{self.table}"
                                        ORDER BY "{col}" NULLS LAST;""")
        return self.column_rows(cur), cur.rowcount

    def value_count(self, col):
        cur = self.db_inst.execute(f"""SELECT COUNT("{col}")
                                        FROM "{self.schema}"."{self.table}";
                                    """)
        return cur.fetchone()[0]

    def column_rows(self, cur):
        return [v[0] for v in cur]

    def most_frequent_value(self, col):
        cur = self.db_inst.execute(f"""SELECT "{col}", COUNT(*)
                                        FROM "{self.schema}"."{self.table}"
                                        GROUP BY "{col}"
                                        ORDER BY COUNT DESC
                                        LIMIT 1;""")
        return cur.fetchone()[0], cur.rowcount

    def table_data(self):
        cur = self.db_inst.execute(
            f"""SELECT * FROM "{self.schema}"."{self.table}";""")
        return cur.fetchall(), cur.rowcount


def clparser():
    '''Create a parser to handle input arguments and displaying
    a script specific help message.'''
    desc_msg = """Create a summary of unique values for each column
        in a Postgres table. Summarize results in an Excel workbook.
        Version %s, %s""" % (__version__, __vdate)
    parser = argparse.ArgumentParser(description=desc_msg)
    parser.add_argument('output_file',
                        help="Name of the XLSX output file")
    parser.add_argument('-v', '--host', type=str, default='env3', dest='host',
                        help="Server hostname.")
    parser.add_argument('-d', '--database', type=str, dest='database',
                        help="Database name.")
    parser.add_argument('-s', '--schema', type=str, dest='schema',
                        help='Database schema.')
    parser.add_argument('-u', '--username', type=str, dest='username',
                        help="Database username.")
    parser.add_argument('-t', '--table', type=str, dest='table',
                        help="Table name to summarize.")
    return parser


def write_summary(data, ofile):
    '''Write results to an output file.'''
    if os.path.exists(ofile):
        os.remove(ofile)

    wkb = openpyxl.Workbook()
    sheet = wkb.active
    sheet.title = 'Data Summary'

    # Excel styling
    font = Font(
        bold=True,
        size=12
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

    # Summary header
    sheet.cell(row=1, column=2).value = "Host"
    sheet.cell(row=1, column=2).font = font
    sheet.cell(row=2, column=2).value = data.db_inst.host

    sheet.cell(row=1, column=3).value = "Database"
    sheet.cell(row=1, column=3).font = font
    sheet.cell(row=2, column=3).value = data.db_inst.db

    sheet.cell(row=1, column=4).value = "Schema"
    sheet.cell(row=1, column=4).font = font
    sheet.cell(row=2, column=4).value = data.schema

    sheet.cell(row=1, column=5).value = "Table"
    sheet.cell(row=1, column=5).font = font
    sheet.cell(row=2, column=5).value = data.table

    sheet.cell(row=1, column=6).value = "Total Rows"
    sheet.cell(row=1, column=6).font = font
    sheet.cell(row=2, column=6).value = data.total_rows

    sheet.cell(row=1, column=7).value = "Description"
    sheet.cell(row=1, column=7).font = font
    sheet.cell(
        row=2, column=7).value = "Distinct values for each column in a table."

    # Summary results
    for col in range(len(data.columns())):
        # column = data.columns()[col]
        if col == 0:
            sheet.cell(row=4, column=col + 1).value = "# of unique values"
            sheet.cell(row=4, column=col + 1).font = font
            sheet.cell(row=5, column=col + 1).value = "most frequent value"
            sheet.cell(row=5, column=col + 1).font = font
            sheet.cell(row=6, column=col + 1).value = "value frequency"
            sheet.cell(row=6, column=col + 1).font = font
            sheet.cell(row=7, column=col + 1).value = "column name"
            sheet.cell(row=7, column=col + 1).font = font
        # Number of unique values
        sheet.cell(row=4,
                   column=col + 2).value = data.unique(data.columns()[col])[1]
        # Most frequent value
        sheet.cell(row=5,
                   column=col + 2).value = data.most_frequent_value(data.columns()[col])[0]
        # Value frequency
        sheet.cell(row=6,
                   column=col + 2).value = data.most_frequent_value(data.columns()[col])[1]
        # Column names
        sheet.cell(row=7, column=col + 2).value = data.columns()[col]
        sheet.cell(row=7, column=col + 2).font = font
        sheet.cell(row=7, column=col + 2).border = border_all
        sheet.cell(row=7, column=col + 2).fill = fill
        # Unique column values
        row = 8
        for value in data.unique(data.columns()[col])[0]:
            sheet.cell(row=row, column=col + 2).value = value
            row += 1
        # Turn on filter
        sheet.auto_filter.ref = f"B7:{get_column_letter(len(data.columns()) + 1)}7"
        # Column A cell width
        sheet.column_dimensions['A'].width = 25

    wkb.save(ofile)


if __name__ == "__main__":
    parser = clparser()
    args = parser.parse_args()

    if args.output_file:
        if not os.path.splitext(args.output_file)[-1] in ['.xlsx']:
            raise Exception(f"File extension not valid: {args.output_file}")

    db_inst = Database(
        args.host,
        args.database,
        args.username
    )

    schema_sql = f"""SELECT schema_name
              FROM information_schema.schemata
              WHERE schema_name = '{args.schema}';"""
    if not db_inst.has_row(schema_sql):
        raise Exception(f"Schema does not exist: {args.schema}")

    table_sql = f"""SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = '{args.schema}'
                    AND table_name = '{args.table}';"""
    if not db_inst.has_row(table_sql):
        raise Exception(f"Table does not exist: {args.schema}.{args.table}")

    data = DataSummary(db_inst, args.schema, args.table)

    write_summary(data, args.output_file)
