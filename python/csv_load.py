# ============================================================================
# Name: csv_load.py
#
# Description:
# Load CSV file into Oracle table.  This can be called without any parameters
# which case user will be prompted.  
# Script can also be invoked as follows:
#   python csv_load.py [d|t|s|p] [OracleTable] [CSV File]
# where
#  d=DEV | t=TEST | s=STAGE | P-PROD
#  OracleTable = Oracle Table to be Loaded
#  CSV File    = Name of CSV file to load into Oracle (default | delimited)
# ============================================================================
# Revision History:
# Date       By        Comment
# ---------- --------- -----------------------------------------------------
# 09/05/2019 jhoang    Original release.
# ============================================================================

import os
import sys
import csv
import string
import getpass
import datetime
import cx_Oracle

sid = {'d': 'localhost:12000/ESSORAD'
      ,'t': 'localhost:11000/ESSORAD'
      ,'s': 'localhost:14000/ESSORAS'
      ,'p': 'localhost:10000/ESSORAP'}
dbe = ('d','t','s','p')
pwd = {'d': 'SamplePW', 't': 'SamplePW', 's': 'SamplePW', 'p': 'SamplePW'}

class MyException (Exception):
    pass

# ====================================================================
# Connect to Oracle
# ====================================================================

def ConnOra (env):
    global con
    if env not in dbe:
        raise MyException ("Invalid Database Environment: " + env)
    try:
        cstr = "ESS_CONV/" + pwd[env] + "@" + sid[env]
        con  = cx_Oracle.connect (cstr, encoding="UTF-8", nencoding="UTF-8")
    except cx_Oracle.DatabaseError:
        print ("Unable to connect to Oracle (ess_conv@"+sid[env]+")")
        raise
    except MyException as err:
        print (err)
        raise
    except:
        print (sys.exc_info() [0])
        raise

# ====================================================================
# Obtain Oracle Table Columns
# ====================================================================

sql = "select column_name" \
           " ,data_type" \
      "  from user_tab_columns" \
      " where table_name = :tbn" \
      " order by column_id"

def GetTable (itb, cur):
    global vals
    global tbnm
    tbnm = itb.upper ()
    cur.arraysize = 1000
    try:
        cur.execute (sql, tbn=tbnm)
        tbdef = cur.fetchmany()
        lim = ''
        for i in range (len(tbdef)):
            vals = vals + lim + ':' + str(i+1)
            lim = ','
    except:
        print (sys.exc_info() [0])
        raise


# ====================================================================
# If no parameters passed, prompt 
# ====================================================================

if len (sys.argv) == 1:
    while True:
        try:
          # ipwd = getpass.getpass (prompt="ESS_CONV Password: ")
            ienv = input ("\nDatabase Environment (Dev[d], Test[t], Stage[s], or Prod[p]): ")
            env  = ienv[0].lower()
            ConnOra (env)
        except:
            pass
        else:
            break

elif len (sys.argv) < 4:
    print ("Usage: python csv_load.py [d|t|s|p] [OracleTable] [CSV File]")
    exit ()

else:
    ienv = sys.argv[1]
    env  = ienv[0].lower()
    try:
        ConnOra (env)
    except:
        print (sys.exc_info() [0])
        exit ()


# ====================================================================
# Get Oracle table to load
# ====================================================================

vals = ''
cur  = con.cursor ()

if len (sys.argv) == 1:
    while True:
        try:
            itbn = input ("\nLoad into Table: ")
            GetTable (itbn, cur)
        except:
            pass
        else:
            break
else:
    itbn = sys.argv[2]
    try:
        GetTable (itbn, cur)
    except:
        exit ()


ins_sql = "insert into " + tbnm + " values (" + vals + ")"
cur.execute ("alter session set nls_date_format = 'yyyy-mm-dd hh24:mi:ss'")


# ====================================================================
# Get CSV File to Load
# ====================================================================

if len (sys.argv) == 1:
    while True:
        try:
            ifile   = input ("\nCSV File to Load: ")
            csvfile = open (ifile, newline='')
        except:
            print (sys.exc_info() [0])
        else:
            break
else:
    ifile = sys.argv[3]
    try:
        csvfile = open (ifile, newline='')
    except:
        print (sys.exc_info() [0])
        exit ()


# ====================================================================
# Read CSV File and load into Oracle
# ====================================================================

nrow      = 0
inp_array = []
dt_start  = datetime.datetime.now()
reader    = csv.reader (csvfile, delimiter='|', quotechar='"')

for lin in reader:
    nrow += 1
    if nrow > 1:
        row = tuple (lin)
        inp_array.append (lin)
    if nrow % 500 == 0:
        cur.executemany (ins_sql, inp_array, batcherrors=True)
        for err in cur.getbatcherrors():
            print ("Error", error.message, "at row offset", err.offset)
        inp_array = []

if nrow % 500 != 0:
    cur.executemany (ins_sql, inp_array, batcherrors=True)
    for err in cur.getbatcherrors():
        print ("Error", error.message, "at row offset", err.offset)


dt_end = datetime.datetime.now()
print (tbnm+" Number of Row(s) Loaded = " + str (nrow-1))
print ("Date/Time Started: " + dt_start.strftime ("%m/%d/%Y %H:%M:%S"))
print ("Date/Time Ended:   " + dt_end.strftime ("%m/%d/%Y %H:%M:%S"))

con.commit ()
cur.close ()
con.close ()
csvfile.close ()
exit ()
