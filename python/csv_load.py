# ============================================================================
# Name: csv_load.py
#
# Description:
# Load CSV file into Oracle table.  This can be called without any parameters
# so user will be prompted.  
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
# 09/10/2019 jhoang    Add generation of acknowledgment and error files.
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
    global ncol
    tbnm = itb.upper ()
    cur.arraysize = 1000
    try:
        cur.execute (sql, tbn=tbnm)
        tbdef = cur.fetchmany()
        lim = ''
        ncol = 0
        for i in range (len(tbdef)):
            vals = vals + lim + ':' + str(i+1)
            ncol += 1
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

ncol = 0
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
nerr      = 0
inp_array = []
dt_start  = datetime.datetime.now()
reader    = csv.reader (csvfile, delimiter='|', quotechar='"')
ackfn      = "ACK_" + tbnm + "_" + dt_start.strftime ("%Y%m%d") + ".csv"
errfn      = "ERR_" + tbnm + "_" + dt_start.strftime ("%Y%m%d") + ".csv"

for lin in reader:
    nrow += 1
    if nrow == 1:
        nfields = len (lin)
        if ncol != nfields:
            print ("Error - Num Oracle columns (",ncol,") does not match Num of Fields in CSV file (",len(lin),")")
            exit ()

        cridx   = lin.index ('ECDR_CREATE_DATE')
        upidx   = lin.index ('ECDR_UPDATE_DATE')
        tyidx   = lin.index ('RECORD_TYP')
        ackfile = open (ackfn, 'w', newline='')
        ackfile.write ("ECDR_Audit_Key|ECDR_CREATE_DATE|ECDR_UPDATE_DATE|ECDR_RECORD_TYP|MERIT_Audit_Key|MERIT_CREATE_DATE|MERIT_UPDATE_DATE|MERIT_RECORD_TYP|LOAD_SUCCESSFUL\n")

    else:
        row = tuple (lin)
        inp_array.append (lin)

    # ================================================================
    # Insert Every 500 Rows
    # ================================================================

    if nrow % 500 == 0:
        cur.executemany (ins_sql, inp_array, batcherrors=True)
        for err in cur.getbatcherrors():
            nerr += 1
            if nerr == 1:
                errfile = open (errfn, 'w', newline='')
                errfile.write ("Audit_Key|ECDR_CREATE_DATE|ECDR_UPDATE_DATE|ECDR_RECORD_TYP|ERROR_CAUSE\n")
            errfile.write (inp_array[err.offset][0]+"|"+inp_array[err.offset][cridx]+"|"+inp_array[err.offset][upidx]+"|"+inp_array[err.offset][tyidx]+"|"+err.message+"\n")
        for lst in inp_array:
            ackfile.write (lst[0]+"|"+lst[cridx]+"|"+lst[upidx]+"|"+lst[tyidx]+"|||||Y\n")
        inp_array = []

# ====================================================================
# Insert Remaining Rows 
# ====================================================================

if nrow % 500 != 0:
    cur.executemany (ins_sql, inp_array, batcherrors=True)
    for lst in inp_array:
        ackfile.write (lst[0]+"|"+lst[cridx]+"|"+lst[upidx]+"|"+lst[tyidx]+"|||||Y\n")
    for err in cur.getbatcherrors():
        nerr += 1
        if nerr == 1:
            errfile = open (errfn, 'w', newline='')
            errfile.write ("Audit_Key|ECDR_CREATE_DATE|ECDR_UPDATE_DATE|ECDR_RECORD_TYP|ERROR_CAUSE\n")
        errfile.write (inp_array[err.offset][0]+"|"+inp_array[err.offset][cridx]+"|"+inp_array[err.offset][upidx]+"|"+inp_array[err.offset][tyidx]+"|"+err.message+"\n")


# ====================================================================
# Output Statistics and Commit/Cleanup
# ====================================================================

dt_end = datetime.datetime.now()
print ("Table:", tbnm)
print ("Number of Row(s) Loaded: " + str (nrow-1-nerr))
print ("Number of Errors:        " + str (nerr))
print ("Date/Time Started:       " + dt_start.strftime ("%m/%d/%Y %H:%M:%S"))
print ("Date/Time Ended:         " + dt_end.strftime ("%m/%d/%Y %H:%M:%S"))
print ("Acknowledgment File:     " + ackfn)
if nerr > 0:
    print ("Error File:              " + errfn)

con.commit ()
cur.close ()
con.close ()
csvfile.close ()
ackfile.close ()
if nerr > 0:
    errfile.close ()

exit()
