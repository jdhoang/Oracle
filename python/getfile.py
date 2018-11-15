# ==============================================================================
# Name: getfile.py
#
# Description:
# Obtain file stored at BLOB in Oracle and write to filesystem.
#
# ============================================================================
# Revision History:
# Date       By        Comment
# ---------- --------- -----------------------------------------------------
# 10/16/2018 jhoang    Original release.
#
# ==============================================================================

import os
import getpass
import cx_Oracle

ouser  = raw_input("Oracle User: ")
opwd   = getpass.getpass(prompt="Oracle Password: ")
ienv   = raw_input("Env (dev[d],test[t],acpt[a],research[r]): ")

env = ienv[0].lower()
sid = {
    'd': 'dlv-fpe-d001:1521/DOFPE100',
    't': 'tlv-fpe-d002:1521/TOFPE100',
    'a': 'alv-fpe-d001:1521/AOFPE100',
    'r': 'alv-fpe-d002:1521/AOFPE001'
}

fil_id = input ("\nFile ID: ")

# ===========================================
# Query File Name
# ===========================================

con_str = ouser + "/" + opwd + "@" + sid[env]
con = cx_Oracle.connect(con_str)
cur = con.cursor()

sql = "select fa.file_id, fa.file_actl_nme from appl_file_asmp fa where fa.file_id = " + str (fil_id)
cur.execute(sql)

for fnm in cur:
    print fnm[1]
    fil_nm = fnm[1]

# ===========================================
# Read BLOB and write to filesystem
# ===========================================

blob_sql = "select file_blob_img from file_dtl where file_id = " + str (fil_id)
cur.execute (blob_sql)

for lrow in cur:
    lob = lrow[0].read ()
    fil = open (fil_nm, 'wb')
    fil.write (lob)
    fil.close ()

cur.close()
con.close()

