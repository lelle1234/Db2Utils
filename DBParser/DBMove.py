#!/usr/bin/python3

import ibm_db
import getopt
import sys
import os
from toposort import toposort_flatten

db = None
host = "localhost"
port = "50000"
user = None
pwd = None
outfile = None
targetdb = None

try:
    opts, args = getopt.getopt(sys.argv[1:], "h:d:P:u:p:o:t:")
except getopt.GetoptError:
    sys.exit(-1)
for o, a in opts:
    if o == "-d":
        db = a
    if o == "-h":
        host = a
    if o == "-P":
        port = a
    if o == "-u":
        user = a
    if o == "-p":
        pwd = a
    if o == "-t":
        targetdb = a


if db is None or user is None or pwd is None or targetdb is None:
    print("Usage: DBMove.py [-h <host> -P <port>] -d <db> -u <user> -p <pwd> -t <target>")
    sys.exit(1)

db = db.upper()
targetdb = targetdb.upper()

cfg = (db, host, port, user, pwd)
conn = ibm_db.connect("DATABASE=%s; HOSTNAME=%s; PORT=%s; PROTOCOL=TCPIP; UID=%s; PWD=%s" % cfg, "", "")

get_db_type = "values nya.get_db_type()"

find_edges = """
SELECT rtrim(t.tabschema) || '.' || rtrim(t.tabname)
     , coalesce(rtrim(r.reftabschema) || '.' || rtrim(r.reftabname), 'dummy')
FROM syscat.tables t
LEFT JOIN syscat.references r
    ON (t.tabschema, t.tabname) = (r.tabschema, r.tabname)
WHERE t.tabschema not like 'SYS%' 
  AND t.type = 'T'
  AND rtrim(t.tabschema) not like 'NYA_%' 
  AND t.tabschema <> 'TMP'
ORDER BY 1
"""
  
identity_skip = """
select rtrim(tabschema) || '.' || rtrim(tabname) from syscat.columns
where identity = 'Y' and generated = 'D'
"""

stmt = ibm_db.prepare(conn, get_db_type)
ibm_db.execute(stmt, ())
tpl = ibm_db.fetch_tuple(stmt)
db_type = tpl[0]

edges = dict()
stmt = ibm_db.prepare(conn, find_edges)
ibm_db.execute(stmt, ())
tpl = ibm_db.fetch_tuple(stmt)
while tpl:
    n1, n2 = tpl
    try:
        edges[n1].add(n2)
    except KeyError:
        edges[n1] = set()
        edges[n1].add(n2)

    tpl = ibm_db.fetch_tuple(stmt)
sorted_nodes = list(toposort_flatten(edges))

# print(sorted_nodes)


identity_skip_arr = []
edges = dict()
stmt = ibm_db.prepare(conn, identity_skip)
ibm_db.execute(stmt, ())
tpl = ibm_db.fetch_tuple(stmt)
while tpl:
    identity_skip_arr.append(tpl[0])
    tpl = ibm_db.fetch_tuple(stmt)

# print(identity_skip)
os.makedirs(db, exist_ok=True)
export_file = open("%s/export.sql" % db, "w")
load_file = open("%s/load.sql" % db, "w")
export_file.write("connect to %s;\n" % db)
load_file.write("connect to %s;\n" % targetdb)
if db_type == "N":
    load_file.write("""set integrity for nya.person off;\n""")
    load_file.write("""alter table nya.person 
                            alter column EMAIL_UC drop generated 
                            alter column NORMALIZED_FIRSTNAME drop generated 
                            alter column NORMALIZED_LASTNAME drop generated;\n""")
    load_file.write("""set integrity for nya.person immediate checked;\n""")

for t in sorted_nodes:
    if t == "dummy":
        continue
    export_file.write("export to %s.ixf of ixf lobs to . modified by codepage=819 messages export_%s.msg select * from %s;\n" % (t,t,t))
    identityskip = "identityoverride"
    if t in identity_skip_arr:
        identityskip = " "

    load_file.write("load from %s.ixf of ixf lobs from . modified by generatedoverride %s messages load_%s.msg replace into %s;\n" % (t, identityskip, t, t))

if db_type == "N":
    load_file.write("""set integrity for nya.person off;\n""")
    load_file.write("""alter table nya.person 
                            alter column EMAIL_UC set generated always as ( upper(email)) 
                            alter column NORMALIZED_FIRSTNAME set generated always as ( NYA.REMOVE_DIACRITICS( FIRSTNAME ) ) 
                            alter column NORMALIZED_LASTNAME set generated always as ( NYA.REMOVE_DIACRITICS( LASTNAME ) );\n""")
    load_file.write("""set integrity for nya.person immediate checked force generated;\n""")
    load_file.write("""echo set integrity for all tables;\n""")

export_file.write("connect reset;\n")
load_file.write("connect reset;\n")

export_file.close()
load_file.close()


