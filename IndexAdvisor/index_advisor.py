#!/usr/bin/python3

import sys
import getopt
from itertools import chain, combinations
from functools import reduce

# pip3 install tabulate --user
from tabulate import tabulate
# pip3 install ibm_db --user
import ibm_db


# init
db = user = pwd = infile = None
host = "localhost"
port = 50000
schema = "db2inst1"


def fix_query(q):
    q = q.strip()
    if q[-1] == ';':
        return q[:-1]
    return q


# set of all sub-sets
def power_set(iterable):
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))


CREATE_PROC = """
CREATE OR REPLACE PROCEDURE EXEC_IN_EXPLAIN_MODE(stmt varchar(32672), mode char(1))
BEGIN
        DECLARE m varchar(25);
        SET m = CASE mode WHEN 'R' THEN ' recommend indexes '
                          WHEN 'E' THEN ' evaluate indexes '
                          ELSE ' explain '
                END;

        EXECUTE IMMEDIATE 'set current explain mode' || m;
        EXECUTE IMMEDIATE stmt;
        EXECUTE IMMEDIATE 'set current explain mode no';

END 
"""

# table to keep track of cost for combination of indexes
DDL_COST_TABLE = """
create table costtbl 
(nr int not null primary key
,indexes varchar(4096) not null
,total_cost decimal(20,4) not null
)"""

CLEAN_COST_TABLE = "delete from costtbl"

GET_INDEX_NAMES = """
select distinct rtrim(name)
from (
    select name, rank() over (order by explain_time desc) as rnk
    from advise_index
    where exists = 'N'
)
where rnk = 1
"""

SAVE_QUERY_COST = """
insert into costtbl (nr, indexes, total_cost)
select ?, ?, dec(total_cost,20,4) as before_total_cost
from Explain_Operator a
where a.operator_type = 'RETURN'
order by a.explain_time desc
fetch first 1 rows only
with ur
"""

ENABLE_INDEX = "update advise_index set use_index = 'Y' where name = ?"
DISABLE_INDEX = "update advise_index set use_index = 'N' where name = ?"

REPORT_COST = """
select row_number() over (order by length(indexes)) as nr
     , substr(indexes, 2, ?) as index_combination
     , int(total_cost)
     , case when max_cost = total_cost 
            then '' 
            else decimal(100.0*(max_cost - total_cost) / max_cost,5,2) || '%'
       end as improvement 
from (
    select indexes, total_cost
         , (select total_cost from costtbl where indexes = '') as max_cost
         , row_number() over (partition by length(indexes)-length(replace(indexes,',','')) 
                              order by total_cost) as rn 
    from costtbl 
) 
where rn = 1 
order by length(indexes)-length(replace(indexes,',','')), total_cost desc"""

DISPLAY_INDEXES = "select distinct cast(creation_text as varchar(2000)) from advise_index where name = ?"

ORIGINAL_COST = "select total_cost from costtbl where indexes = ''"

minnr = 0
maxnr = 100
try:
    opts, args = getopt.getopt(sys.argv[1:], "d:u:p:i:h:P:s:m:n:")
except getopt.GetoptError:
    sys.exit(-1)
for o, a in opts:
    if o == "-d":
        db = a
    if o == "-u":
        user = a
    if o == "-p":
        pwd = a
    if o == "-i":
        infile = a
    if o == "-h":
        host = a
    if o == "-P":
        port = a
    if o == "-s":
        schema = a
    if o == "-m":
        minnr = int(a)
    if o == "-n":
        maxnr = int(a)

# check
if None in [db, user, pwd, infile]:
    print("Usage: ./index_evaluator.py -d <db> -u <user> -p <pwd> -i <infile> [-h <host> -P <port> -s <schema>]")
    sys.exit(1)

cfg = (db, host, port, user, pwd)
conn = ibm_db.connect("DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s; PWD=%s" % cfg, "", "")

# read query from file
query = ""
for line in open(infile, "r"):
    query += " " + line

# create procedure
ibm_db.exec_immediate(conn, CREATE_PROC)
ibm_db.exec_immediate(conn, "delete from advise_index")

# try to create costtbl
try:
    ibm_db.exec_immediate(conn, DDL_COST_TABLE)
except:
    # assume it exists
    pass

ibm_db.exec_immediate(conn, CLEAN_COST_TABLE)
print("Begin recomending indexes")
ibm_db.callproc(conn, 'EXEC_IN_EXPLAIN_MODE', (fix_query(query), "R"))
print("End recomending indexes")


indexes = []
indexes_sql_stmt = ibm_db.exec_immediate(conn, GET_INDEX_NAMES)
tpl = ibm_db.fetch_tuple(indexes_sql_stmt)
while tpl:
    indexes.append(tpl[0])
    tpl = ibm_db.fetch_tuple(indexes_sql_stmt)

indset = set(indexes)
print("number of suggested indexes %d" % len(indset))
print(indset)
ps = list(power_set(indset))
n = 0
print("Evaluating %d index combinations" % (len(ps)))

save_cost = ibm_db.prepare(conn, SAVE_QUERY_COST)
enable_index = ibm_db.prepare(conn, ENABLE_INDEX)
disable_index = ibm_db.prepare(conn, DISABLE_INDEX)
p: []
for p in ps:
    n += 1
    sp = set(p)
    if len(sp) < minnr or len(sp) > maxnr:
        continue

    print("evaluating combination %d" % n)

    # enable all in p
    for i in p:
        ibm_db.execute(enable_index, (i,))

    # disable all not in p
    for i in list(indset - sp):
        ibm_db.execute(disable_index, (i,))

    # evaluate cost for current set of indexes
    ibm_db.callproc(conn, 'EXEC_IN_EXPLAIN_MODE', (fix_query(query), "E"))

    # save cost
    inds = reduce((lambda x, y: x+","+y), p, "")
    ibm_db.execute(save_cost, (n, inds))

# number of chars needed to display all indexes
max_len = sum(map((lambda x: len(x)), indexes)) + len(indexes)

# result, lowest cost for 1,2,...n indexes from set of 2^n index combinations
a = []
stmt = ibm_db.prepare(conn, REPORT_COST)
ibm_db.execute(stmt, (max_len,))
tpl = ibm_db.fetch_tuple(stmt)
a.append(tpl)
while tpl:
    tpl = ibm_db.fetch_tuple(stmt)
    a.append(tpl)

print()
h = ['#', 'Index names', 'Cost', 'Improvement']
print(tabulate(a[:-1], headers=h))
print()

question = input("Output Index Combination? (N, 2,3,...)\n")

if question == "N" or question == "1":
    print("Bye")
    sys.exit(1)
else:
    choice = a[int(question) - 1][1]
    indexes = choice.split(",")
    stmt = ibm_db.prepare(conn, DISPLAY_INDEXES)
    print()
    for i in indexes:
        ibm_db.execute(stmt, (i,))
        tpl = ibm_db.fetch_tuple(stmt)[0]
        result = tpl.replace('"', '').replace(' ASC', '')
        print(result)
        print()

ibm_db.commit(conn)
