#!/usr/bin/python3

import sys
import getopt
import os
import socket

p = os.path.dirname(os.path.abspath(sys.argv[0])) 
import ibm_db

fk_sql = """
select rtrim(x.tabschema)
     , rtrim(x.tabname)
     , rtrim(x.constname)
     , rtrim(x.fk_colnames)
     , rtrim(x.reftabschema)
     , rtrim(x.reftabname)
     , rtrim(x.pk_colnames)
     , case x.deleterule WHEN 'C' then 'CASCADE' 
                         WHEN 'R' then 'RESTRICT' 
                         ELSE 'NO ACTION' 
       end
     , case x.updaterule WHEN 'C' then 'CASCADE'  
                         WHEN 'R' then 'RESTRICT'  
                         ELSE 'NO ACTION' 
       end
     , enforced
     , enablequeryopt
from syscat.references x
join syscat.tabconst y
    using (tabschema, tabname, constname)
where x.reftabname = ? 
  and x.reftabschema = ? 
with ur"""

create_str = """
ALTER TABLE %s.%s ADD CONSTRAINT %s
    FOREIGN KEY (%s)
    REFERENCES %s.%s (%s)
        ON UPDATE %s
        ON DELETE %s 
%sENFORCED
%sQUERY OPTIMIZATION @
"""

drop_str = "ALTER TABLE %s.%s DROP CONSTRAINT %s @"

def main():

    dbname = None
    tables = None
    schema = None
    user   = None
    pwd    = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:t:s:u:p:")
    except getopt.GetoptError:
        sys.exit(-1)
    for o, a in opts:
        if (o == "-d"):
            dbname = a
        if (o == "-t"):
            tables = a.split(',')
        if (o == "-s"):
            schema = a
        if (o == "-u"):
            user = a
        if (o == "-p"):
            pwd = a

    if dbname == None or tables == None or schema == None or user == None or pwd == None:
        print("Usage: print_recreate_fk.py -d <db> -u <usr> -p <pwd> -s <schema> -t t1,t2,...tn")
        sys.exit(1)

    ip = socket.gethostbyname(socket.gethostname())
    cfg = (dbname, ip, user, pwd)
    conn = ibm_db.connect("DATABASE=%s;HOSTNAME=%s;PORT=50000;PROTOCOL=TCPIP;UID=%s; PWD=%s" % cfg,"","")


    create_stmts = []
    drop_stmts = []

    s1 = ibm_db.prepare(conn, fk_sql)
    for t in tables:
        ibm_db.execute(s1, (t,schema))
        restore_sql = ''
        row = ibm_db.fetch_tuple(s1)
        while row != False:
            tabscema    = row[0]
            tabname     = row[1]
            constname   = row[2]
            fk_colnames = row[3]
            reftabschema= row[4]
            reftabname  = row[5]
            pk_colnames = row[6]
            deleterule  = row[7]
            updaterule  = row[8]
            enforced    = row[9]
            if enforced == 'N':
                enforced = 'NOT '
            else:
                enforced = ''
            queryopt    = row[10]
            if queryopt == 'N':
                queryopt = 'DISABLE '
            else:
                queryopt = 'ENABLE '
            fk = filter ((lambda x:x!=''), fk_colnames.split(' '))
            cols = ''
            for c in fk: 
                cols = cols + ',' + c
            fkcols = cols[1:]

            pk = filter ((lambda x:x!=''), pk_colnames.split(' '))
            cols = ''
            for c in pk: 
                cols = cols + ',' + c
            pkcols = cols[1:]

            create = create_str % (tabscema,tabname,constname,fkcols,reftabschema
                                  ,reftabname,pkcols,updaterule,deleterule,enforced,queryopt)
        
            drop = drop_str % (tabscema,tabname,constname)

            create_stmts.append(create)
            drop_stmts.append(drop)

            row = ibm_db.fetch_tuple(s1)

    ibm_db.rollback(conn)
    for x in drop_stmts:
        print(x)

    for x in create_stmts:
        print(x)


if __name__ == "__main__":
    main()



