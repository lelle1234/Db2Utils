#!/usr/bin/python3

import ibm_db
import ibm_db_dbi
import sys

from DBTable import *
from DBColumn import *
from DBIndex import *
from DBConstraint import *
#from DBView import *

import argparse


class DB:
    def __init__(self, hostname, dbname, port, username, password, schemaname=None, tablename=None):
        self._tables_ = list()
        self._tabledict_ = dict()
        self._schemaname_ = schemaname
        self._tablename_ = tablename
        self._views_ = list() 
        self._triggers_ = list()
       
        cfg = (dbname, hostname, port, username, password)
        ibm_db_conn = ibm_db.connect("DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s" % cfg, "", "")
        conn = ibm_db_dbi.Connection(ibm_db_conn)
        c1 = conn.cursor()
        self._read_tables_(c1)
#        self._read_views_(c1)
        self._read_indexes_(c1)
        self._read_constraints_(c1)
#        self._read_triggers_(c1)

    def _read_triggers_(self, c1):
        pass

    def _read_views_(self, c1):
        sql = f"""select viewschema, viewname, text 
                  from syscat.views"""          
        if self._schemaname_ is None:
            sql += f"\n where viewschema in (select schemaname from nya.validation_schemas)"
        else:
            sql += f"\n where viewschema = {self._schemaname_}"

        if self._tablename_ is not None:
            sql += f"\n and viewname = '{self._tablename_}'"

        sql += f"\norder by viewschema, viewname"



        c1.execute(sql)
        for row in c1.fetchall():
            tabschema, tabname, text = row
            v = DBView(tabschema, tabname, text)
            self._views_.append(v)

    def _read_tables_(self, c1):
        # FIXME: read all table types
        sql = f"""select rtrim(t.tabschema) tabschema, rtrim(t.tabname) tabname, rtrim(c.colname) colname
                 , colno, rtrim(c.typename) typename
                 , c.length / case when coalesce(c.typestringunits,'') = 'CODEUNITS32'
                                    and typename <> 'BLOB' 
                                then 4
                                else 1
                              end as length
                 , c.scale, c.nulls, case when c.codepage = '0' and c.typename like '%CHAR%'
                                          then ' FOR BIT DATA' else '' end as bit_data
                 ,  c.identity, c.generated, c.text
                 ,  rtrim(t.tbspace), rtrim(t.index_tbspace), rtrim(t.long_tbspace), t.append_mode
                 , rtrim(c.default), compression, rowcompmode, tableorg, t.remarks as table_comment
                 , c.remarks as column_comment
                 , c.inline_length
                from syscat.columns c
                join syscat.tables t
                    using (tabschema, tabname)
                where type = 'T'"""
        if self._schemaname_ is None:
            sql += f"\n and t.tabschema in (select schemaname from nya.validation_schemas)"
        else:
            sql += f"\n and t.tabschema = '{self._schemaname_}'"

        if self._tablename_ is not None:
            sql += f"\n and t.tabname = '{self._tablename_}'"

        sql += " order by t.tabschema, t.tabname, c.colno"

        c1.execute(sql)
        t = None
        for row in c1.fetchall():
            tabschema, tabname, colname, colno, typename, length, scale, nulls, bit_data, identity, \
                generated, text, tbspace, index_tbspace, long_tbspace, append_mode, default, compression, \
                rowcompmode, tableorg, table_comment, column_comment, inline_length = row

            if colno == 0:
                # new table
                if t is not None:
                    # store previous table
                    self._tables_.append(t)
                    self._tabledict_[t._tabschema_, t._tabname_] = t
                t = DBTable(tabschema, tabname, tbspace, index_tbspace, long_tbspace,
                            append_mode, compression, rowcompmode, tableorg, table_comment)
                t.set_db(self)

            # add columns
            c = DBColumn(colno, colname, typename, length, scale, nulls, bit_data,
                         identity, generated, text, default, column_comment, inline_length)
            t.add_column(c)
        else:
            if t is not None:
                # add last table
                self._tabledict_[t._tabschema_, t._tabname_] = t
                self._tables_.append(t)

    def _read_indexes_(self, c1):
        sql = f"""select rtrim(i.tabschema), rtrim(i.tabname), rtrim(i.indschema), rtrim(i.indname)
                , i.uniquerule, i.indextype, i.reverse_scans, i.pagesplit, i.collectstatistcs
                , i.user_defined, i.compression
                , COALESCE(ic.text, ic.colname), ic.colorder, ic.colseq, i.remarks, i.nullkeys
                , ix.typemodel, rtrim(ix.datatype), ix.hashed, ix.length, ix.scale, ix.pattern
                from syscat.indexes i
                join syscat.indexcoluse ic
                    using (indschema, indname)
                join syscat.tables t
                    using (tabschema, tabname)
                left join syscat.indexxmlpatterns ix
                    using (indschema, indname)
                where t.type = 'T'
                  and i.indextype in ('CLUS','REG','XVIL')""" 
        if self._schemaname_ is None:
            sql += f"\n and t.tabschema in (select schemaname from nya.validation_schemas)"
        else:
            sql += f"\n and t.tabschema = '{self._schemaname_}'"

        if self._tablename_ is not None:
            sql += f"\n and t.tabname = '{self._tablename_}'"

        sql += """\n order by rtrim(i.tabschema), rtrim(i.tabname)
                    , case i.uniquerule when 'P' then -999 when 'U' then -100 + iid else iid end
                    , ic.colseq"""
        c1.execute(sql)
        i = None
        for row in c1.fetchall():
            tabschema, tabname, indschema, indname, uniquerule, indextype, reverse_scans, pagesplit, \
             collectstatistcs, user_defined, compression, colname, colorder, colseq, comment, nullkeys, \
             typemodel, datatype, hashed, length, scale, pattern = row

            if colseq == 1:
                # new index
                t = self._tabledict_[(tabschema, tabname,)]
                i = DBIndex(tabschema, tabname, indschema, indname, uniquerule, indextype, reverse_scans,
                            pagesplit, collectstatistcs, user_defined, compression, comment, nullkeys,
                            typemodel, datatype, hashed, length, scale, pattern)
                if colorder == "A":
                    i.add_column(colname)
                elif colorder == "D":
                    i.add_column(colname + " DESC")
                t.add_index(i)

            else:
                # add columns / include
                # col = ""
                # inc = ""
                if colorder == "A":
                    i.add_column(colname)
                elif colorder == "D":
                    i.add_column(colname + " DESC")
                else:
                    i.add_include(colname)

    def _read_candidate_keys_(self, c1):
        sql = """select rtrim(t.tabschema), rtrim(t.tabname), rtrim(t.constname), rtrim(k.colname) \
                    , t.type, t.enforced, t.enablequeryopt, k.colseq, t.remarks 
                 from syscat.keycoluse k 
                 join syscat.tabconst t
                    using (tabschema, tabname, constname)
                 join syscat.tables x
                    using (tabschema, tabname)
                 where x.type = 'T' and t.type in ('P','U')"""

        if self._schemaname_ is None:
            sql += f"\n and x.tabschema in (select schemaname from nya.validation_schemas)"
        else:
            sql += f"\n and x.tabschema = '{self._schemaname_}'"
        
        if self._tablename_ is not None:
            sql += f"\n and x.tabname = '{self._tablename_}'"

        sql += "\n order by t.tabschema, t.tabname, t.type, t.constname, k.colseq"

        t = None
        c = None
        c1.execute(sql)
        for row in c1.fetchall():
            tabschema, tabname, constname, colname, consttype, enforced, enablequeryopt, colseq, comment = row
            if colseq == 1:
                if t is not None:
                    t.add_constraint(c)

                t = self.get_table(tabschema, tabname)
                c = DBCandidateKey(tabschema, tabname, constname, consttype, enforced, enablequeryopt, comment)
            c.add_column(colname)
        else:
            if t is not None:
                t.add_constraint(c)

    def _read_foreign_keys_(self, c1):
        sql = """select rtrim(r.tabschema), rtrim(r.tabname), rtrim(r.constname)
                      , rtrim(r.refkeyname), rtrim(r.reftabschema), rtrim(r.reftabname)
                      , r.deleterule, r.updaterule, rtrim(k1.colname), rtrim(k2.colname)
                      , enforced, enablequeryopt, k1.colseq, c.type, c.remarks
                from syscat.tabconst c
                join syscat.references r
                    using (tabschema, tabname, constname)
                join syscat.keycoluse k1 
                    on (r.tabschema, r.tabname, r.constname)
                     = (k1.tabschema, k1.tabname, k1.constname)
                join syscat.keycoluse k2 
                    on (r.reftabschema, r.reftabname, r.refkeyname)
                     = (k2.tabschema, k2.tabname, k2.constname)
                    and k1.colseq = k2.colseq"""

        if self._schemaname_ is None:
            sql += f"\n where r.tabschema in (select schemaname from nya.validation_schemas)"
        else:
            sql += f"\n where r.tabschema = '{self._schemaname_}'"

        if self._tablename_ is not None:
            sql += f"\n and r.tabname = '{self._tablename_}'"

        sql += "order by r.tabschema, r.tabname, r.constname, k1.colseq"

        t = None
        f = None
        c1.execute(sql)
        for row in c1.fetchall():
            tabschema = row[0]
            tabname = row[1]
            constname = row[2]
            refkeyname = row[3]
            reftabschema = row[4]
            reftabname = row[5]
            deleterule = row[6]
            updaterule = row[7]
            enforced = row[10]
            enablequeryopt = row[11]
            colseq = row[12]
            consttype = row[13]
            comment = row[14]

            if colseq == 1:
                # next constraint, add prev
                if t is not None:
                    t.add_constraint(f)
                
                t = self.get_table(tabschema, tabname)
                f = DBForeignKey(tabschema, tabname, constname, refkeyname, reftabschema, reftabname,
                                 deleterule, updaterule, enforced, enablequeryopt, consttype, comment)
            f.add_column(row[8])
            f.add_refcolumn(row[9])
        else:
            if t is not None:
                t.add_constraint(f)

    def _read_check_constraints_(self, c1):
        sql = """select rtrim(c.tabschema), rtrim(c.tabname), rtrim(c.constname)
                      , rtrim(c.type), ltrim(rtrim(c.text)), t.enforced, t.enablequeryopt, t.type, t.remarks
                from syscat.checks c 
                join syscat.tabconst t 
                    using (constname, tabschema, tabname)"""
        if self._schemaname_ is None:
            sql += f"\n where t.tabschema in (select schemaname from nya.validation_schemas)"
        else:
            sql += f"\n where t.tabschema = '{self._schemaname_}'"
        
        if self._tablename_ is not None:
            sql += f"\n and t.tabname = '{self._tablename_}'"

        sql += "\n order by c.tabschema, c.tabname, c.constname"

        c1.execute(sql)
        for row in c1.fetchall():
            tabschema, tabname, constname, consttype, text, enforced, enablequeryopt, typex, comment = row
            if consttype == "S":
                # System-generated check constraint for a GENERATED ALWAYS column
                continue

            t = self.get_table(tabschema, tabname)
            c = DBCheck(tabschema, tabname, constname, text, enforced, enablequeryopt, typex, comment)
            t.add_constraint(c)

    def _read_constraints_(self, c0):
        self._read_candidate_keys_(c0)
        self._read_foreign_keys_(c0)
        self._read_check_constraints_(c0)

    def __str__(self):
        s = '\n\n'.join([(str(x)) for x in self._tables_])
        # s += '\n\n'.join([(str(x)) for x in self._views_])
        return s

    def get_table(self, tabschema, tabname):
        return self._tabledict_[tabschema, tabname]

    def get_all_tables(self):
        return [x for x in self._tables_]

    def get_tables(self, tabschema):
        return [x for x in self._tables_ if x._tabschema_ == tabschema]

    def get_schemas(self):
        return list(set([x._tabschema_ for x in self._tables_]))

if __name__ == "__main__":
    # self, tabschema, tabname, tbspace, index_tbspace, long_tbspace
    # append_mode, compression, rowcompmode, tableorg
    # t = DBTable("TEST", "TEST", "TBSPC1", "INXSPC1", "", "", "R", "A", "R")

    # colno, colname, typename, length, scale, nulls, bit_data, identity, generated, text, default
    # c1 = DBColumn(0, "COL0", "INTEGER", 43, None, "N", '', "Y", "A", None, None)
    # c2 = DBColumn(1, "COL1", "CHARACTER", 4, None, "N", '', "N", "", None, "'BANAN'")
    # c3 = DBColumn(1, "COL2", "DECIMAL", 4, 1, "N", '', "N", "", None, "3.1")

    # t.add_column(c1)
    # t.add_column(c2)
    # t.add_column(c3)

    # print(t)
    # print(c1)
    # print(c2)
    # print(c3)

    #print(d.get_schemas())
    #sys.exit(1)

    # x = str(d)
    # print(x)

#    tt = d._tables_
#    for t in tt:
#        print(t._tabschema_, t._tabname_, "<=", t.get_parents())
#        #break

#    for t in tt:
#        print(t._tabschema_, t._tabname_, "=>", t.get_children())
#        #break

    # tt = d.get_table("STUDENT", "UPPEHALLSTILLSTAND_LOG")
    # print(tt) # ._display_())

    parser = argparse.ArgumentParser(description="Create database version")
    required_args = parser.add_argument_group("Required arguments")
    required_args.add_argument("-H", "--hostname", required=False, default="localhost")
    required_args.add_argument("-d", "--dbname", required=True)
    required_args.add_argument("-P", "--dbport", required=False, default="50000")
    required_args.add_argument("-t", "--table", required=False, default=None)
    required_args.add_argument("-s", "--schema", required=False)
    required_args.add_argument("-u", "--username", required=True)
    required_args.add_argument("-p", "--password", required=True)
    required_args.add_argument("-D", "--dumpdir", required=False)
#    required_args.add_argument("-V", "--validationdir", required=True)

    ns = parser.parse_args()

    db = DB(ns.hostname, ns.dbname, ns.dbport, ns.username, ns.password, ns.schema, ns.table)
    if ns.dumpdir is None:
        print(db)
        sys.exit(0)

    for t in db.get_all_tables():
        with open(ns.dumpdir + "/" + t._tabschema_ + "." + t._tabname_ + ".sql", "w") as f:
            f.write(str(t))

    sys.exit(0)
