#!/usr/bin/python3

from toposort import toposort_flatten
import ibm_db
import sys
import getopt
import os
import re
from DBQueries import DBQueries

"""Parser that tries to retrive db object in topological order"""


class DBNode:
    def __init__(self, oschema, oname, ospecificname, remarks):
        self._oschema_ = oschema
        self._oname_ = oname
        self._specificname_ = ospecificname
        self._val_ = (self._oschema_, self._oname_, self._specificname_)
        self._remarks_ = remarks

    def __eq__(self, other):
        return self._val_ == other.val


class DBGraph:
    def __init__(self, the_conn):
        self._conn_ = the_conn
        self._edges_ = dict()
        self._tables_ = dict()
        self._routines_ = dict()
        self._triggers_ = dict()
        self._indexes_ = dict()
        self._constraints_ = dict()

        self._read_tables_()
        self._read_indexes_()
        self._read_routines_()
        self._read_constraints_()
        self._read_triggers_()
        self._ordered_graph_ = self._read_edges_()

    def __str__(self):
        res = ""
        for node in g._ordered_graph_:
            node_type = node[0]

            if node_type == "DUMMIE":
                # virtual root node, ignore
                continue

            if node_type == "T":
                tabschema, tabname = node[2], node[3]
                try:
                    x = g._tables_[(tabschema, tabname,)]
                    res += str(x)
                    res += x.get_remarks()
                except KeyError:
                    print("TABLE_ERROR")
                    print(node)
                    # sys.exit(1)
                continue

            if node_type == "I":
                try:
                    x = g._indexes_[(node[2], node[3],)]
                    res += str(x)
                    res += x.get_remarks()
                except KeyError:
                    print("INDEX ERROR:")
                    print(node)
                    sys.exit(1)
                continue

            if node_type == "F":
                try:
                    # FIXME: x = g._routines_[node[1:4]]
                    x = g._routines_[(node[1], node[2], node[3],)]
                    res += str(x)
                    res += x.get_remarks()
                except KeyError:
                    print("ERROR:")
                    print(node)
                    # sys.exit(1)
                continue

            if node_type == "C":
                constname, tabschema, tabname = node[1], node[2], node[3]
                try:
                    x = g._constraints_[(constname, tabschema, tabname,)]
                    res += str(x)
                    res += x.get_remarks()
                except KeyError:
                    print("CONSTRAINT ERROR:")
                    print(node)
                    # sys.exit(1)
                continue

            if node_type == "X":
                trigschema, trigname = node[2], node[3]
                x = g._triggers_[(trigschema, trigname,)]
                res += str(x)
                res += x.get_remarks()
                continue

            # Fall through
            print("UNMATCHED ERROR:")
            print(node)
            sys.exit(1)

        # restart sequences for identity columns
        for k in self._tables_:
            t = self._tables_[k]
            if not isinstance(t, DBTable):
                continue
            c = t.get_identity_column()
            if c:
                schema, table, column = c.get_id()
                sql = f"select coalesce(max({column}), 0)+1 from {schema}.{table}"
                stmt = ibm_db.prepare(self._conn_, sql)
                ibm_db.execute(stmt, ())
                nextval = ibm_db.fetch_tuple(stmt)[0]
                res += f"\n ALTER TABLE {schema}.{table} ALTER COLUMN {column} RESTART WITH {nextval} @\n"

        return res

    def _read_edges_(self):
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_edges)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        last_node = None
        while tpl:
            # FIXME: n1 = tuple( x.strip() for x in tpl[0:4] )
            n1 = tuple(map((lambda x: x.strip()), (tpl[0], tpl[1], tpl[2], tpl[3])))
            n2 = tuple(map((lambda x: x.strip()), (tpl[4], tpl[5], tpl[6], tpl[7])))
            if last_node == n1:
                self._edges_[n1].add(n2)
            else:
                try:
                    self._edges_[n1].add(n2)
                except KeyError:
                    self._edges_[n1] = set()
                    self._edges_[n1].add(n2)
                last_node = n1
            tpl = ibm_db.fetch_tuple(stmt)
        return list(toposort_flatten(self._edges_))

    def _read_tables_(self):
        # tables
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_tables)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            tmp = DBTable(*tpl)
            self._tables_[(tpl[0], tpl[1])] = tmp
            tpl = ibm_db.fetch_tuple(stmt)
        self._read_columns_()

        # views
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_views)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            tabschema, tabname, query_opt, text, vtype, remarks = tpl
            text = os.linesep.join([s for s in text.splitlines() if s])
            tmp = DBView(tabschema, tabname, query_opt, text, vtype, remarks)
            self._tables_[(tpl[0], tpl[1])] = tmp
            tpl = ibm_db.fetch_tuple(stmt)

    def _read_columns_(self):
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_tab_columns)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            c = DBColumn(*tpl)
            try:
                tmp = self._tables_[(tpl[0], tpl[1],)]
                tmp.add_column(c)
            except KeyError:
                pass

            tpl = ibm_db.fetch_tuple(stmt)

    def _read_indexes_(self):
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_indexes)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            i = DBIndex(*tpl)
            self._indexes_[(tpl[2], tpl[3])] = i
            tpl = ibm_db.fetch_tuple(stmt)
        self._read_index_columns()

    def _read_index_columns(self):
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_index_columns)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            indschema, indname, colname, colorder = tpl
            i = self._indexes_[(indschema, indname,)]
            i.add_column(colname, colorder)
            tpl = ibm_db.fetch_tuple(stmt)

    def _read_constraints_(self):
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_checks)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            constname, tabschema, tabname, enforced, trusted, enablequeryopt, text, remarks = tpl
            text = os.linesep.join([s for s in text.splitlines() if s])
            c = DBConstraintCheck(constname, tabschema, tabname, enforced, trusted, enablequeryopt, text, remarks)
            self._constraints_[(constname, tabschema, tabname,)] = c
            tpl = ibm_db.fetch_tuple(stmt)

        stmt = ibm_db.prepare(self._conn_, DBQueries.read_candidate_keys)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            constname, tabschema, tabname, ctype, enforced, trusted, enablequeryopt, colnames, remarks = tpl
            c = DBConstraintCandidate(constname, tabschema, tabname, enforced, trusted, enablequeryopt, ctype,
                                      colnames, remarks)
            self._constraints_[(constname, tabschema, tabname,)] = c
            tpl = ibm_db.fetch_tuple(stmt)

        stmt = ibm_db.prepare(self._conn_, DBQueries.read_foreign_keys)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            constname, tabschema, tabname, ctype, enforced, trusted, enablequeryopt, \
                deleterule, updaterule,  reftabschema, reftabname, colnames, refcolnames, remarks = tpl
            c = DBConstraintForeign(constname, tabschema, tabname, enforced, trusted, enablequeryopt,
                                    deleterule, updaterule,  reftabschema, reftabname, colnames, refcolnames, remarks)
            self._constraints_[(constname, tabschema, tabname,)] = c
            tpl = ibm_db.fetch_tuple(stmt)

    def _read_triggers_(self):
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_triggers)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            tabschema, tabname, trigschema, trigname, text, remarks = tpl
            text = os.linesep.join([s for s in text.splitlines() if s])
            t = DBTrigger(tabschema, tabname, trigschema, trigname, text, remarks)
            self._triggers_[(trigschema, trigname,)] = t
            tpl = ibm_db.fetch_tuple(stmt)

    def _read_routines_(self):
        # SQL bodied routines
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_routines)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            specificname, routineschema, routinename, text, rtype, remarks = tpl
            text = os.linesep.join([s for s in text.splitlines() if s])
            r = DBRoutine(specificname, routineschema, routinename, text, rtype, remarks)
            self._routines_[(tpl[0], tpl[1], tpl[2])] = r
            tpl = ibm_db.fetch_tuple(stmt)

        # external routines
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_external_routines)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            specificname, routineschema, routinename, routinetype, typename, length, scale,\
                language, parameter_style, deterministic, external_action, fenced, threadsafe,\
                implementation, remarks, codepage, sql_data_access = tpl
            r = DBRoutineExternal(specificname, routineschema, routinename, routinetype, typename, length, scale,
                                  language, parameter_style, deterministic, external_action, fenced, threadsafe,
                                  implementation, remarks, codepage, sql_data_access)
            self._routines_[(tpl[0], tpl[1], tpl[2])] = r
            tpl = ibm_db.fetch_tuple(stmt)
        self._read_external_routine_params_()

    def _read_external_routine_params_(self):
        stmt = ibm_db.prepare(self._conn_, DBQueries.read_external_routine_parms)
        ibm_db.execute(stmt, ())
        tpl = ibm_db.fetch_tuple(stmt)
        while tpl:
            specificname, routineschema, routinename, parmname, typename, length, scale, codepage = tpl
            p = DBExternalRoutineParameter(*tpl)
            try:
                tmp = self._routines_[(specificname, routineschema, routinename,)]
                tmp.add_params(p)
            except KeyError:
                pass
            tpl = ibm_db.fetch_tuple(stmt)


class DBEdge:
    def __init__(self, node1):
        self.node1 = node1
        self.deps = set()

    def add_dependency(self, node):
        self.deps.add(node)


class DBTable(DBNode):
    def __init__(self, schema, oname, compression, tablespace, indexspace, longspace,
                 organization, volatile, append, remarks):
        super(DBTable, self).__init__(schema, oname, 'N/A', remarks)
        self._compression_ = compression
        self._tablespace_ = tablespace
        self._indexspace_ = indexspace
        self._longspace_ = longspace
        self._organization_ = organization
        self._volatile_ = volatile
        self._append_ = append
        self._columns_ = []

    def add_column(self, col):
        self._columns_.append(col)

    def get_identity_column(self):
        for c in self._columns_:
            if c.is_identity():
                return c
        return None

    def get_remarks(self):
        if self._remarks_:
            s = ""
            rlen = len(self._remarks_) + len([s for s in self._remarks_ if ord(s) > 127])
            if rlen >= 255:
                s += "echo "

            s += f"COMMENT ON TABLE {self._oschema_}.{self._oname_} IS '{self._remarks_}' @\n\n"
        else:
            s = ''
        for c in self._columns_:
            s += c.get_remarks()
        s += "\n"
        return s

    def __str__(self):
        s = f"CREATE TABLE {self._oschema_}.{self._oname_}\n"
        s += f"("
        s += " " + str(self._columns_[0]) + "\n"
        for n in range(1, len(self._columns_)):
            s += ", " + str(self._columns_[n]) + "\n"

        s += ") "
        s += f"IN {self._tablespace_}"
        if self._indexspace_:
            s += f" INDEX IN {self._indexspace_}"
        if self._longspace_:
            s += f" LONG IN {self._longspace_}"
        s += "\n"

        if self._compression_:
            s += f"COMPRESS YES ADAPTIVE \n"
        s += "ORGANIZE BY "
        if self._organization_ == "R":
            s += "ROW"
        else:
            s += "COLUMN"
        s += " @\n\n"

        return s


class DBView(DBNode):
    def __init__(self, schema, oname, optmization, text, viewtype, remarks):
        super(DBView, self).__init__(schema, oname, 'N/A', remarks)
        self._optimization_ = optmization
        self._text_ = text
        self._type_ = viewtype

    def __str__(self):
        s = self._text_ + " @\n\n"
        if self._optimization_ == "Y":
            s += f"ALTER VIEW {self._oschema_}.{self._oname_} ENABLE QUERY OPTIMIZATION @\n\n"

        if self._type_ == "S":
            s += f"REFRESH TABLE {self._oschema_}.{self._oname_} @\n\n"

        s += "\n"
        return s

    def get_remarks(self):
        if self._remarks_:
            s = ""
            rlen = len(self._remarks_) + len([s for s in self._remarks_ if ord(s) > 127])
            if rlen >= 255:
                s += "echo "

            s += f"COMMENT ON TABLE {self._oschema_}.{self._oname_} IS '{self._remarks_}' @\n\n"
        else:
            s = ''
        s += "\n"
        return s


class DBColumn:
    def __init__(self, schema, name, colname, typename, length, scale, nulls, default, generated,
                 identity, text, compress, inline_length, remarks):
        self._oschema_ = schema
        self._oname_ = name
        self._colname_ = colname
        self._typename_ = typename
        self._length_ = length
        self._scale_ = scale
        self._nulls_ = nulls
        self._default_ = default
        self._generated_ = generated
        self._identity_ = identity
        self._text_ = text
        self._compress_ = compress
        self._inline_length_ = inline_length
        self._remarks_ = remarks

    def is_identity(self):
        return self._identity_ == "Y"

    def get_id(self):
        return self._oschema_, self._oname_, self._colname_

    def __str__(self):
        s = f"{self._colname_} {self._typename_}"
        if self._typename_ in ("CHARACTER", "VARCHAR"):
            s += f"({self._length_})"
            if self._typename_ == 'DECIMAL':
                s += f"({self._length_}, {self._scale_})"

        if self._nulls_ == 'N':
            s += " NOT NULL"

        if self._generated_ == "D":
            s += " GENERATED BY DEFAULT "
        elif self._generated_ == "A":
            s += " GENERATED ALWAYS "

        if self._identity_ == "Y":
            s += f"AS IDENTITY"
        elif self._generated_ in ("D", "A"):
            s += self._text_

        return s

    def get_remarks(self):
        if self._remarks_:
            s = ""
            rlen = len(self._remarks_) + len([s for s in self._remarks_ if ord(s) > 127])
            if rlen >= 255:
                s += "echo "

            s += f"COMMENT ON COLUMN {self._oschema_}.{self._oname_}.{self._colname_} IS '{self._remarks_}' @\n"
            return s
        return ''


class DBIndex(DBNode):
    def __init__(self, schema, table, indschema, indname, unique, itype, pctfree,
                 reverse_scans, compression, nullkeys, remarks, typemodel, datatype, length, scale, pattern):
        super(DBIndex, self).__init__(indschema, indname, 'N/A', remarks)
        self._tabschema_ = schema
        self._tabname_ = table
        self._unique_ = unique
        self._itype_ = itype
        self._pctfree_ = pctfree
        self._reverse_scans_ = reverse_scans
        self._compression_ = compression
        self._nullkeys_ = nullkeys
        self._index_columns_ = []
        self._index_columns_include_ = []
        self._typemodel_ = typemodel
        self._datatype_ = datatype
        self._length_ = length
        self._scale_ = scale
        self._pattern_ = pattern

    def add_column(self, colname, colorder):
        if colorder == "I":
            self._index_columns_include_.append(colname)
        else:
            self._index_columns_.append((colname, colorder,))

    def get_remarks(self):
        if self._remarks_:
            s = ""
            rlen = len(self._remarks_) + len([s for s in self._remarks_ if ord(s) > 127])
            if rlen >= 255:
                s += "echo "
            s += f"COMMENT ON INDEX {self._oschema_}.{self._oname_} IS '{self._remarks_}' @\n\n"
            return s
        return ''

    def __str__(self):
        s = "CREATE "
        if self._unique_ in ("P", "U"):
            s += "UNIQUE "
        s += f"INDEX {self._oschema_}.{self._oname_} ON {self._tabschema_}.{self._tabname_}\n"
        x = self._index_columns_[0]
        s += " (" + x[0]
        if x[1] == "D":
            s += " DESC"
        for n in range(1, len(self._index_columns_)):
            x = self._index_columns_[n]
            s += ", " + x[0]
            if x[1] == "D":
                s += " DESC"
        s += ") \n"
        if len(self._index_columns_include_) > 0:
            s += f"INCLUDE ({self._index_columns_include_[0]}"
            for n in range(1, len(self._index_columns_include_)):
                s += ", " + str(self._index_columns_include_[n])
            s += ") \n"
        if self._itype_ == "XVIL":
            s += f"GENERATE KEY USING XMLPATTERN '{self._pattern_}'\n"
            s += f"  AS SQL {self._datatype_}"
            if self._datatype_ in ('CHARACTER', 'VARCHAR'):
                s += f"({self._length_} OCTETS) "
            if self._datatype_ == 'DECIMAL':
                s += f"({self._length_}, {self._scale_}) "
            if self._typemodel_ == "Q":
                s += "IGNORE INVALID VALUES\n"
            else:
                s += "REJECT INVALID VALUES\n"

        if self._compression_ == "Y":
            s += "COMPRESS YES\n"
        if self._itype_ == "CLUS":
            s += "CLUSTER \n"
        if self._nullkeys_ == 'N':
            s += "EXCLUDE NULL KEYS\n"
        # always allow reverse scans
        s += "ALLOW REVERSE SCANS\n"
        s += "COLLECT SAMPLED DETAILED STATISTICS @\n\n"
        return s


class DBRoutine(DBNode):
    def __init__(self, ospecificname, schema, oname, text, rtype, remarks):
        super(DBRoutine, self).__init__(schema, oname, ospecificname, remarks)
        self._text_ = text
        self._type_ = rtype

    def __str__(self):
        if re.search("specific", self._text_, re.IGNORECASE):
            return self._text_ + " @\n\n"

        # add specific after args
        res = ""
        n = 0
        start = 0
        for c in self._text_:
            res += c
            if c == "(" and start >= 0:
                start = 1
                n += 1

            if c == ")" and start >= 0:
                n -= 1

            if start == 1 and n == 0:
                res += f"\n SPECIFIC {self._specificname_} \n"
                start = -1

        return res + " @\n\n"

    def add_params(self, p):
        # dummy
        pass

    def get_remarks(self):
        if not self._remarks_:
            return ''

        s = ""
        rlen = len(self._remarks_) + len([s for s in self._remarks_ if ord(s) > 127])
        if rlen >= 255:
            s += "echo "

        if self._type_ == "F":
            s += f"COMMENT ON SPECIFIC FUNCTION "
        else:
            s += f"COMMENT ON SPECIFIC PROCEDURE "
        s += f"{self._oschema_}.{self._specificname_} IS '{self._remarks_}' @\n\n"
        return s


class DBExternalRoutineParameter:
    def __init__(self, specificname, rschema, rname, pname, typename, length, scale, codepage):
        self._specificname_ = specificname
        self._rschema_ = rschema
        self._rname_ = rname
        self._pname_ = pname
        self._typename_ = typename
        self._length_ = length
        self._scale_ = scale
        self._codepage_ = codepage

    def __str__(self):
        s = ""
        if self._pname_:
            s += f"{self._pname_}"
        s = f" {self._typename_}"
        if self._typename_ in ("CHARACTER", "VARCHAR", "DECIMAL"):
            s += f"({self._length_}"
            if self._typename_ == 'DECIMAL':
                s += f",{self._scale_}"
            s += ")"
            if self._codepage_ == 0 and self._typename_ in ("CHARACTER", "VARCHAR"):
                s += " FOR BIT DATA"
        return s


class DBRoutineExternal(DBNode):
    def __init__(self, ospecificname, oschema, oname, rtype, returns, length, scale,
                 language, parameter_style, deterministic, external_action, fenced, threadsafe, implementation,
                 remarks, codepage, sql_data_access):
        super(DBRoutineExternal, self).__init__(oschema, oname, ospecificname, remarks)
        self._rtype_ = rtype
        self._returns_ = returns
        self._length_ = length
        self._scale_ = scale
        self._language_ = language
        self._parameter_style = parameter_style
        self._deterministic_ = deterministic
        self._external_action_ = external_action
        self._fenced_ = fenced
        self._threadsafe_ = threadsafe
        self._implementation_ = implementation
        self._codepage_ = codepage
        self._sql_data_access_ = sql_data_access
        self._params_ = []

    def add_params(self, param):
        self._params_.append(param)

    def __str__(self):
        if self._rtype_ == "F":
            s = "CREATE OR REPLACE FUNCTION "
        else:
            s = "CREATE OR REPLACE PROCEDURE "
        s += f"{self._oschema_}.{self._oname_} (\n"
        for p in self._params_:
            s += str(p)
            if p != self._params_[-1]:
                s += ", "

        s += ")\n"
        s += f"RETURNS {self._returns_}"
        if self._returns_ in ("CHARACTER", "VARCHAR"):
            s += f"({self._length_})"
        if self._returns_ == 'DECIMAL':
            s += f"({self._length_},{self._scale_})"

        if self._codepage_ == 0 and self._returns_ in ("CHARACTER", "VARCHAR"):
            s += " FOR BIT DATA"
        s += "\n"
        s += f"SPECIFIC {self._specificname_}\n"
        s += f"EXTERNAL NAME '{self._implementation_}'\n"
        s += f"LANGUAGE {self._language_}\n"
        s += f"PARAMETER STYLE {self._parameter_style}\n"

        if self._fenced_ == "Y":
            s += "FENCED "
        else:
            s += "NOT FENCED "

        if self._threadsafe_ == "Y":
            s += "THREADSAFE "
        else:
            s += "NOT THREADSAFE "
        s += "\n"

        if self._deterministic_ == "Y":
            s += "DETERMINISTIC "
        else:
            s += "NOT DETERMINISTIC "

        s += "\n"
        if self._sql_data_access_ == "N":
            s += "NO SQL"
        elif self._sql_data_access_ == "M":
            s += "MODIFIES SQL DATA"
        elif self._sql_data_access_ == "R":
            s += "CONTAINS SQL"
        elif self._sql_data_access_ == "C":
            s += "CONTAINS SQL"

        s += "\n"
        if self._external_action_ == "N":
            s += "NO EXTERNAL ACTION"
        else:
            s += "EXTERNAL ACTION"

        s += "\n @\n\n"

        return s

    def get_remarks(self):
        # return ''
        if not self._remarks_:
            return ''

        s = ""
        rlen = len(self._remarks_) + len([s for s in self._remarks_ if ord(s) > 127])
        if rlen >= 255:
            s += "echo "

        if self._rtype_ == "F":
            s += f"COMMENT ON SPECIFIC FUNCTION "
        else:
            s += f"COMMENT ON SPECIFIC PROCEDURE "
        s += f"{self._oschema_}.{self._specificname_} IS '{self._remarks_}' @\n\n"
        return s


class DBConstraint(DBNode):
    def __init__(self, constname, schema, oname, enforced, trusted, optimization, remarks):
        super(DBConstraint, self).__init__(schema, oname, constname, remarks)
        self._enforced_ = enforced
        self._trusted_ = trusted
        self._optimization_ = optimization

    def get_remarks(self):
        if not self._remarks_:
            return ''

        s = ""
        rlen = len(self._remarks_) + len([s for s in self._remarks_ if ord(s) > 127])
        if rlen >= 255:
            s += "echo "

        s += f"COMMENT ON CONSTRAINT {self._oschema_}.{self._oname_}.{self._specificname_} IS '{self._remarks_}' @\n\n"
        return s


class DBConstraintCheck(DBConstraint):
    def __init__(self, constname, schema, oname, enforced, trusted, optimization, text, remarks):
        super(DBConstraintCheck, self).__init__(constname, schema, oname, enforced, trusted, optimization, remarks)
        self._text_ = text

    def __str__(self):
        s = f"ALTER TABLE {self._oschema_}.{self._oname_} ADD CONSTRAINT {self._specificname_}\n"
        s += f"     CHECK ( {self._text_} )\n"
        if self._enforced_ == "N":
            s += "NOT "
        s += "ENFORCED\n"
        if self._enforced_ == "N" and self._trusted_ == "Y":
            s += "TRUSTED\n"
        if self._optimization_ == "Y":
            s += "ENABLE"
        else:
            s += "DISABLE"
        s += " QUERY OPTIMIZATION @\n\n"
        return s


class DBConstraintCandidate(DBConstraint):
    def __init__(self, constname, schema, oname, enforced, trusted, optimization, ctype, colnames, remarks):
        super(DBConstraintCandidate, self).__init__(constname, schema, oname, enforced, trusted, optimization, remarks)
        self._type_ = ctype
        self._colnames_ = colnames

    def __str__(self):
        # if self._oname_ == ""
        s = f"ALTER TABLE {self._oschema_}.{self._oname_} ADD CONSTRAINT {self._specificname_}\n"
        if self._type_ == 'U':
            s += "  UNIQUE "
        else:
            s += "  PRIMARY KEY "
        s += f"( {self._colnames_} )\n"
        if self._enforced_ == "N":
            s += "NOT "
        s += "ENFORCED\n"
        if self._enforced_ == "N" and self._trusted_ == "Y":
            s += "TRUSTED\n"
        if self._optimization_ == "Y":
            s += "ENABLE"
        else:
            s += "DISABLE"
        s += " QUERY OPTIMIZATION @\n\n"
        return s


class DBConstraintForeign(DBConstraint):
    def __init__(self, constname, schema, oname, enforced, trusted, optimization, delete, update,
                 reftabschema, reftabname, colnames, refcolnames, remarks):
        super(DBConstraintForeign, self).__init__(constname, schema, oname, enforced, trusted, optimization, remarks)
        self._delete_ = delete
        self._update_ = update
        self._reftabschema = reftabschema
        self._reftabname_ = reftabname
        self._colnames_ = colnames
        self._refcolnames_ = refcolnames

    def __str__(self):
        s = f"ALTER TABLE {self._oschema_}.{self._oname_} ADD CONSTRAINT {self._specificname_}\n" \
            f"  FOREIGN KEY ({self._colnames_})\n" \
            f"  REFERENCES {self._reftabschema}.{self._reftabname_}\n" \
            f"              ({self._refcolnames_})\n" \
            f"    ON UPDATE "
        if self._update_ == "A":
            s += "NO ACTION\n"
        else:
            s += "RESTRICT\n"
        s += "    ON DELETE "
        if self._delete_ == "A":
            s += "NO ACTION \n"
        elif self._delete_ == "C":
            s += "CASCADE \n"
        elif self._delete_ == "N":
            s += "SET NULL \n"
        else:
            s += "RESTRICT \n"

        if self._enforced_ == "N":
            s += "NOT "
        s += "ENFORCED\n"
        if self._enforced_ == "N" and self._trusted_ == "Y":
            s += "TRUSTED\n"
        if self._optimization_ == "Y":
            s += "ENABLE"
        else:
            s += "DISABLE"
        s += " QUERY OPTIMIZATION @\n\n"
        return s


class DBTrigger(DBNode):
    def __init__(self, schema, table, trigschema, trigname, text, remarks):
        super(DBTrigger, self).__init__(trigschema, trigname, 'N/A', remarks)
        self._tabschema_ = schema
        self._tabname_ = table
        self._text_ = text

    def __str__(self):
        s = self._text_ + " @\n\n"
        return s

    def get_remarks(self):
        if not self._remarks_:
            return ''

        s = ""
        rlen = len(self._remarks_) + len([s for s in self._remarks_ if ord(s) > 127])
        if rlen >= 255:
            s += "echo "

        s += f"COMMENT ON TRIGGER {self._oschema_}.{self._oname_} IS '{self._remarks_}' @\n\n"
        return s


db = None
host = "localhost"
port = "50000"
user = None
pwd = None
outfile = None

try:
    opts, args = getopt.getopt(sys.argv[1:], "h:d:P:u:p:o:")
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
    if o == "-o":
        outfile = a

cfg = (db, host, port, user, pwd)
conn = ibm_db.connect("DATABASE=%s; HOSTNAME=%s; PORT=%s; PROTOCOL=TCPIP; UID=%s; PWD=%s" % cfg, "", "")

g = DBGraph(conn)
if not outfile:
    print(g)
    sys.exit(0)

with open(outfile, mode="w") as f:
    print(g, file=f)
sys.exit(0)
