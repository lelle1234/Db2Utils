#!/usr/bin/python3

from DBColumn import *
from DBIndex import *
from DBConstraint import *


class DBTable:
    def __init__(self, tabschema, tabname, tbspace, index_tbspace, long_tbspace,
                 append_mode, compression, rowcompmode, tableorg, table_comment):
        self._tabschema_ = tabschema
        self._tabname_ = tabname
        self._tbspace_ = tbspace
        self._index_tbspace_ = index_tbspace
        self._long_tbspace_ = long_tbspace
        self._append_mode_ = append_mode
        if compression == "R":
            self._compress_ = "COMPRESS YES"
            if rowcompmode == "A":
                self._compress_ += " ADAPTIVE"
            else:
                self._compress_ += " STATIC"

        else:
            self._compress_ = "COMPRESS NO"

        # if append_mode == "Y":

        self._tableorg_ = "ORGANIZE BY "
        if tableorg == "R":
            self._tableorg_ += "ROW"
        elif tableorg == "C":
            self._tableorg_ += "COLUMN"

        self._columns_ = []
        self._indexes_ = []
        self._constraints_ = []

        self._table_comment_ = table_comment


    def set_db(self, db):
        self._db_ = db

    def get_db(self):
        return self._db_

    def add_column(self, col):
        self._columns_.append(col)

    def add_index(self, i):
        self._indexes_.append(i)

    def add_constraint(self, c):
        self._constraints_.append(c)

    def get_parents(self):
        return [x._reftabschema_ + "." + x._reftabname_ for x in
                self._constraints_ if x._constraintype_ == "F"]

    def get_children(self):
        return [x._tabschema_ + "." + x._tabname_ for x in
                self.get_db()._tables_ if self._tabschema_ + "." + self._tabname_ in x.get_parents()]

    def __str__(self):
        # if self._type_ == "T":
        res = "--#SET TERMINATOR @\n\n"
        res += f"CREATE TABLE {self._tabschema_}.{self._tabname_}"
        # elif self._type_ == "V":
        #    res = f"CREATE VIEW {self._tabschema_}.{self._tabname_}"
        for c in self._columns_:
            res += "\n" + str(c)
        res += f"\n) IN {self._tbspace_}"
        if self._index_tbspace_ is not None and  self._index_tbspace_ != "":
            res += f" INDEX IN {self._index_tbspace_}"
        if self._long_tbspace_ is not None and self._long_tbspace_ != "":
            res += f" LONG IN {self._long_tbspace_}"
        res += f"\n{self._compress_}"
        res += f"\n{self._tableorg_}"
        res += " @"
#        res += "\n"
        for i in self._indexes_:
            res += "\n" + str(i)

        for c in self._constraints_:
            res += "\n" + str(c)

        if self._table_comment_ is not None:
            res += "\n\n" + f"COMMENT ON TABLE {self._tabschema_}.{self._tabname_} IS '{self._table_comment_}' @"

        res += "\n"
        for c in self._columns_:
            if c._column_comment_ is not None:
                res += "\n" + f"COMMENT ON COLUMN {self._tabschema_}.{self._tabname_}.{c._colname_} IS '{c._column_comment_}' @"

        res += "\n\n"
        return res


if __name__ == "__main__":
    # self, tabschema, tabname, tbspace, index_tbspace, long_tbspace
    # append_mode, compression, rowcompmode, tableorg
    t = DBTable("TEST", "TEST", "TBSPC1", "INXSPC1", "", "", "R", "A", "R")

    # colno, colname, typename, length, scale, nulls, bit_data, identity, generated, text, default
    c1 = DBColumn(0, "COL0", "INTEGER", 43, None, "N", '', "Y", "A", None, None)
    c2 = DBColumn(1, "COL1", "CHARACTER", 4, None, "N", '', "N", "", None, "'BANAN'")
    c3 = DBColumn(1, "COL2", "DECIMAL", 4, 1, "N", '', "N", "", None, "3.1")

    t.add_column(c1)
    t.add_column(c2)
    t.add_column(c3)

    xpk = DBIndex("TEST", "TEST", "TEST", "XPK_TEST", "P", "REG", "Y", "N", "S", "N", "Y")
    xpk.add_column("COL0")
    xpk.add_include("COL1")

    t.add_index(xpk)
    print(t)
