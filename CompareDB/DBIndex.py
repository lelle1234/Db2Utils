#!/usr/bin/python3

import ibm_db
import ibm_db_dbi


class DBIndex:
    def __init__(self, tabschema, tabname, indschema, indname, uniquerule, indextype, reverse_scans, pagesplit,
                 collectstatistcs, user_defined, compression, comment, nullkeys, typemodel, datatype, hashed, length, 
                 scale, pattern):
        self._tabschema_ = tabschema
        self._tabname_ = tabname
        self._indschema_ = indschema
        self._indname_ = indname
        self._columns_ = []
        self._include_ = []

        self._uniquerule_ = uniquerule
        self._indextype_ = indextype
        self._reverse_scans_ = reverse_scans
        self._pagesplit_ = pagesplit
        self._collectstatistcs_ = collectstatistcs
        self._user_defined_ = user_defined
        self._compression_ = compression
        self._comment_ = comment
        self._nullkeys_ = nullkeys
        self._typemodel_ = typemodel
        self._datatype_ = datatype 
        self._hashed_ = hashed
        self._length_ = length
        self._scale_ = scale 
        self._pattern_ = pattern

    def add_column(self, colname):
        self._columns_.append(colname)

    def add_include(self, colname):
        self._include_.append(colname)

    def __str__(self):
        ind = f"\nCREATE " 
        if self._uniquerule_ in ("P", "U"):
            ind += "UNIQUE "
        ind += f"INDEX {self._indschema_}.{self._indname_} ON {self._tabschema_}.{self._tabname_}"
        ind += "\n    ("
        ind += f", ".join(self._columns_)
        ind += ")"
        if len(self._include_) > 0:
            ind += "\nINCLUDE (" + f", ".join(self._include_) + ")"

        if self._indextype_ == 'XVIL':
            ind += f"""\nGENERATE KEY USING XMLPATTERN '{self._pattern_}'
              AS SQL {self._datatype_}"""
            
            if self._datatype_ in ['CHARACTER', 'VARCHAR']: #and self._hashed_ == 'N':
                ind += f"({self._length_} OCTETS)"

            if self._typemodel_ == 'R':
                ind += " REJECT INVALID VALUES"
            else:
                ind += " IGNORE INVALID VALUES"

        if self._compression_ == "Y":
            ind += "\nCOMPRESS YES"
        else:
            ind += "\nCOMPRESS NO"

        if self._nullkeys_ == "N":
            ind += "\nEXCLUDE NULL KEYS"

        if self._indextype_ == "CLUS":
            ind += "\nCLUSTER"

        if self._reverse_scans_ == "Y":
            ind += "\nALLOW REVERSE SCANS" 

        if self._collectstatistcs_ == "D":
            ind += "\nCOLLECT DETAILED STATISTICS"
        elif self._collectstatistcs_ == "S":
            ind += "\nCOLLECT SAMPLED DETAILED STATISTICS"
        elif self._collectstatistcs_ == "Y":
            ind += "\nCOLLECT STATISTICS"
        ind += " @"

        if self._comment_ is not None:
            ind += f"\n\nCOMMENT ON INDEX {self._indschema_}.{self._indname_} IS '{self._comment_}' @"

        return ind


if __name__ == "__main__":
    xpk = DBIndex("TEST", "TEST", "TEST", "XPK_TEST", "P", "REG", "Y", "N", "S", "N", "Y")
    xpk.add_column("COL0")
    xpk.add_include("COL1")

    print(xpk)
