#!/usr/bin/python3

class DBConstraint:
    pass

# FIXME:
class DBCandidateKey(DBConstraint):
    def __init__(self, tabschema, tabname, constname, constraintype, enforced, enablequeryopt, comment):
        self._constname_ = constname
        self._tabschema_ = tabschema
        self._tabname_ = tabname
        self._constraintype_ = constraintype
        self._columns_ = []
        self._comment_ = comment

        if enforced == "N":
            self._enforced_ = "NOT ENFORCED"
        elif enforced == "Y":
            self._enforced_ = "ENFORCED"

        if enablequeryopt == "N":
            self._enablequeryopt_ = "DISABLE QUERY OPTIMIZATION"
        elif enablequeryopt == "Y":
            self._enablequeryopt_ = "ENABLE QUERY OPTIMIZATION"

    def add_column(self, col):
        self._columns_.append(col)

    def __str__(self):
        cols = ', '.join(self._columns_)
        s = f"\nALTER TABLE {self._tabschema_}.{self._tabname_} ADD CONSTRAINT {self._constname_}"
        if self._constraintype_ == "U":
            s += f"\n    UNIQUE ({cols})"
        else:
            s += f"\n    PRIMARY KEY ({cols})"
        s += f"\n{self._enforced_}"
        s += f"\n{self._enablequeryopt_} @"

        if self._comment_ is not None:
            s += f"\n\nCOMMENT ON CONSTRAINT {self._tabschema_}.{self._tabname_}.{self._constname_} IS '{self._comment_}' @"

        return s


class DBForeignKey(DBConstraint):
    def __init__(self, tabschema, tabname, constname, refkeyname, reftabschema,
                 reftabname, delete, update, enforced, enablequeryopt, constraintype, comment):
        self._constname_ = constname
        self._tabschema_ = tabschema
        self._tabname_ = tabname
        self._constraintype_ = constraintype
        self._reftabschema_ = reftabschema
        self._reftabname_ = reftabname
        self._comment_ = comment

        if delete == "A":
            self._deleterule_ = "NO ACTION"
        elif delete == "C":
            self._deleterule_ = "CASCADE"
        elif delete == "N":
            self._deleterule_ = "SET NULL"
        else:
            self._deleterule_ = "RESTRICT"

        if update == "A":
            self._updaterule_ = "NO ACTION"
        else:
            self._updaterule_ = "RESTRICT"

        self._cols_ = []
        self._refcols_ = []

        if enforced == "N":
            self._enforced_ = "NOT ENFORCED"
        elif enforced == "Y":
            self._enforced_ = "ENFORCED"

        if enablequeryopt == "N":
            self._enablequeryopt_ = "DISABLE QUERY OPTIMIZATION"
        elif enablequeryopt == "Y":
            self._enablequeryopt_ = "ENABLE QUERY OPTIMIZATION"

    def add_column(self, col):
        self._cols_.append(col)
        
    def add_refcolumn(self, col):
        self._refcols_.append(col)
    
    def __str__(self):
        cols = ', '.join(self._cols_)
        refcols = ', '.join(self._refcols_)
        s = f"\nALTER TABLE {self._tabschema_}.{self._tabname_} ADD CONSTRAINT {self._constname_}"
        s += f"\n    FOREIGN KEY ({cols})"
        s += f"\n    REFERENCES {self._reftabschema_}.{self._reftabname_}" 
        s += f"\n                ({refcols})"
        s += f"\n        ON DELETE {self._deleterule_}"
        s += f"\n        ON UPDATE {self._updaterule_}"
        s += f"\n{self._enforced_}"
        s += f"\n{self._enablequeryopt_} @"
        
        if self._comment_ is not None:
            s += f"\n\nCOMMENT ON CONSTRAINT {self._tabschema_}.{self._tabname_}.{self._constname_} IS '{self._comment_}' @"
        
        return s


class DBCheck(DBConstraint):
    def __init__(self, tabschema, tabname, constname, text, enforced, enablequeryopt, constraintype, comment):
        self._constname_ = constname
        self._tabschema_ = tabschema
        self._tabname_ = tabname
        self._constraintype_ = constraintype
        self._comment_ = comment
        if enforced == "N":
            self._enforced_ = "NOT ENFORCED"
        elif enforced == "Y":
            self._enforced_ = "ENFORCED"

        if enablequeryopt == "N":
            self._enablequeryopt_ = "DISABLE QUERY OPTIMIZATION"
        elif enablequeryopt == "Y":
            self._enablequeryopt_ = "ENABLE QUERY OPTIMIZATION"

        self._text_ = text

    def __str__(self):
        s = f"\nALTER TABLE {self._tabschema_}.{self._tabname_} ADD CONSTRAINT {self._constname_}"
        s += f"\n    CHECK ({self._text_})"
        s += f"\n{self._enforced_}"
        s += f"\n{self._enablequeryopt_} @"

        if self._comment_ is not None:
            s += f"\n\nCOMMENT ON CONSTRAINT {self._tabschema_}.{self._tabname_}.{self._constname_} IS '{self._comment_}' @"

        return s


if __name__ == "__main__":
    c = DBCheck("TEST", "TEST", "C1_TEST", "C0 == 3", "Y", "Y", "C")
    print(c)
    
    c = DBForeignKey("TEST", "CHILD", "FK_PARENT", "PK_PARENT", "TEST", "PARENT",
                     "C", "R", "Y", "Y", "F")
    c.add_column("C1")
    c.add_column("C2")
    c.add_refcolumn("P1")
    c.add_refcolumn("P2")
    print(c)

    c = DBCandidateKey("TEST", "PARENT", "PK_PARENT", "P", "Y", "Y")
    c.add_column("P1")
    c.add_column("P2")
    print(c)
