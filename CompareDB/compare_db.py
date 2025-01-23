#!/usr/bin/python3

import sys
import os
import tempfile
from filecmp import dircmp
import argparse
import difflib
import configparser

import ibm_db

from DB import *

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Compare databases")
    required_args = parser.add_argument_group("Required arguments")
    required_args.add_argument("-H", "--hostname", required=False, default="localhost")
    required_args.add_argument("--db1", required=True)
    required_args.add_argument("--db2", required=True)
    required_args.add_argument("-P", "--dbport", required=False, default="50000")
    required_args.add_argument("-u", "--username", required=False)
    required_args.add_argument("-p", "--password", required=False)
    required_args.add_argument("-f", "--config", required=False)
    required_args.add_argument("-s", "--schema", required=True)

    ns = parser.parse_args()

    if ns.config is not None:
        config = configparser.ConfigParser()
        config.read(ns.config)
        username = config['config']['username']
        password = config['config']['password']
    else:
        username, password = ns.username, ns.password

    with tempfile.TemporaryDirectory() as tmpdirname:
        connstr1 = f"DATABASE={ns.db1};HOSTNAME={ns.hostname};PORT=50000;PROTOCOL=TCPIP;UID={username}; PWD={password}"
        connstr2 = f"DATABASE={ns.db2};HOSTNAME={ns.hostname};PORT=50000;PROTOCOL=TCPIP;UID={username}; PWD={password}"
        conn1 = ibm_db.connect(connstr1, "", "")
        conn2 = ibm_db.connect(connstr2, "", "")

        db1 = DB(ns.hostname, ns.db1, ns.dbport, username, password, ns.schema, None)
        p1 = tmpdirname + "/" + ns.db1
        os.mkdir(p1)
        for t in db1.get_all_tables():
            with open(p1 + "/" + t._tabschema_ + "." + t._tabname_ + ".sql", "w") as f:
                f.write(str(t))

        db2 = DB(ns.hostname, ns.db2, ns.dbport, username, password, ns.schema, None)
        p2 = tmpdirname + "/" + ns.db2
        os.mkdir(p2)
        for t in db2.get_all_tables():
            with open(p2 + "/" + t._tabschema_ + "." + t._tabname_ + ".sql", "w") as f:
                f.write(str(t))

        dcmp = dircmp(p1, p2)
        rc = 0
        print()
        if len(dcmp.left_only) > 0:
            rc = -1
            print(f"Only in {p1}:")
            for l in dcmp.left_only:
                print(l)
       
        print()
        if len(dcmp.right_only) > 0:
            rc = -1
            print(f"Only in {p2}:")
            for r in dcmp.right_only:
                print(r)

        print()
        for d in dcmp.diff_files:
            rc = -1
            with open(dcmp.left+'/'+d) as f1:
                f1con = f1.readlines()
            
            with open(dcmp.right+'/'+d) as f2:
                f2con = f2.readlines()

            print(f"Difference in {d}:")
            for line in difflib.unified_diff(f1con, f2con, f"{ns.db1}", f"{ns.db2}"):
                print(f"{line.rstrip()}")

    sys.exit(rc)
