#!/usr/bin/python3

import sys
import tempfile
from filecmp import dircmp
import argparse
import difflib
import configparser

import ibm_db

from DB import *

products = { 
        "N": "nya/src/main/resources/db/table/",
        "S": "studera/src/main/resources/db/table/",
        "I": "idp/src/main/resources/db/table/",
        }

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Verify ddl")
    required_args = parser.add_argument_group("Required arguments")
    required_args.add_argument("-H", "--hostname", required=False, default="localhost")
    required_args.add_argument("-d", "--dbname", required=True)
    required_args.add_argument("-P", "--dbport", required=False, default="50000")
    required_args.add_argument("-u", "--username", required=False)
    required_args.add_argument("-p", "--password", required=False)
    required_args.add_argument("-f", "--config", required=False)
    required_args.add_argument("-b", "--basedir", required=True)

    ns = parser.parse_args()

    if ns.config is not None:
        config = configparser.ConfigParser()
        config.read(ns.config)
        username = config['config']['username']
        password = config['config']['password']
    else:
        username, password = ns.username, ns.password

    with tempfile.TemporaryDirectory() as tmpdirname:
        schema = None

        # determine databasetype
        connstr = f"DATABASE={ns.dbname};HOSTNAME={ns.hostname};PORT=50000;PROTOCOL=TCPIP;UID={username}; PWD={password}"
        conn = ibm_db.connect(connstr, "", "")
        s1 = ibm_db.prepare(conn, "values nya.get_db_type()")
        ibm_db.execute(s1, ())
        product_path = products[ibm_db.fetch_tuple(s1)[0]]


        db = DB(ns.hostname, ns.dbname, ns.dbport, username, password, schema, None)
        for t in db.get_all_tables():
            with open(tmpdirname + "/" + t._tabschema_ + "." + t._tabname_ + ".sql", "w") as f:
                f.write(str(t))

        dcmp = dircmp(ns.basedir + product_path, tmpdirname)
        rc = 0
        print()
        if len(dcmp.left_only) > 0:
            rc = -1
            print(f"Only in {product_path}:")
            for l in dcmp.left_only:
                print(l)
       
        print()
        if len(dcmp.right_only) > 0:
            rc = -1
            print(f"Only in {ns.dbname}@{ns.hostname}:")
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
            for line in difflib.unified_diff(f1con, f2con, f"{product_path}", f"{ns.dbname}@{ns.hostname}"):
                print(f"{line.rstrip()}")

    sys.exit(rc)
