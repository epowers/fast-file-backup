#!/usr/bin/env python

import argparse
import mimetypes
import os
import pathlib
import re
import sqlite3 as sql
import sys

import getdents as gd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('-rm', '--remove', action='store_true')
    parser.add_argument('-db', '--database', type=pathlib.Path, default='index.db')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    verbose = args.verbose
    db_path = args.database
    remove = args.remove
    limit = args.limit

    if not os.path.isfile(db_path):
        parser.error(f"database: {db_path} is not a valid database")
        sys.exit(1)

    con = sql.connect(db_path)
    cur = con.cursor()

    offset = 0
    while limit > 0:
        query = """SELECT path, size FROM DirEnt WHERE type={} ORDER BY size DESC LIMIT 1 OFFSET {}""".format(gd.DT_REG, offset)
        offset += 1
        res = cur.execute(query)
        row = res.fetchone()
        if row is None:
            break
        path, size_ = row

        query = """SELECT path FROM DirEnt WHERE type={} AND size={} AND path != ?""".format(gd.DT_REG, size_)
        res = cur.execute(query, [path])
        matches = set([path])
        for row in res:
            path, = row
            matches.add(path)
        matches = list(matches)
        matches.sort()

        if len(matches) > 1:
            limit -= 1
            path0 = matches.pop(0)
            print(size_)
            print(path0)
            for path in matches:
                if remove:
                    print("Removing: {}".format(path))
                    os.remove(path)
                    query = """DELETE FROM DirEnt WHERE path=?"""
                    cur.execute(query, [path])
                else:
                    print("Duplicate: {}".format(path))
            print()
            if remove:
                con.commit()


if __name__ == '__main__':
    main()
