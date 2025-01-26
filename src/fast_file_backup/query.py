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
    parser.add_argument('-p', '--filter-path', type=str)
    parser.add_argument('-t', '--filter-type', type=str)
    parser.add_argument('-db', '--database', type=pathlib.Path, default='index.db')
    parser.add_argument('-q', '--query', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    filter_path = args.filter_path
    filter_type = args.filter_type
    query = args.query
    verbose = args.verbose
    db_path = args.database

    if not os.path.isfile(db_path):
        parser.error(f"database: {db_path} is not a valid database")
        sys.exit(1)

    con = sql.connect(db_path)
    cur = con.cursor()

    if filter_path:
        filter_path = '%{}%'.format(filter_path)
        query = """SELECT path, type, size, blocks FROM DirEnt WHERE path LIKE ? ORDER BY path"""
        resultset = cur.execute(query, [filter_path])
    else:
        query = """SELECT path, type, size, blocks FROM DirEnt ORDER BY path"""
        resultset = cur.execute(query)

    totalsize = 0
    totalblocks = 0
    mimetypes_count = {}
    for row in resultset:
        path, type_, size_, blocks = row
        if type_ == gd.DT_REG:
            mimetype_, _ = mimetypes.guess_type(path)
            mimetype_ = str(mimetype_)
            if filter_type:
                if not re.search(filter_type, mimetype_):
                    continue

            totalsize += size_
            totalblocks += blocks

            if mimetype_ in mimetypes_count:
                mimetypes_count[mimetype_]['count'] += 1
                mimetypes_count[mimetype_]['size'] += size_
            else:
                mimetypes_count[mimetype_] = {
                        'count': 1,
                        'size': size_,
                        }
        elif type_ == gd.DT_DIR:
            pass
        else:
            continue
        print(path)

    print()
    print("Total Size: {:,} bytes".format(totalsize))
    print("Total blocks: {:,} (x 512 = {:,} bytes)".format(totalblocks, totalblocks * 512))
    print("Mime file type count:")
    for mimetype_, item in mimetypes_count.items():
        print("    {}: {:,} ({:,} bytes)".format(mimetype_, item['count'], item['size']))


if __name__ == '__main__':
    main()
