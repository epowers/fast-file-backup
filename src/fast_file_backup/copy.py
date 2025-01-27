#!/usr/bin/env python

import argparse
import mimetypes
import os
import pathlib
import re
import shutil
import sqlite3 as sql
import sys

import getdents as gd


def main():
    from fast_file_backup.ingest import append_path_to_data, execute_data

    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=pathlib.Path, metavar='<source path>')
    parser.add_argument('dest', type=pathlib.Path, metavar='<dest path>')
    parser.add_argument('-cp', '--copy', action='store_true')
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('-db', '--database', type=pathlib.Path, default='index.db')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    verbose = args.verbose
    db_path = args.database
    source_dir = str(args.source)
    dest_dir = str(args.dest)
    copy = args.copy
    limit = args.limit

    if not os.path.isfile(db_path):
        parser.error(f"database: {db_path} is not a valid database")
        sys.exit(1)

    con = sql.connect(db_path)
    cur = con.cursor()

    offset = 0
    while limit > 0:
        query = """SELECT path, size FROM DirEnt WHERE type={} AND path LIKE ? ORDER BY size DESC LIMIT 1 OFFSET {}""".format(gd.DT_REG, offset)
        offset += 1
        res = cur.execute(query, ['{}/%'.format(source_dir)])
        row = res.fetchone()
        if row is None:
            break
        source_path, size_ = row

        query = """SELECT path FROM DirEnt WHERE type={} AND size={} AND path != ? AND path LIKE ?""".format(gd.DT_REG, size_)
        res = cur.execute(query, [source_path, '{}/%'.format(dest_dir)])
        matches = set()
        for row in res:
            path, = row
            matches.add(path)
        matches = list(matches)

        # TODO: check matches for same filename/path OR do a hash or something...

        if len(matches) == 0:
            limit -= 1
            dest_path = os.path.join(dest_dir, source_path[len(source_dir)+1:])
            print(size_)
            print(source_path)
            if source_path != dest_path:
                if not os.path.isfile(source_path):
                    print('Skipping... source file is not a regular file: {}'.format(source_path))
                elif os.path.exists(dest_path):
                    print('Skipping... destination exists: {}'.format(dest_path))
                elif copy:
                    print('Copying to: {}'.format(dest_path))
                    shutil.copy2(source_path, dest_path, follow_symlinks=False)
                    data = []
                    append_path_to_data(dest_path, gd.DT_DIR, data)
                    execute_data(con, cur, data)
                else:
                    print('Destination: {}'.format(dest_path))
            print()


if __name__ == '__main__':
    main()
