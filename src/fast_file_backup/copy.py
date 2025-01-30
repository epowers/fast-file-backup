#!/usr/bin/env python

import argparse
import mimetypes
import os
import pathlib
import re
import shutil
import sqlite3 as sql
import sys
from traceback import print_exc

import getdents as gd


def main():
    from fast_file_backup.ingest import append_path_to_data, execute_data

    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=pathlib.Path, metavar='<source path>')
    parser.add_argument('dest', type=pathlib.Path, metavar='<dest path>')
    parser.add_argument('-cp', '--copy', action='store_true')
    parser.add_argument('--dedup', action='store_true')
    parser.add_argument('--no-db-update', action='store_true')
    parser.add_argument('--overwrite', action='store_true')
    parser.add_argument('-t', '--filter-type', type=str)
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('-db', '--database', type=pathlib.Path, default='index.db')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    source_dir = str(args.source)
    dest_dir = str(args.dest)
    copy = args.copy
    dedup = args.dedup
    no_db_update = args.no_db_update
    overwrite = args.overwrite
    filter_type = args.filter_type
    limit = args.limit
    db_path = args.database
    verbose = args.verbose

    if not os.path.isfile(db_path):
        parser.error(f"database: {db_path} is not a valid database")
        sys.exit(1)

    con = sql.connect(db_path)
    cur = con.cursor()

    offset = 0
    while limit != 0:
        # get next path
        query = """SELECT path, size FROM DirEnt WHERE type={} AND path LIKE ? ORDER BY size DESC LIMIT 1 OFFSET {}""".format(gd.DT_REG, offset)
        offset += 1

        res = cur.execute(query, ['{}/%'.format(source_dir)])
        row = res.fetchone()
        if row is None:
            break
        source_path, size_ = row

        if filter_type:
            mimetype_, _ = mimetypes.guess_type(source_path)
            mimetype_ = str(mimetype_)
            if not re.search(filter_type, mimetype_):
                continue

        if dedup:
            # check for duplicates in source dir
            query = """SELECT path FROM DirEnt WHERE type={} AND size={} AND path LIKE ?""".format(gd.DT_REG, size_)
            res = cur.execute(query, ['{}/%'.format(source_dir)])
            matches = set()
            for row in res:
                path, = row
                matches.add(path)
            matches = list(matches)
            matches.sort()
            source_path = matches[0]

        # check same source/dest path
        dest_path = os.path.join(dest_dir, source_path[len(source_dir)+1:])
        if source_path == dest_path:
            continue

        # check if source is a file (not a link, device, directory, etc.)
        if not os.path.isfile(source_path):
            print('Skipping... source file is not a regular file: {}'.format(source_path))
            print()
            continue
        # check if destination exists.
        # TODO: check size of destination as well...
        elif os.path.exists(dest_path) and not overwrite:
            # print('Skipping... destination exists: {}'.format(dest_path))
            continue

        # check for existing copy in dest dir
        matches = set()
        query = """SELECT path FROM DirEnt WHERE type={} AND size={} AND path != ? AND path LIKE ?""".format(gd.DT_REG, size_)
        res = cur.execute(query, [source_path, '{}/%'.format(dest_dir)])
        for row in res:
            path, = row
            # TODO: check matches for same filename/path OR do a hash or something...
            matches.add(path)
        matches = list(matches)
        if len(matches) != 0 and not overwrite:
            continue

        # copy file
        print(size_)
        print(source_path)
        if copy:
            print('Copying to: {}'.format(dest_path))
            dest_parent = os.path.dirname(dest_path)
            if not os.path.exists(dest_parent):
                os.makedirs(dest_parent)
            try:
                shutil.copyfile(source_path, dest_path, follow_symlinks=False)
            except OSError:
                print_exc()
                print()
                continue

            if not no_db_update:
                data = []
                append_path_to_data(dest_path, gd.DT_DIR, data)
                execute_data(con, cur, data)
        else:
            print('Destination: {}'.format(dest_path))
        limit -= 1
        print()


if __name__ == '__main__':
    main()
