#!/usr/bin/env python

import argparse
import os
import pathlib
import sqlite3 as sql
import sys

#import psutil
import getdents as gd

def arg_isdir(arg):
    path = os.path.abspath(arg)
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(f"directory: {arg} is not a valid directory")

def append_path_to_data(path, type_, data):
    try:
        stat = os.lstat(path)
    except FileNotFoundError as e:
        return
    data.append((
        path,
        type_,
        stat.st_ino, # inode
        stat.st_mode,
        stat.st_dev,
        stat.st_nlink,
        stat.st_uid,
        stat.st_gid,
        stat.st_size,
        stat.st_atime,
        stat.st_mtime,
        stat.st_ctime,
        stat.st_atime_ns,
        stat.st_mtime_ns,
        stat.st_ctime_ns,
        stat.st_blocks,
        stat.st_rdev,
        #stat.st_flags,
        ))

DIRENT_TABLE_SCHEMA = (
    ('id', 'INTEGER PRIMARY KEY'),
    ('path', 'TEXT NOT NULL UNIQUE'),
    ('type', 'INTEGER'),
    ('inode', 'INTEGER'),
    ('mode', 'INTEGER'),
    ('dev', 'INTEGER'),
    ('nlink', 'INTEGER'),
    ('uid', 'INTEGER'),
    ('gid', 'INTEGER'),
    ('size', 'INTEGER'),
    ('atime', 'INTEGER'),
    ('mtime', 'INTEGER'),
    ('ctime', 'INTEGER'),
    ('atime_ns', 'INTEGER'),
    ('mtime_ns', 'INTEGER'),
    ('ctime_ns', 'INTEGER'),
    ('blocks', 'INTEGER'),
    ('rdev', 'INTEGER'),
    #('flags', 'INTEGER'),
    )

def execute_data(con, cur, data):
    cur.executemany("""INSERT OR IGNORE INTO DirEnt (path) VALUES (?)""",
            [row[:1] for row in data])
    for row in data:
        cur.executemany("""UPDATE DirEnt SET {} WHERE path=?""".format(
            ",".join(["{}=?".format(e[0]) for e in DIRENT_TABLE_SCHEMA[2:]])),
            [row[1:] + row[:1]])
    con.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', nargs='*', type=arg_isdir)
    parser.add_argument('-db', '--database', type=pathlib.Path, default='index.db')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    verbose = args.verbose
    paths = args.directory
    db_path = args.database
    BUFFER_SIZE = 2**24 # max(2**24, psutil.virtual_memory().free // 8)

    if not paths:
        paths = [os.getcwd()]

    con = sql.connect(db_path)
    cur = con.cursor()

    # create table
    cur.execute("""CREATE TABLE IF NOT EXISTS DirEnt ({})""".format(
        ", ".join(" ".join(e) for e in DIRENT_TABLE_SCHEMA)))
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS DirEnt_path_idx ON DirEnt (path)""")
    cur.execute("""CREATE INDEX IF NOT EXISTS DirEnt_size_idx ON DirEnt (size)""")
    con.commit()

    # add directory paths from command line
    data = []
    for path in paths:
        if verbose:
            print(f"Adding path: {path}")
        append_path_to_data(path, gd.DT_DIR, data)
    execute_data(con, cur, data)

    # recursively search paths updating the database
    current_id = -1
    while True:
        res = cur.execute("""SELECT id, path, type FROM DirEnt WHERE id > {} ORDER BY id LIMIT 1""".format(current_id))
        row = res.fetchone()
        if row is None:
            break
        current_id, path, type_ = row

        data = []
        append_path_to_data(path, type_, data)

        if type_ == gd.DT_DIR:
            for inode, type_, name in gd.getdents(path, BUFFER_SIZE):
                name = os.path.join(path, name)
                if verbose:
                    print(f"Adding path: {name}")
                append_path_to_data(name, type_, data)

        execute_data(con, cur, data)

if __name__ == '__main__':
    main()
