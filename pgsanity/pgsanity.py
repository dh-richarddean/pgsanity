#!/usr/bin/env python

from __future__ import print_function
from __future__ import absolute_import
import argparse
import sys

from pgsanity import sqlprep
from pgsanity import ecpg

def get_config(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='Check syntax of SQL for PostgreSQL')
    parser.add_argument('files', nargs='*', default=None)
    return parser.parse_args(argv)

def check_file(filename=None, show_filename=False):
    #either work with sys.stdin or open the file
    filelike = sys.stdin
    if filename is not None:
        filelike = open(filename, "r")

    #prep the sql, store it in a temp file
    sql_string = filelike.read()
    success, msg = check_string(sql_string)

    #report results
    result = 0
    if not success:
        #possibly show the filename with the error message
        prefix = ""
        if show_filename and filename is not None:
            prefix = filename + ": "
        print(prefix + msg)
        result = 1

    return result

def check_string(sql_string):
    prepped_sql = sqlprep.prepare_sql(sql_string)
    success, msg = ecpg.check_syntax(prepped_sql)
    return success, msg

def check_files(files):
    if files is None or len(files) == 0:
        return check_file()
    else:
        #show filenames if > 1 file was passed as a parameter
        show_filenames = (len(files) > 1)

        accumulator = 0
        for filename in files:
            accumulator |= check_file(filename, show_filenames)
        return accumulator

def main():
    config = get_config()
    return check_files(config.files)

if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
