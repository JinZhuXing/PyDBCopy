import argparse
import sys

import mysql.connector
from progressbar import *


# main process
def main(args):
    # get arguments ------------------------------------------------------------
    src_db_addr = args.src_db_addr
    src_db_user = args.src_db_user
    src_db_pass = args.src_db_pass
    src_db_name = args.src_db_name
    src_db_table_name = args.src_db_table_name
    dst_db_addr = args.dst_db_addr
    dst_db_user = args.dst_db_user
    dst_db_pass = args.dst_db_pass
    dst_db_name = args.dst_db_name
    dst_db_table_name = args.dst_db_table_name

    # connect to database ------------------------------------------------------
    src_dbconfig = {
        'host': src_db_addr,
        'user': src_db_user,
        'passwd': src_db_pass,
        'db': src_db_name,
        'auth_plugin': 'mysql_native_password'
    }
    src_connection = mysql.connector.connect(**src_dbconfig)
    src_connection.autocommit = True
    src_cursor = src_connection.cursor()

    dst_dbconfig = {
        'host': dst_db_addr,
        'user': dst_db_user,
        'passwd': dst_db_pass,
        'db': dst_db_name,
        'auth_plugin': 'mysql_native_password'
    }
    dst_connection = mysql.connector.connect(**dst_dbconfig)
    dst_connection.autocommit = False
    dst_cursor = dst_connection.cursor()

    # get current existing count at destination database ----------------------
    dst_cursor.execute('SELECT COUNT(*) FROM ' + dst_db_table_name)
    dst_count = dst_cursor.fetchone()[0]

    # get total count at source database --------------------------------------
    src_cursor.execute('SELECT COUNT(*) FROM ' + src_db_table_name)
    src_count = src_cursor.fetchone()[0]

    # copy data from source database to destination database every 1000 rows --
    # show progressbar
    print('Copying data from source database to destination database...')
    print('Total rows: ' + str(src_count))
    print('Current rows: ' + str(dst_count))

    widgets = ['Progress: ', Percentage(), ' ', Bar(marker='#', left='[', right=']'), ' ', ETA()]
    pbar = ProgressBar(widgets=widgets, maxval=src_count).start()
    proc_count = 0
    for i in range(src_count // 1000 + 1):
        # skip for existing row count
        if ((i + 1) * 1000 < dst_count):
            proc_count += 1000
            pbar.update(proc_count)
            continue
        
        src_cursor.execute('SELECT * FROM ' + src_db_table_name + ' ORDER BY id ASC LIMIT ' + str(i * 1000) + ', 1000')
        for row in src_cursor:
            # skip for existing row count
            if (proc_count < dst_count):
                proc_count += 1
                pbar.update(proc_count)
                continue

            dst_cursor.execute('INSERT INTO ' + dst_db_table_name + ' VALUES (' + ','.join(['%s'] * len(row)) + ')', row)
            proc_count += 1
            pbar.update(proc_count)

        dst_connection.commit()

    pbar.finish()
    print('Done!')
    
    # close connection --------------------------------------------------------
    src_cursor.close()
    src_connection.close()
    dst_cursor.close()
    dst_connection.close()


# argument parser
def parse_arguments(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument('--src_db_addr', type=str,
                        help = 'Source DB Address', default='localhost')
    parser.add_argument('--src_db_user', type=str,
                        help = 'Source DB User Name', default='root')
    parser.add_argument('--src_db_pass', type=str,
                        help = 'Source DB User Password', default='password')
    parser.add_argument('--src_db_name', type=str,
                        help = 'Source DB Name', default='database')
    parser.add_argument('--src_db_table_name', type=str,
                        help = 'Source DB Table Name', default='src_table')
    
    parser.add_argument('--dst_db_addr', type=str,
                        help = 'Destination DB Address', default='localhost')
    parser.add_argument('--dst_db_user', type=str,
                        help = 'Destination DB User Name', default='root')
    parser.add_argument('--dst_db_pass', type=str,
                        help = 'Destination DB User Password', default='password')
    parser.add_argument('--dst_db_name', type=str,
                        help = 'Destination DB Name', default='database')
    parser.add_argument('--dst_db_table_name', type=str,
                        help = 'Destination DB Table Name', default='dst_table')

    return (parser.parse_args(argv))


# start point
if __name__ == '__main__':
    main(parse_arguments(sys.argv[1:]))
