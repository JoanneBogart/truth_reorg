import sqlite3
import numpy as np

def connect_read(path):
    '''
    Not obvious how to connect read-only to SQLite db. Package it up here
    '''
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    return conn

def assemble_create_table(table_name, columns):
    '''
    Return string which will create table with supplied names
    and column specifications (a tuple (col_name, col_type) )
    '''
    stmt = 'CREATE TABLE ' + table_name + '('
    col_specs = [f'{c[0]} {c[1]}' for c in columns]
    stmt += ','.join(col_specs) + ')'
    return stmt

_SN_OBJECT_TYPE = 22
_MAX_STAR_ID = 41021613038

def make_sn_int_id(host):
    '''
    Parameters
    ----------
    host     int        id of host galaxy

    When host is a real galaxy, new id will be
    host * 1024 + (object-type-id), which is probably 22
    Otherwise assign int id to be host_id + CONSTANT
    where CONSTANT is large enough that all int ids are larger
    than MAX_STAR_ID.   Least host id is 0.

    '''
    OFFSET = _MAX_STAR_ID + 1

    if host < 100000:
        new_id = host + OFFSET
    else:
        new_id = host * 1024 + _SN_OBJECT_TYPE

    return new_id
