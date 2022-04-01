import os
import sqlite3

from desc.truth_reorg.truth_reorg_utils import assemble_create_table, connect_read

'''
Inputs
Old sn variability sqlite file
New truth sn summary file
Path to file to be created

Outputs
New variability file which
   - includes id_int column from truth sn summary
   - excludes rows for objects which don't appear in new truth sn sumamry
'''

_SN_DIR = os.path.join(os.getenv('SCRATCH'), 'desc/truth/sn')
_SUMM_FILE = os.path.join(_SN_DIR, 'truth_sn_summary_v1-0-0.db')
_SUMM_TABLE = 'truth_sn_summary'
_OUT_TABLE = 'truth_sn_variability'
_OUT_FILE = os.path.join(_SN_DIR, _OUT_TABLE + '.db')
_VAR_FILE = os.path.join(_SN_DIR, 'sum_variable-31mar.db')
_VAR_TABLE = 'sn_variability_truth'

_OUT_COLUMNS = [('id', 'TEXT'), ('obsHistID', 'BIGINT'), ('MJD', 'DOUBLE'),
                ('bandpass', 'TEXT'), ('delta_flux', 'FLOAT'),
                ('id_int', 'BIGINT')]

class SnVariabilityWriter:
    def __init__(self, summ_file=_SUMM_FILE, out_file=_OUT_FILE,
                 var_file=_VAR_FILE):
        self._summ_file = summ_file
        self._var_file = var_file
        self._out_file = out_file

        ins = f'insert into {_OUT_TABLE} VALUES ('
        for i in range(len(_OUT_COLUMNS) - 1):
            ins += '?,'
        ins += '?)'
        self._insert = ins

    def create(self, chunksize=50000, max_chunk=None):
        read_conn = connect_read(self._summ_file)

        attach_q = "attach '" + self._var_file + "' as var"
        read_conn.execute(attach_q)

        select_columns = (f'{_VAR_TABLE}.id', 'obsHistID', 'MJD',
                          'bandpass', 'delta_flux', 'id_int')
        table_spec = f'{_SUMM_TABLE} INNER JOIN var.{_VAR_TABLE} ON '
        table_spec += f'{_SUMM_TABLE}.id = var.{_VAR_TABLE}.id'
        select_q = 'SELECT ' + ','.join(select_columns) + ' from '
        select_q += table_spec

        read_cur = read_conn.cursor()
        read_cur.arraysize = chunksize
        read_cur.execute(select_q)

        # If we got this far, create new table
        create_query = assemble_create_table(_OUT_TABLE, _OUT_COLUMNS)
        with sqlite3.connect(self._out_file) as conn:
            conn.execute(create_query)

        done = False
        i_chunk = 0

        while not done:
            if max_chunk:
                if i_chunk >= max_chunk:
                    break
            done = self._do_chunk(read_cur)
            if done:
                break
            i_chunk += 1
            if i_chunk % 10 == 0:
                print('Next chunk is ', i_chunk)

        read_conn.close()

    def _do_chunk(self, read_cur):
        '''
        Get a chunk of rows and write them to the new db.
        Return True if there is nothing more to do
        '''
        rows = read_cur.fetchmany()
        if len(rows) == 0:
            return True


        with sqlite3.connect(self._out_file) as conn:
            cur = conn.cursor()
            cur.executemany(self._insert, rows)
            conn.commit()

        return False

if __name__ == '__main__':
    # for testing
    #writer = SnVariabilityWriter(out_file=os.path.join(_SN_DIR, 'truth_sn_var_test.db'))

    # real
    writer = SnVariabilityWriter()

    # for testing
    # writer.create(chunksize=1000, max_chunk=3)

    # for real
    writer.create()
