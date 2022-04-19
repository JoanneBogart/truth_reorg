import os
import numpy as np
import sqlite3

from lsst.sims.catUtils.dust import EBVbase
'''
This is a companion script to trim_sn_summary.py.  The output of
trim_sn_summary.py is this input to complete_sn_summary.


complete_sn_summary must run in a DC2-era lsst_sims environment. It will
     - Add new integer id column (keep original id)
     - Add Rv, Av columns
     - Add columns for max observed delta flux

'''

_INIT_COLUMNS = [('id', 'TEXT'), ('host_galaxy', 'BIGINT'),
                 ('ra', 'DOUBLE'), ('dec', 'DOUBLE'), ('redshift', 'DOUBLE'),
                 ('c', 'DOUBLE'), ('mB', 'DOUBLE'), ('t0', 'DOUBLE'),
                 ('x0', 'DOUBLE'), ('x1', 'DOUBLE')]
_ADD_COLUMNS = [('id_int', 'BIGINT'), ('av', 'FLOAT'), ('rv', 'FLOAT'),
                ('max_flux_u', 'FLOAT'),('max_flux_g', 'FLOAT'),
                ('max_flux_r', 'FLOAT'),('max_flux_i', 'FLOAT'),
                ('max_flux_z', 'FLOAT'),('max_flux_y', 'FLOAT')]
_INITIAL_TABLE = 'initial_summary'

_SN_DIR = os.path.join(os.getenv('SCRATCH'), 'desc/truth/sn')
_IN_FILE = os.path.join(_SN_DIR, 'initial_table.db')
_IN_TABLE = 'initial_summary'
_OUT_TABLE = 'truth_sn_summary'
_OUT_FILE = os.path.join(_SN_DIR, _OUT_TABLE + '.db')
_VAR_FILE = os.path.join(_SN_DIR, 'sum_variable-31mar.db')
_VAR_TABLE = 'sn_variability_truth'
_MAX_STAR_ID = 41021613038
_SN_OBJ_TYPE = 22

class SnSummaryWriter:
    '''
    This class finishes the work of creating the table
    truth_sn_summary. It will
        * Adds columns for max flux per band
        * Adds Rv, Av
        * Add new integer id

    '''
    ebv_model = EBVbase()

    def __init__(self, out_file=_OUT_FILE, in_file=_IN_FILE,
                 in_table=_IN_TABLE, var_file=_VAR_FILE):
        self._out_file = out_file
        self._out_table = _OUT_TABLE
        self._in_file = in_file
        self._in_table = in_table
        self._var_file = var_file

    @staticmethod
    def _connect_read(path):
        '''
        Not obvious how to connect read-only to SQLite db. Package it up here
        '''
        conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
        return conn

    @staticmethod
    def get_MW_AvRv(ra, dec, Rv=3.1):
        '''
        Copied from
        https://github.com/LSSTDESC/sims_TruthCatalog/blob/master/python/desc/sims_truthcatalog/synthetic_photometry.py#L133
        '''
        #eq_coord = np.array([[np.radians(ra)], [np.radians(dec)]])
        eq_coord = np.array([np.radians(ra), np.radians(dec)])
        ebv = SnSummaryWriter.ebv_model.calculateEbv(equatorialCoordinates=eq_coord,
                                                     interp=True)
        Av = Rv*ebv
        return Av, Rv

    @staticmethod
    def make_int_id(host):
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
            new_id = host * 1024 + _SN_OBJ_TYPE

        return new_id

    _MAX_FLUX_QUERY = '''select bandpass, max(delta_flux)
    from sn_variability_truth where id=? group by bandpass'''

    _INSERT = 'insert into ' + _OUT_TABLE +  ''' VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

    @staticmethod
    def get_max_fluxes(conn, id):
        '''
        Give connection to variability file and id, find max flux for
        each band.  Return a tuple of values in the usual order
        '''
        out_dict = {}
        cur = conn.cursor()
        cur.execute(SnSummaryWriter._MAX_FLUX_QUERY, (id,))
        for row in cur:
            out_dict[row[0]] = row[1]

        return (out_dict.get('u'), out_dict.get('g'), out_dict.get('r'),
                out_dict.get('i'), out_dict.get('z'), out_dict.get('y'))

    @staticmethod
    def assemble_create_table(table_name, columns):
        '''
        Return string which will create table with supplied names
        and column specifications (a tuple (col_name, col_type) )
        '''
        stmt = 'CREATE TABLE ' + table_name + '('

        col_specs = [f'{c[0]} {c[1]}' for c in columns]
        stmt += ','.join(col_specs) + ')'
        return stmt

    def _do_chunk(self, in_cur):
        '''
        Fetch the next set of rows, calculate additional columns
        and write to output.
        Returns
        -------
        False if there might be more data; otherwise (all done) True
        '''
        rows = in_cur.fetchmany()
        if len(rows) == 0:
            return True

        id_list, host, ra, dec, c5, c6, c7, c8, c9 = zip(*rows)

        Av, rv = self.get_MW_AvRv(ra, dec)
        Rv = np.full((len(Av),), rv)
        id_int = [self.make_int_id(h) for h in host]

        max_deltas = [self.get_max_fluxes(self._conn_var, id) for id in id_list]
        u, g, r, i, z, y = zip(*max_deltas)
        to_write = list(zip(id_list, host, ra, dec, c5, c6, c7, c8, c9,
                            id_int, Av, Rv, u, g, r, i, z, y))

        self._conn_out.cursor().executemany(self._INSERT, to_write)

        self._conn_out.commit()

        return False

    def complete(self, chunksize=20000, max_chunk=None):
        self._conn_in = self._connect_read(self._in_file)
        self._conn_var = self._connect_read(self._var_file)
        self._conn_out = sqlite3.connect(self._out_file)

        out_columns = _INIT_COLUMNS + _ADD_COLUMNS

        create_query = self.assemble_create_table(_OUT_TABLE, out_columns)

        self._conn_out.cursor().execute(create_query)

        self._in_names = [e[0] for e in _INIT_COLUMNS]
        rd_query = 'select ' + ','.join(self._in_names) + ' from ' + self._in_table
        in_cur = self._conn_in.cursor()
        in_cur.arraysize = chunksize
        in_cur.execute(rd_query)

        done = False
        i_chunk = 0

        while not done:
            done = self._do_chunk(in_cur)
            if done:
                print("all done")
            else:
                print('completed chunk ', i_chunk)
            i_chunk += 1
            if max_chunk:
                if i_chunk >= max_chunk:
                    break

        self._conn_in.close()
        self._conn_out.close()
        self._conn_var.close()

if __name__ == '__main__':

    out_file = os.path.join(_SN_DIR, 'truth_sn_summary.db')
    writer = SnSummaryWriter(out_file=out_file)

    # A call suitable for testing
    #writer.complete(chunksize=10, max_chunk=3)

    writer.complete()
