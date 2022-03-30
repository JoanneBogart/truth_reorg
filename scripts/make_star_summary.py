import os
import numpy as np
import sqlite3

# Note: because of the following import, this code must be run in
# an environment where old lsst-sims is available
from lsst.sims.catUtils.dust import EBVbase

from desc.truth_reorg.truth_reorg_utils import assemble_create_table,connect_read

from desc.truth_reorg.oldsim_utils import  get_MW_AvRv

'''
Inputs:
Old (trimmed) star truth summary
LC stats

Outputs:
truth_star_summary table in SQLite file

The following need to happen:
* read in columns of interest from the inputs.
  - From the summary table, need only
    id, ra, dec, flux_<band>.  Don't need is_variable
    because we're going to include the more useful above_threshold instead
  - From LC stats need model and stdev_<band>
* use them all in the new table except from set stdev_<band> store only max
* convert id field to int
* add Av, Rv as was done for SNe
'''

__all__ = ['StarSummaryWriter']

_STAR_DIR = os.path.join(os.getenv('SCRATCH'), 'desc/truth/star')
_OLD_SUMMARY = os.path.join(_STAR_DIR, 'star_truth_summary_trimmed.db')
_OLD_SUMMARY_TABLE = 'truth_summary'
_LC_STATS = os.path.join(_STAR_DIR, 'star_lc_stats_trimmed.db')
_LC_STATS_TABLE = 'stellar_variability_stats'
_OUT_TABLE = 'truth_star_summary'

#NOTE: Have confirmed that old summary table and lc status have the
#      same ordering for id

_OUT = os.path.join(_STAR_DIR, 'truth_star_summary.db')

class StarSummaryWriter:

    _OUT_COLUMNS = [('id', 'BIGINT'), ('ra', 'DOUBLE'), ('dec', 'DOUBLE'),
                    ('flux_u', 'FLOAT'), ('flux_g', 'FLOAT'),
                    ('flux_r', 'FLOAT'), ('flux_i', 'FLOAT'),
                    ('flux_z', 'FLOAT'), ('flux_y', 'FLOAT'),
                    ('model', 'TEXT'), ('max_stdev_delta_mag', 'FLOAT'),
                    ('above_threshold', 'INT'),
                    ('av', 'FLOAT'), ('rv', 'FLOAT')]
    _SUMM_COLUMNS = ('id', 'ra', 'dec', 'flux_u', 'flux_g', 'flux_r',
                     'flux_i', 'flux_z', 'flux_y')
    _LC_STATS_COLUMNS = ('model', 'stdev_u', 'stdev_g', 'stdev_r',
                         'stdev_i', 'stdev_z', 'stdev_y')

    _INSERT = 'insert into ' + _OUT_TABLE +  ''' VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

    _DMAG_THRESHOLD = 0.001

    def __init__(self, old_summary=_OLD_SUMMARY, lc_stats=_LC_STATS):
        self._old_summary = old_summary
        self._lc_stats = lc_stats
        self._ebv_model = EBVbase()

    @staticmethod
    def to_int(s):
        return int(s)

    def _do_chunk(self, summ_cur, lc_cur):
        '''
        Read in a chunk from each of two tables, compute Av, Rv and
        max_stdev, glue it all back together and write to output.
        Return True if input is exhausted, else False
        '''
        summ_rows = summ_cur.fetchmany()
        if len(summ_rows) == 0:
            return True
        lc_rows = lc_cur.fetchmany()

        id_text, ra, dec, flux_u, flux_g, flux_r, flux_i, flux_z, flux_y = zip(*summ_rows)
        model, stdev_u, stdev_g, stdev_r, stdev_i, stdev_z, stdev_y = zip(*lc_rows)
        max_mag = np.amax(np.array([stdev_u, stdev_g, stdev_r, stdev_i,
                                    stdev_z, stdev_y]), axis=0)
        # convert boolean to int (actually first np.int64)
        # Following does not play nicely with sqlite.  It doesn't seem to
        # know what to do with np.int64
        above_np = np.multiply(max_mag > self._DMAG_THRESHOLD, 1)

        # so try this
        above_threshold = [int(m) for m in above_np]
        av, rv = get_MW_AvRv(self._ebv_model, ra, dec)

        id_int = [int(i_t) for i_t in id_text]

        to_write = list(zip(id_int, ra, dec,
                            flux_u, flux_g, flux_r, flux_i, flux_z, flux_y,
                            model, max_mag, above_threshold, av, rv))

        self._out_conn.cursor().executemany(self._INSERT, to_write)
        self._out_conn.commit()

        return False

    def create(self, out_file=_OUT, chunksize=20000, max_chunk=None):
        self._outfile = out_file
        self._chunksize = chunksize

        old_summary_conn = connect_read(self._old_summary)
        lc_conn = connect_read(self._lc_stats)
        out_conn = sqlite3.connect(out_file)
        self._out_conn = out_conn
        # create new table
        create_stmt = assemble_create_table(_OUT_TABLE, self._OUT_COLUMNS)
        out_conn.execute(create_stmt)

        old_summary_cur = old_summary_conn.cursor()
        old_summary_cur.arraysize = chunksize

        lc_cur = lc_conn.cursor()
        lc_cur.arraysize = chunksize

        select_old = 'SELECT ' + ','.join(self._SUMM_COLUMNS) + ' from ' + _OLD_SUMMARY_TABLE
        select_lc = 'SELECT ' + ','.join(self._LC_STATS_COLUMNS) + ' from ' + _LC_STATS_TABLE

        old_summary_cur.execute(select_old)
        lc_cur.execute(select_lc)
        done = False
        i_chunk = 0

        while not done:
            done = self._do_chunk(old_summary_cur, lc_cur)
            if done:
                print("all done")
            else:
                if i_chunk % 10 == 0:
                    print('completed chunk ', i_chunk)
            i_chunk += 1
            if max_chunk:
                if i_chunk >= max_chunk:
                    break

        old_summary_conn.close()
        lc_conn.close()
        self._out_conn.close()

if __name__ == '__main__':
    #out_file = os.path.join(_STAR_DIR, 'truth_star_summary_test_v1.db') # TEST
    out_file = os.path.join(_STAR_DIR, 'truth_star_summary_v2.db')

    writer = StarSummaryWriter()

    #writer.create(out_file=out_file, chunksize=10000, max_chunk=3) # TEST
    writer.create(out_file=out_file, chunksize=50000)
