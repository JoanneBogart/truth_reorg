import numpy as np
import sqlite3
import os
from desc.truth_reorg.sphgeom_utils import Region, DC2_RA_MID, DC2_RA_NE, DC2_DEC_NE, DC2_DEC_S
from desc.truth_reorg.truth_reorg_utils import connect_read

# Note: this code must be run in lsst_distrib environment for lsst.sphgeom

'''

File to be trimmed
Names of ra, dec columns (default to 'ra', 'dec')
Path for output file
'''

class Trimmer:
    '''
    Parameters
    ----------
    File to be trimmed
    Output file
    Table name
    Names of ra, dec columns (default to 'ra', 'dec')
    '''
    _IO_DIR = os.path.join(os.getenv('SCRATCH'), 'desc/truth/star')
    _IFILE = os.path.join(_IO_DIR, 'truth_star_summary_big.db')
    _OFILE = os.path.join(_IO_DIR, 'truth_star_summary_trimmed.db')
    def __init__(self, ifile=_IFILE, ofile=_OFILE,
                 table_name='truth_star_summary', ra_name='ra', dec_name='dec'):
        self._ifile = ifile
        self._ofile = ofile
        self._ra_name = ra_name
        self._dec_name = dec_name
        self._region = None
        self._table_name = table_name

        read_schema_query = "select sql from sqlite_schema where name=?"
        read_columns_query = "select * from pragma_table_info(?)"
        with connect_read(ifile) as conn:
            cursor = conn.cursor()
            cursor.execute(read_schema_query, (table_name,))
            row = cursor.fetchone()
            self._create_string = row[0]
            cursor.execute(read_columns_query, (table_name,))
            column_info = cursor.fetchall()
            # Format of column_info is rowid, column_name, column_type,..
            self._columns = [c[1] for c in column_info]

        # Form insert query
        ins = f'insert into {table_name} VALUES ('
        for i in range(len(self._columns) - 1):
            ins += '?,'
        ins += '?)'
        self._insert = ins

    def set_region(self, ra_mid, ne_ra, ne_dec, s_dec):
        self._region = Region(ra_mid, (ne_ra, ne_dec), (s_dec, ne_dec))

    def trim(self, chunksize=50000, max_chunk=None):
        # Make mask of rows to be kept
        radec_q = f'select {self._ra_name},{self._dec_name} from ' + self._table_name
        column_string = ','.join(self._columns)
        bigread_q = ' '.join(['select', column_string, 'from', self._table_name])
        with connect_read(self._ifile) as conn:
            cur = conn.cursor()
            cur.arraysize = chunksize
            mask_chunks = []
            cur.execute(radec_q)

            done = False
            i_chunk = 0
            while not done:
                if max_chunk:
                    if i_chunk >= max_chunk:
                        done = True
                        break
                rows = cur.fetchmany()
                if len(rows) == 0:
                    done = True
                    break
                ra, dec = zip(*rows)
                mask_chunks.append(self._region.contains(ra, dec))
                i_chunk += 1

                # rows = cur.fetchall()
                # ra, dec = zip(*rows)
                # mask_full = self._region.contains(ra, dec)

        read_conn = connect_read(self._ifile)
        read_cur = read_conn.cursor()
        read_cur.arraysize=chunksize

        read_cur.execute(bigread_q)
        done = False
        lower = 0

        # If we got this far, we're ready to write

        with sqlite3.connect(self._ofile) as conn:
            cursor = conn.cursor()
            cursor.execute(self._create_string)

        i_chunk = 0
        while not done:
            if max_chunk:
                if i_chunk >= max_chunk:
                    break
            if i_chunk >= len(mask_chunks):
                break
            #mask_chunk = mask_full[lower : lower+chunksize]
            mask_chunk = mask_chunks[i_chunk]
            done = self._do_chunk(read_cur, mask_chunk)
            lower += chunksize
            i_chunk += 1
            if i_chunk % 10 == 0:
                print('Next chunk is ', i_chunk)

        read_conn.close()

    def _do_chunk(self, read_cur, mask_chunk):
        '''
        Get some rows, decide which to exclude, write the rest
        Return True if there is nothing more to do
        '''
        rows = read_cur.fetchmany()
        if len(rows) == 0:
            return True
        to_write = []
        for e in zip(mask_chunk, rows):
            if e[0]:
                to_write.append(e[1])
        if len(to_write) == 0:
            return False
        with sqlite3.connect(self._ofile) as out_conn:
            out_conn.executemany(self._insert, to_write)
            out_conn.commit()
        return False

if __name__ == '__main__':

    star_dir = os.path.join(os.getenv('SCRATCH'), 'desc/truth/star')
    infile = os.path.join(star_dir, 'truth_star_summary_big.db')
    outfile = os.path.join(star_dir, 'truth_star_summary_trim_more.db')
    ##outfile = os.path.join(star_dir, 'truth_star_trim_test.db')

    trimmer = Trimmer(ifile=infile, ofile=outfile)

    pad_n = 0.6
    pad_s = 0.2
    pad_ew = 0.2

    trimmer.set_region(DC2_RA_MID, DC2_RA_NE + pad_ew,
                       DC2_DEC_NE + pad_n, DC2_DEC_S - pad_s)

    # For real
    trimmer.trim()

    # For debugging (first 20500 objects are not in the region)
    ## trimmer.trim(chunksize=10000, max_chunk=3)
