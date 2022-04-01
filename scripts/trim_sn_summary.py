"""
This script must run in a newish setup of lsst_distrib
Trim SN according to two criteria:
    - exclude sources outside the DC2 footprint
    - exclude Run3.1i sources (id name starts with 'mDDF')
Remove columns from truth_summary which are unnecessary
Add some columns from sne params

A separate script, which must run in a DC2-era lsst_sims environment, will
complete calculation of additional columns.
See   complete_sn_summary.py
"""

import os
import numpy as np
import sqlite3
import astropy.units as u
import lsst.sphgeom

__all__ = ['Region', 'TrimSnSummary']
_RA_MID = 61.855
_RA_NE = 71.46
_DEC_NE = -27.25
_DEC_S = -44.33
class Region:
    def __init__(self, ra_mid=_RA_MID, ne_corner=(_RA_NE, _DEC_NE),
                 dec_range=(-44.33, -27.25)):
        self._ra_mid = ra_mid
        ra0 = ne_corner[0]
        cos_dec0 = np.cos(np.radians(ne_corner[1]))
        self._dra_scale = np.abs(ra0 - self._ra_mid)*cos_dec0
        self._dec_range = dec_range

        self.region_corners = []
        for dec in dec_range:
            dra = self._dra(dec)
            self.region_corners.extend([(ra_mid - dra, dec),
                                        (ra_mid + dra, dec)])
        self.region_polygon = self.get_convex_polygon(self.region_corners)

    @staticmethod
    def DDFRegion():
        return Region(ra_mid=53.125, ne_corner=(53.764, -27.533),
                      dec_range=(-28.667, -27.533))

    @staticmethod
    def get_convex_polygon(corners):
        vertices = []
        for corner in corners:
            lonlat = lsst.sphgeom.LonLat.fromDegrees(*corner)
            vertices.append(lsst.sphgeom.UnitVector3d(lonlat))
        return lsst.sphgeom.ConvexPolygon(vertices)

    def _dra(self, dec):
        return np.abs(self._dra_scale/np.cos(np.radians(dec)))

    def contains(self, ra, dec, degrees=True):
        '''
        Given parallel arrays ra, dec representing points, return a mask
        with an entry set to True if that ra, dec in inside the region.
        Use sphgeom ConvexPolygon routine
        '''
        if degrees:
            # convert to radians
            ra = [(r * u.degree).to_value(u.radian) for r in ra]
            dec = [(d * u.degree).to_value(u.radian) for d in dec]

        return self.region_polygon.contains(ra, dec)

class TrimSnSummary:
    '''
    This class creates a new SQLite file containing the table
    truth_sn_summary.   As compared to old truth_summary it
        * Excludes SNe outside footprint or belonging to Run3.1i
        * Eliminates is_pointsource, is_variable, and flux columns
        * Adds some columns from sn params: mB, t0, x0, x1
'''
    def __init__(self, region, old_sn_summary, sn_params):
        self._region = region
        self._old_summary = old_sn_summary
        self._sn_params = sn_params

    _INIT_COLUMNS = [('id', 'TEXT'), ('host_galaxy', 'BIGINT'),
                     ('ra', 'DOUBLE'), ('dec', 'DOUBLE'), ('redshift', 'DOUBLE'),
                     ('mB', 'DOUBLE'), ('t0', 'DOUBLE'),
                     ('x0', 'DOUBLE'), ('x1', 'DOUBLE')]
    _ADD_COLUMNS = [('id_int', 'BIGINT'), ('av', 'FLOAT'), ('rv', 'FLOAT'),
                    ('max_delta_flux_u', 'FLOAT'),('max_delta_flux_g', 'FLOAT'),
                    ('max_delta_flux_r', 'FLOAT'),('max_delta_flux_i', 'FLOAT'),
                    ('max_delta_flux_z', 'FLOAT'),('max_delta_flux_y', 'FLOAT')]
    _INITIAL_TABLE = 'initial_summary'
    _FINAL_TABLE = 'truth_sn_summary'

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

    def _do_trim_merge(self, outpath, chunksize):
        # Note: have confirmed that the usual two input files are
        # ordered the same way: truth_summary.id = sne_params.snid_in
        # for each row

        self._initial_out = outpath

        # open connection on 1 input file, attach the other
        conn = sqlite3.connect(self._old_summary)
        cur = conn.cursor()

        # select just ra and dec to form inclusion/exclusion mask
        m = None
        cur.arraysize = 600000 # arger than #rows in the tables
        cur.execute("SELECT ra, dec from truth_summary order by rowid")
        chunk = cur.fetchall()
        ra, dec = zip(*chunk)
        msk = self._region.contains(ra, dec)
        del chunk

        # For many-column select use chunk size specified
        cur.arraysize = chunksize
        attach = 'ATTACH DATABASE ? AS params'
        cur.execute(attach, (self._sn_params,))
        # open output file
        conn_write  = sqlite3.connect(self._initial_out)
        cur_write = conn_write.cursor()
        create_table_sql = self.assemble_create_table(self._INITIAL_TABLE,
                                                      self._INIT_COLUMNS)
        cur_write.execute(create_table_sql)

        big_select = '''
        select id, host_galaxy, ra, dec, redshift,
        mB, t0_in as t0, x0_in as x0, x1_in as x1
        from truth_summary join params.sne_params
        on truth_summary.rowid = params.sne_params.rowid
        order by truth_summary.rowid'''
        ins = '''
        insert into initial_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cur.execute(big_select)

        lower = 0
        msk_chunk = msk[lower : lower + chunksize]
        rows = cur.fetchmany()
        chunk_done = 0
        while len(rows) > 0:
            for e in zip(msk_chunk, rows):
                # exclude objects outside footprint or from Run3.1i
                if e[0] and not e[1][0].startswith("mDDF") and not e[1][0].startswith("hl_mddf"):
                    cur_write.execute(ins, e[1])
            chunk_done += 1
            lower += chunksize
            msk_chunk = msk[lower : lower + chunksize]
            rows = cur.fetchmany()

        conn_write.commit()
        conn_write.close()
        conn.close()
        print(f'Completed {chunk_done} chunks with chunk size {chunksize}')

    def create(self, outpath=None, chunksize=100000):
        self._do_trim_merge(outpath, chunksize)

if __name__ == '__main__':
    '''
    Inputs:
       Old sn truth summary table (sqlite)
       sn params table      (sqlite)

    Output:
        New db file with table named truth_sn_summary
    '''

    # Add some padding for region because of great circle sides of polygon
    RA_PAD = 0.2
    DEC_N_PAD = 0.6
    DEC_S_PAD = 0.2

    # For now hardcode the file paths
    sn_dir = os.path.join(os.getenv('SCRATCH'), 'desc/truth/sn')
    sn_sum = os.path.join(sn_dir, 'sum_variable-31mar.db')
    sn_params = os.path.join(sn_dir, 'sne_cosmoDC2_v1.1.4_MS_DDF.db')
    sn_initial_out = os.path.join(sn_dir, 'initial_table.db')

    # Add a little padding in ra to the corners of the region
    region = Region(ra_mid=_RA_MID,
                    ne_corner=(_RA_NE + RA_PAD, _DEC_NE + DEC_N_PAD),
                    dec_range=(_DEC_S - DEC_S_PAD, _DEC_NE + DEC_N_PAD))

    sn_trimmer = TrimSnSummary(region, sn_sum, sn_params)

    sn_trimmer.create(chunksize=30000, outpath=sn_initial_out)
