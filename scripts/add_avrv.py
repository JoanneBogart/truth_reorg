import os
import re
from collections import namedtuple
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from lsst.sims.catUtils.dust import EBVbase
from desc.truth_reorg.oldsim_utils import get_MW_AvRv
from desc.truth_reorg.script_utils import print_callinfo, print_date

###Col = namedtuple('column_descriptor', ['name', 'values', 'datatype'])
Col = namedtuple('column_descriptor', ['name', 'datatype'])
'''
datatype should be a pyarrow datatype, e.g. pyarrow.int32(), pyarrow.float64()
'''

_INPUT_DIR = '/global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/truth/galtruth'

_OUTPUT_DIR = '/global/cscratch1/sd/jrbogart/desc/truth/galtruth_test'
class AugmentAvRv():
    '''
    For an input parquet file with ra,dec columns, generate Av, Rv, columns
    and write output parquet file appending them
    '''
    def __init__(self, input_dir=_INPUT_DIR, output_dir=_OUTPUT_DIR):
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._ebv_model = EBVbase()

        self._file_pattern = re.compile('truth_summary_hp\d+.parquet')

    def process_file(self, infilename, outfilename=None, ra='ra', dec='dec',
                     dry_run=False):
        if not outfilename:
            outfilename = infilename

        inpath = os.path.join(self._input_dir, infilename)
        outpath = os.path.join(self._output_dir, outfilename)

        # Open output file
        self._pq_in = pq.ParquetFile(inpath)
        av_field = pa.field('av', pa.float32())
        rv_field = pa.field('rv', pa.float32())

        if pa.__version__ == '0.15.1':        # old stack
            out_schema = self._pq_in.schema.to_arrow_schema()
            out_schema = out_schema.append(av_field)
            out_schema = out_schema.append(rv_field)
        else:
            out_schema = self._pq_in.schema_arrow
            out_schema = out_schema.append_field(av_field)
            out_schema = out_schema.append_field(rv_field)

        if not dry_run:
            self._pq_out = pq.ParquetWriter(outpath, out_schema)
        num_row_groups = self._pq_in.metadata.num_row_groups

        for i in range(num_row_groups):
            tbl = self._pq_in.read_row_group(i)
            av, rv = get_MW_AvRv(self._ebv_model, tbl[ra], tbl[dec])
            av_l = pa.array(av, pa.float32())
            rv_l = pa.array(rv, pa.float32())

            tbl = tbl.append_column(av_field, av_l)
            tbl = tbl.append_column(rv_field, rv_l)
            if not dry_run:
                self._pq_out.write_table(tbl)

    def process_all(self, ra='ra', dec='dec', dry_run=False):
        '''
        Process all suitable files in the input directory. Each output file
        will have the same basename as corresponding input file
        '''
        files = os.listdir(self._input_dir)
        for f in files:
            if self._file_pattern.match(f):
                if dry_run:
                    print('Found match: ', f)
                else:
                    self.process_file(f, ra, dec, dry_run=dry_run)


def hp_to_filename(hp):
    '''
    Given a healpix number, generate standard filename
    '''
    return f'truth_summary_hp{hp}.parquet'

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Make new parquet file, adding av, rv columns to pre-existing file')
    parser.add_argument('--input-dir', help='where to find inputs',
                        default=_INPUT_DIR)
    parser.add_argument('--output-dir', help='where to write outputs',
                        default=_OUTPUT_DIR)
    parser.add_argument('--ra-name', default='ra',
                        help='name of RA column, default="ra"' )
    parser.add_argument('--dec-name', default='dec',
                        help='name of declination column, default="dec"')
    parser.add_argument('--pixels', type=int, nargs='*', default=[9556],
                        help='healpix pixels for which augmented files will be created. If option is included with no value all suitable files in the directory wil be processed.')
    parser.add_argument('--dry-run', action='store_true',
                        help='If used, go through the motions without creating any files')


    args = parser.parse_args()
    print_callinfo('add_avrv', args)

    augment = AugmentAvRv(input_dir = args.input_dir,
                          output_dir=args.output_dir)

    if (len(args.pixels) > 0):
        for hp in args.pixels:
            print_date(msg=f'Starting pixel {hp}')
            augment.process_file(hp_to_filename(hp), ra=args.ra_name,
                                 dec=args.dec_name, dry_run=args.dry_run)
            print_date(msg=f'Finishing pixel {hp}')

    else:
        print_date(msg=f'Processing all suitable files in directory {args.input_dir}')
        augment.process_all(ra=args.ra_name, dec=args.dec_name,
                            dry_run=args.dry_run)
        print_date(msg='Processing complete')
