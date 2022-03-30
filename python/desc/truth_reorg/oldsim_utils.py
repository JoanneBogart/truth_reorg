import numpy as np

__all__ = ['get_MW_AvRv']


def get_MW_AvRv(ebv_model, ra, dec, Rv=3.1):
    '''
    Copied from
    https://github.com/LSSTDESC/sims_TruthCatalog/blob/master/python/desc/sims_truthcatalog/synthetic_photometry.py#L133
    '''
    eq_coord = np.array([np.radians(ra), np.radians(dec)])
    ebv = ebv_model.calculateEbv(equatorialCoordinates=eq_coord,
                                 interp=True)
    Av = Rv*ebv
    rv = np.full((len(Av),), Rv)

    return Av, rv
