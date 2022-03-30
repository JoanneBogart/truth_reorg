import numpy as np
import lsst.sphgeom
import astropy.units as u

__all__ = ['Region', 'DC2_RA_MID', 'DC2_RA_NE', 'DC2_DEC_NE', 'DC2_DEC_S']

DC2_RA_MID = 61.855
DC2_RA_NE = 71.46
DC2_DEC_NE = -27.25
DC2_DEC_S = -44.33
class Region:
    def __init__(self, ra_mid=DC2_RA_MID, ne_corner=(DC2_RA_NE, DC2_DEC_NE),
                 dec_range=(DC2_DEC_S, DC2_DEC_NE)):
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
