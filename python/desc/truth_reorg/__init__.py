from . import truth_reorg_utils
from . import script_utils

import eups

eupsenv = eups.Eups()
product_name = 'lsst_distrib'
get_product = eupsenv.getSetupProducts(product_name)

if get_product == []:
    from . import oldsim_utils
else:
    from . import sphgeom_utils
