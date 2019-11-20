# pysppin package

import pkg_resources

from . import itis
from . import worms
from . import natureserve
from . import ecos
from . import gap
from . import iucn
from . import sgcn
from . import gbif
from . import utils

__version__ = pkg_resources.require("pysppin")[0].version


def get_package_metadata():
    d = pkg_resources.get_distribution('pysppin')
    for i in d._get_metadata(d.PKG_INFO):
        print(i)

