
from infinispan.client import Infinispan  # noqa
from infinispan.error import *  # noqa
from infinispan.connection import *  # noqa


# Set up logging
import logging
from logging import NullHandler

logging.getLogger(__name__).addHandler(NullHandler())
