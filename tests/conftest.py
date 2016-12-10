# -*- coding: utf-8 -*-

import pytest  # noqa
import logging


# Set up logging for tests
formatter = logging.Formatter(
    '%(asctime)s %(levelname)s  [%(name)s] (%(threadName)s): %(message)s')
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def pytest_addoption(parser):
    parser.addoption("--waitlong", action="store_true",
                     help="Wait longer for server startup, shutdown etc.")
    parser.addoption("--domain", action="store_true",
                     help="Start server in domain mode instead of standalone.")
