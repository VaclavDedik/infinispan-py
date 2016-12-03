# -*- coding: utf-8 -*-

import pytest  # noqa


def pytest_addoption(parser):
    parser.addoption("--waitlong", action="store_true",
                     help="Wait longer for server startup, shutdown etc.")
    parser.addoption("--domain", action="store_true",
                     help="Start server in domain mode instead of standalone.")
