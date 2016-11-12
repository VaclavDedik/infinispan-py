
import pytest  # noqa


def pytest_addoption(parser):
    parser.addoption("--waitlong", action="store_true",
                     help="Wait longer for server startup, shutdown etc.")
