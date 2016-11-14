# -*- coding: utf-8 -*-

import pytest

from infinispan import utils
from infinispan.hotrod import TimeUnits


class TestUtils(object):
    def test_from_pretty_time(self):
        assert utils.from_pretty_time('10s') == (10, TimeUnits.SECONDS)
        assert utils.from_pretty_time('10ms') == (10, TimeUnits.MILISECONDS)
        assert utils.from_pretty_time('10ns') == (10, TimeUnits.NANOSECONDS)
        assert utils.from_pretty_time('10us') == (10, TimeUnits.MICROSECONDS)
        assert utils.from_pretty_time('10m') == (10, TimeUnits.MINUTES)
        assert utils.from_pretty_time('10h') == (10, TimeUnits.HOURS)
        assert utils.from_pretty_time('10d') == (10, TimeUnits.DAYS)

    def test_from_pretty_time_invalid_format(self):
        with pytest.raises(ValueError):
            utils.from_pretty_time('10')
        with pytest.raises(ValueError):
            utils.from_pretty_time('s')
        with pytest.raises(ValueError):
            utils.from_pretty_time('10S')
        with pytest.raises(ValueError):
            utils.from_pretty_time('10s1')
