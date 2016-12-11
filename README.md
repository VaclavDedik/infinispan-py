# Infinispan Python Client

[![Build Status](https://travis-ci.org/VaclavDedik/infinispan-py.svg?branch=master)](https://travis-ci.org/VaclavDedik/infinispan-py)
[![Coverage Status](https://coveralls.io/repos/github/VaclavDedik/infinispan-py/badge.svg?branch=master)](https://coveralls.io/github/VaclavDedik/infinispan-py?branch=master)
[![Python](https://img.shields.io/badge/python-2.7%2C%203.5%2C%20pypy-blue.svg)](https://www.python.org/)

Python client for Infinispan key-value store. Currently supported features:

 * CRUD operations `get`, `put`, `remove`, `contains_key`.
 * Compare-And-Swap operations `put_if_absent`, `replace`, `replace_with_version`, `get_with_version`.
 * Expiration with absolute lifespan or relative maximum idle time. This expiration parameters as passed as optional parameters to create/update methods and they support multiple time units, e.g. `lifespan='1m', max_idle='1d'`.
 * Update and remove operations can optionally return previous values by passing in `previous=True` option.
 * Server-side statistics can be retrieved using the `stats` operation.
 * Clients only need to be configure with a single node's address and from that node the rest of the cluster topology can be discovered. As nodes are added or destroyed, clients update their routing tables to reflect the change.
 * All operations can be called sychronously or asychronously (e.g. `put` is blocking, `put_async` is non-blocking and returns a Future).
 * Named caches are supported.

# Usage

```Python
from infinispan import Infinispan

with Infinispan(host='127.0.0.1', port=11222) as client:
    client.put("key1", "value1")
    value, version = client.get_with_version("key1")
    prev_val = client.replace_with_version("key1", value, version, lifespan='1d', previous=True)
    stats_f = client.stats_async()
    # ...
    print prev_val
    print stats_f.result()
```
