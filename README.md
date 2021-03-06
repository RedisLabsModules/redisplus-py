[![Build Pipeline](https://github.com/redislabsmodules/redisplus-py/actions/workflows/tox-build.yml/badge.svg)](https://github.com/redislabsmodules/redisplus-py)
[![LGTM](https://img.shields.io/lgtm/alerts/g/RedisLabsModules/redisplus-py.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisLabsModules/redisplus-py/alerts/)

# redisplus

The python interface to redis modules redismodules.io. This is a unified client using [redis-py](https://github.com/andymccurdy/redis-py) to send commands to redis, and access the modules.

## Installation

To install redisplus-py run:

```pip install redisplus```

## Python version support

redisplus supports python 3.6 and higher, and is tested using 3.6 and pypy.

## Examples

Documents can be found at [readthedocs](http://placeholder) but below are some examples.

**Connect using a pre-build redis client, for all modules**
```
import redis
from redisplus import Client

r = redis.Redis()
rc = Client(r)
rc.status()
```

**Access json features, setting a json key**

```
import redis
from redisplus import Client

r = redis.Redis()
rc = Client()
rc.json.jsonset("foo", ".", "bar")
rc.exists("foo")
```

----------------------------------------------------------------------------------------------------

## Getting Started

Dependency and package management is described using [pypoetry](https://python-poetry.org/), unit tests are defined with [tox](https://tox.readthedocs.io/en/latest/). Assuming you have a valid version of python available as the binary **python**, the easiest way to provision an environment is by creating a virtual env, and provisioning the environment:

```
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip poetry
poetry install
```

Tests are written with [pytest](https://docs.pytest.org), and most easily run by running *pytest*. Support for individual markers can be found in the project's [pyproject.toml](pyproject.toml) file. Similarly, if you have [pyenv](https://github.com/pyenv/pyenv) installed, tox uses pyenv to find versions of python, for validating tests across multiple python versions. *tox -listenvs* will output options.

### Tasks

redisplus uses [invoke](https://pyinvoke.org) once dependencies for convenience tasks. Run *invoke -l* to see the list of supported tasks.
