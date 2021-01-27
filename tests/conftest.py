from functools import partial
from unittest.mock import patch

import pytest
from fakeredis import FakeRedis, FakeServer


@pytest.fixture(scope='function', autouse=True)
def patch_redis_adapter():
    """Patches RedisAdapter

    1. patches RedisAdapter.__bases__ to use FakeRedis instead of Redis
    2. patches redis.from_url with FakeRedis.from_url
    """

    redis_adapter_path = 'corva.state.redis_adapter'

    redis_adapter_patcher = patch(f'{redis_adapter_path}.RedisAdapter.__bases__', (FakeRedis,))

    server = FakeServer()  # use FakeServer to share cache between different instances of RedisState

    with redis_adapter_patcher, \
         patch(f'{redis_adapter_path}.from_url', side_effect=partial(FakeRedis.from_url, server=server)):
        # necessary to stop mock.patch from trying to call delattr when reversing the patch
        redis_adapter_patcher.is_local = True
        yield
