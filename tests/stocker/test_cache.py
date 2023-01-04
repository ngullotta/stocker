from stocker.cache import CacheController


def test_cache_controller(tmp_path):
    assert CacheController(tmp_path) is not None
