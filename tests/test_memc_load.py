import pytest
from memc_load.memc_load import insert_to_memc
from memc_load import appsinstalled_pb2 as pb


class DummyMemcacheClient:
    def __init__(self):
        self.store = {}

    def set(self, key, value):
        self.store[key] = value
        return True


def test_insert_to_memc_success(monkeypatch):
    dummy = DummyMemcacheClient()

    from memc_load import memc_load

    memc_load.devtype_to_memc["idfa"] = dummy

    result = insert_to_memc(
        dev_type="idfa",
        dev_id="test-device",
        lat=12.34,
        lon=56.78,
        apps=[1, 2, 3],
        dry=False,
    )

    assert result is True
    key = "idfa:test-device"
    assert key in dummy.store

    ua = pb.UserApps()
    ua.ParseFromString(dummy.store[key])
    assert ua.apps == [1, 2, 3]
    assert ua.lat == 12.34
    assert ua.lon == 56.78


def test_insert_to_memc_dry(monkeypatch):
    # Должно вернуть True, ничего не сохранив
    result = insert_to_memc("gaid", "dummy", 0, 0, [10], dry=True)
    assert result is True
