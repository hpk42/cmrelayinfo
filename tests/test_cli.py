import json
import time

import pytest

import cmrelayinfo
from cmrelayinfo import create_qr_url, get_relay_list


def test_create_qr_url_domain():
    assert create_qr_url("nine.testrun.org") == "dcaccount:nine.testrun.org"


def test_create_qr_url_ip():
    url = create_qr_url("116.203.128.59")
    assert url.startswith("dclogin:")
    assert "@116.203.128.59/" in url
    assert "ip=993" in url and "sp=465" in url


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(cmrelayinfo, "get_cache_dir", lambda: tmp_path)
    return tmp_path


def test_relay_list_cache_fresh(cache_dir):
    cache_dir.joinpath("relays.json").write_text(
        json.dumps(
            {
                "fetched_at": int(time.time()),
                "relays": ["a.example", "b.example"],
                "via": {"b.example": "chat.sus.fr"},
            }
        )
    )
    relays, via = get_relay_list()
    assert relays == ["a.example", "b.example"]
    assert via == {"b.example": "chat.sus.fr"}


def test_relay_list_cache_stale_fetch_fails_uses_stale(cache_dir, monkeypatch):
    cache_dir.joinpath("relays.json").write_text(
        json.dumps({"fetched_at": 1, "relays": ["stale.example"]})
    )

    def failing_urlopen(*args, **kwargs):
        raise OSError("network down")

    monkeypatch.setattr(cmrelayinfo.urllib.request, "urlopen", failing_urlopen)
    relays, via = get_relay_list()
    assert relays == ["stale.example"]
    assert via == {}


def test_expand_relay_args_hub(monkeypatch):
    from cmrelayinfo import expand_relay_args

    page = (
        '<a href="dcaccount:https://e2e.sus.fr/new">join</a>'
        '<a href="dcaccount:https://chat.me.ke/new">join</a>'
    )
    monkeypatch.setattr(cmrelayinfo, "fetch_url", lambda url, timeout=30: page)
    relays, via, failed = expand_relay_args(["chat.sus.fr", "nine.testrun.org"])
    assert relays == ["e2e.sus.fr", "chat.me.ke", "nine.testrun.org"]
    assert via == {"e2e.sus.fr": "chat.sus.fr", "chat.me.ke": "chat.sus.fr"}
    assert failed == []


def test_expand_relay_args_hub_failure(monkeypatch):
    from cmrelayinfo import expand_relay_args

    def failing_fetch(url, timeout=30):
        raise OSError("boom")

    monkeypatch.setattr(cmrelayinfo, "fetch_url", failing_fetch)
    relays, via, failed = expand_relay_args(["chat.sus.fr", "a.example"])
    assert relays == ["a.example"]
    assert failed == [("chat.sus.fr", "hub fetch failed: boom")]


def test_relay_label():
    from cmrelayinfo import relay_label

    assert relay_label("a.example", None) == "a.example"
    assert relay_label("b.example", "chat.sus.fr") == "b.example [chat.sus.fr]"


def test_relay_list_no_cache_fetch_fails_exits(cache_dir, monkeypatch):
    def failing_urlopen(*args, **kwargs):
        raise OSError("network down")

    monkeypatch.setattr(cmrelayinfo.urllib.request, "urlopen", failing_urlopen)
    with pytest.raises(SystemExit):
        get_relay_list()
