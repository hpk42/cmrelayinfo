from cmrelayinfo import parse_relay_list

PAGE = """
<html><body>
<h1>Chatmail relays</h1>
<ul>
<li><a href="https://nine.testrun.org">nine.testrun.org</a> is the default onboarding relay</li>
<li><a href="https://mehl.cloud">mehl.cloud</a>, hosted in Germany</li>
<li><a href="https://chat.adminforge.de">chat.adminforge.de</a> by adminForge</li>
<li><a href="https://mailchat.pl">mailchat.pl</a> Polish community</li>
</ul>
<p>See <a href="https://chatmail.at/doc/relay">the relay documentation</a>
and <a href="https://delta.chat">delta.chat</a>
and <a href="https://github.com/chatmail/relay">the repository</a>.</p>
</body></html>
"""


def test_parse_relay_list_extracts_relay_domains():
    relays = parse_relay_list(PAGE)
    assert relays == [
        "nine.testrun.org",
        "mehl.cloud",
        "chat.adminforge.de",
        "mailchat.pl",
    ]


def test_parse_relay_list_empty_page():
    assert parse_relay_list("<html><body>nothing here</body></html>") == []


def test_parse_relay_list_dcaccount_links():
    page = (
        '<a href="dcaccount:https://e2e.sus.fr/new">Join e2e.sus.fr</a>'
        '<a href="dcaccount:https://jp.deltachat.me/new">whatever text</a>'
        '<a href="https://real.example">real.example</a>'
    )
    assert parse_relay_list(page) == ["e2e.sus.fr", "jp.deltachat.me", "real.example"]


def test_parse_relay_list_excludes_link_hubs():
    page = (
        '<a href="https://chat.sus.fr">chat.sus.fr</a>'
        '<a href="https://real.example">real.example</a>'
    )
    assert parse_relay_list(page) == ["real.example"]


def test_parse_relay_list_ignores_duplicates():
    page = (
        '<a href="https://a.example">a.example</a>'
        '<a href="https://a.example">a.example</a>'
    )
    assert parse_relay_list(page) == ["a.example"]
