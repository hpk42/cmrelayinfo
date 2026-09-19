import io

from cmrelayinfo import (
    NOTEXISTS,
    format_error,
    format_service,
    parse_metadata_tokens,
    parse_turn_value,
    read_metadata_response,
)


class FakeConn:
    """Feeds pre-recorded IMAP wire data to read_metadata_response."""

    def __init__(self, wire):
        self._buf = io.BytesIO(wire)

    def readline(self):
        return self._buf.readline()

    def read(self, n):
        return self._buf.read(n)


def run_parse(wire, tag=b"ABC1"):
    conn = FakeConn(wire)
    status, tokens = read_metadata_response(conn.readline, conn.read, tag)
    return status, parse_metadata_tokens(tokens)


def test_literal_values_dovecot_style():
    wire = (
        b'* METADATA "" (/shared/vendor/deltachat/irohrelay {24}\r\n'
        b"https://nine.testrun.org"
        b" /shared/vendor/deltachat/turn {44}\r\n"
        b"nine.testrun.org:3478:1758650868:secretpass1"
        b" /shared/vendor/deltachat/maxsmtprecipients {2}\r\n"
        b"50"
        b")\r\n"
        b"ABC1 OK Getmetadata completed\r\n"
    )
    status, meta = run_parse(wire)
    assert "OK" in status
    assert meta["/shared/vendor/deltachat/irohrelay"] == "https://nine.testrun.org"
    assert meta["/shared/vendor/deltachat/turn"] == (
        "nine.testrun.org:3478:1758650868:secretpass1"
    )
    assert meta["/shared/vendor/deltachat/maxsmtprecipients"] == "50"


def test_quoted_and_nil_values():
    wire = (
        b'* METADATA "" (/shared/comment "hello world" /shared/admin NIL)\r\n'
        b"ABC1 OK done\r\n"
    )
    status, meta = run_parse(wire)
    assert meta["/shared/comment"] == "hello world"
    assert meta["/shared/admin"] is None


def test_missing_keys_yield_empty():
    wire = b'* METADATA "" ()\r\nABC1 OK done\r\n'
    status, meta = run_parse(wire)
    assert meta == {}


def test_multiline_literal_with_crlf_inside():
    text = b"line one\r\nline two"
    wire = (
        b'* METADATA "" (/shared/comment {%d}\r\n' % len(text)
        + text
        + b")\r\n"
        + b"ABC1 OK done\r\n"
    )
    status, meta = run_parse(wire)
    assert meta["/shared/comment"] == "line one\r\nline two"


def test_entries_kept_when_server_answers_no():
    wire = (
        b'* METADATA "" (/shared/vendor/deltachat/irohrelay {22}\r\n'
        b"https://chat.nuvon.app)\r\n"
        b"ABC1 NO [SERVERBUG] Internal error occurred\r\n"
    )
    status, meta = run_parse(wire)
    assert meta["/shared/vendor/deltachat/irohrelay"] == "https://chat.nuvon.app"


def test_parse_turn_value():
    turn = parse_turn_value("nine.testrun.org:3478:1758650868:secretpass1")
    assert turn == {"host": "nine.testrun.org", "port": 3478, "expiry": 1758650868}
    assert "secretpass1" not in str(turn)
    assert parse_turn_value("garbage") is None


def test_parse_turn_value_password_with_colons():
    # some relays hand out a realm-prefixed password, which core keeps
    # whole because it splits off only the first three fields
    turn = parse_turn_value("relay.example:3478:1790235107:chatmail:secretpass2=")
    assert turn == {"host": "relay.example", "port": 3478, "expiry": 1790235107}
    assert "secretpass2" not in str(turn)


def test_parse_turn_value_rejects_what_core_rejects():
    # core parses the port with u16::from_str and the expiry with
    # i64::from_str, so neither a port out of range nor a
    # non-numeric expiry yields a TURN server
    assert parse_turn_value("host:99999:1758650868:pass") is None
    assert parse_turn_value("host:3478:notanumber:pass") is None
    assert parse_turn_value("host:3478:1758650868") is None


def test_notexists_constant():
    assert NOTEXISTS == "NOTEXISTS"


def test_format_error_rpc_dict():
    exc = ValueError(
        {
            "code": -1,
            "message": "Error:\n\n“IMAP failed to connect to imap.x.example:993:"
            "tls: Could not find DNS resolutions”",
        }
    )
    msg = format_error(exc)
    assert msg.startswith("IMAP failed to connect")
    assert "\n" not in msg


def test_format_error_plain_exception():
    assert format_error(OSError("network down")) == "network down"


def test_format_error_truncates():
    msg = format_error(ValueError("x" * 300))
    assert len(msg) <= 60


def test_format_error_login_failure():
    exc = ValueError(
        {
            "code": -1,
            "message": 'Cannot login as "x@y.example". Please check if the '
            "email address and the password are correct.",
        }
    )
    assert format_error(exc) == "can not login"


def test_format_error_rpc_machinery():
    exc = ValueError(
        {"code": -32000, "message": "RPC server closed"},
    )
    assert format_error(exc) == "can not connect"
    exc2 = RuntimeError("RPC server failed to start: 2026-07-26 ERROR blah\nmore")
    assert format_error(exc2) == "can not connect"


def test_format_service_cells():
    assert format_service(False, False, None) == NOTEXISTS
    assert format_service(True, True, None) == "YES"
    assert format_service(True, False, "timeout") == "ERROR: timeout"
    long = format_service(True, False, "x" * 50)
    assert len(long) <= len("ERROR: ") + 8


def test_format_max_message():
    from cmrelayinfo import format_max_message

    assert format_max_message(31457280, None) == "30MB"
    assert format_max_message(None, None) == NOTEXISTS
    assert format_max_message(None, "timeout") == "ERROR: timeout"


def test_format_admin():
    from cmrelayinfo import format_admin

    assert format_admin(None) == NOTEXISTS
    assert format_admin("mailto:root@nine.testrun.org") == "root@nine.testrun.org"
    assert len(format_admin("a" * 60)) <= 26
