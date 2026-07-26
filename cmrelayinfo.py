"""
cmrelayinfo: report chatmail relay capabilities.

For each relay, cmrelayinfo logs in with a cached chat profile
(created once per relay, then reused) and reports what the relay
advertises to clients:

- iroh relay URL (webxdc realtime channels), IMAP METADATA
- TURN server (calls), IMAP METADATA
- maxsmtprecipients, IMAP METADATA
- storage quota (GETQUOTAROOT)

The relay list comes from scraping https://chatmail.at/relays (cached),
or from relay domains (or IP addresses) given on the command line.
"""

import argparse
import contextlib
import imaplib
import ipaddress
import json
import logging
import os
import random
import re
import smtplib
import socket
import ssl
import string
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone

from deltachat_rpc_client import DeltaChat, Rpc
from xdg_base_dirs import xdg_cache_home

__version__ = "0.1.0"

RELAYS_URL = "https://chatmail.at/relays"
RELAY_LIST_TTL = 24 * 3600  # seconds

METADATA_KEYS = [
    "/shared/comment",
    "/shared/admin",
    "/shared/vendor/deltachat/irohrelay",
    "/shared/vendor/deltachat/turn",
    "/shared/vendor/deltachat/maxsmtprecipients",
]

NOTEXISTS = "NOTEXISTS"

# seconds for probing iroh relay and TURN reachability
SERVICE_TIMEOUT = 8

# domains on the relay list page that are not relays themselves
NON_RELAY_HOSTS = {
    "chatmail.at",
    "www.chatmail.at",
    "delta.chat",
    "support.delta.chat",
    "github.com",
    "codeberg.org",
    # chat.sus.fr is a link hub pointing to further chatmail relays,
    # not a relay itself; it is followed separately, see HUB_URLS
    "chat.sus.fr",
}

# link hubs listing further relays as dcaccount: links
HUB_URLS = [
    "https://chat.sus.fr/",
]


def log(msg):
    print(msg, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# relay list scraping and caching
# ---------------------------------------------------------------------------


def get_cache_dir():
    d = xdg_cache_home().joinpath("cmrelayinfo")
    d.mkdir(parents=True, exist_ok=True)
    return d


def fetch_url(url, timeout=30):
    """GET a URL (following redirects) and return the decoded body."""
    req = urllib.request.Request(
        url, headers={"User-Agent": f"cmrelayinfo/{__version__}"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def parse_relay_list(html_text):
    """Extract relay domains from the chatmail.at/relays page.

    Relay entries link their own domain and use the domain as link text.
    """
    from html.parser import HTMLParser

    links = []

    class LinkParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self._href = None
            self._text = []

        def handle_starttag(self, tag, attrs):
            if tag == "a":
                self._href = dict(attrs).get("href")
                self._text = []

        def handle_data(self, data):
            if self._href is not None:
                self._text.append(data)

        def handle_endtag(self, tag):
            if tag == "a" and self._href is not None:
                links.append((self._href, "".join(self._text).strip()))
                self._href = None

    LinkParser().feed(html_text)

    domains = []
    for href, text in links:
        if href.startswith("dcaccount:"):
            # account creation links like dcaccount:https://relay.example/new
            href = href[len("dcaccount:") :]
            text = None
        try:
            url = urllib.parse.urlparse(href)
        except ValueError:
            continue
        host = (url.netloc or "").lower()
        if url.scheme not in ("http", "https") or not host:
            continue
        if text is not None and host != text.lower():
            continue
        if host in NON_RELAY_HOSTS or host in domains:
            continue
        domains.append(host)
    return domains


def get_relay_list(refresh=False, verbose=0):
    """Return (relays, via) where via maps a domain to the hub it came from."""
    cache_path = get_cache_dir().joinpath("relays.json")
    cached = None
    if cache_path.exists():
        with contextlib.suppress(Exception):
            cached = json.loads(cache_path.read_text())
    if cached and not refresh:
        age = time.time() - cached.get("fetched_at", 0)
        if age < RELAY_LIST_TTL:
            if verbose:
                log(f"# relay list from cache ({int(age)}s old): {cache_path}")
            relays = [r for r in cached["relays"] if r not in NON_RELAY_HOSTS]
            return relays, cached.get("via", {})
    try:
        relays = parse_relay_list(fetch_url(RELAYS_URL))
        if not relays:
            raise ValueError(f"no relay domains parsed from {RELAYS_URL}")
        via = {}
        for hub_url in HUB_URLS:
            hub_host = urllib.parse.urlparse(hub_url).netloc
            try:
                for domain in parse_relay_list(fetch_url(hub_url)):
                    if domain not in relays:
                        relays.append(domain)
                        via[domain] = hub_host
                        if verbose:
                            log(f"# found {domain} via hub {hub_host}")
            except Exception as e:
                log(f"# warning: fetching hub {hub_url} failed ({e})")
        cache_path.write_text(
            json.dumps(
                {"fetched_at": int(time.time()), "relays": relays, "via": via},
                indent=1,
            )
        )
        if verbose:
            log(f"# fetched {len(relays)} relays from {RELAYS_URL}")
        return relays, via
    except Exception as e:
        if cached:
            log(f"# warning: fetching {RELAYS_URL} failed ({e}), using stale cache")
            return cached["relays"], cached.get("via", {})
        raise SystemExit(
            f"could not fetch relay list from {RELAYS_URL}: {e}\n"
            "pass relay domains explicitly instead"
        )


def expand_relay_args(relay_args, verbose=0):
    """Expand explicitly given relays, resolving known hub hosts.

    A hub host like chat.sus.fr is not a relay itself; it is fetched
    and replaced by the relays it links. Returns (relays, via, failed)
    where failed is a list of (hub_host, error) pairs.
    """
    hub_hosts = {urllib.parse.urlparse(u).netloc for u in HUB_URLS}
    relays, via, failed = [], {}, []
    for arg in relay_args:
        host = arg.lower().rstrip("/")
        if host in hub_hosts:
            try:
                found = parse_relay_list(fetch_url(f"https://{host}/"))
            except Exception as e:
                failed.append((host, f"hub fetch failed: {short_neterr(e)}"))
                continue
            if not found:
                failed.append((host, "hub lists no relays"))
                continue
            for domain in found:
                if domain not in relays:
                    relays.append(domain)
                    via[domain] = host
                    if verbose:
                        log(f"# found {domain} via hub {host}")
        elif host not in relays:
            relays.append(host)
    return relays, via, failed


# ---------------------------------------------------------------------------
# account setup (one cached profile per relay, cmping style)
# ---------------------------------------------------------------------------


def is_ip_address(host):
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def create_qr_url(domain_or_ip):
    """dcaccount URL for domains, dclogin URL with random credentials for IPs."""
    if is_ip_address(domain_or_ip):
        chars = string.ascii_lowercase + string.digits
        username = "".join(random.choices(chars, k=12))
        password = "".join(random.choices(chars, k=20))
        encoded_password = urllib.parse.quote(password, safe="")
        return (
            f"dclogin:{username}@{domain_or_ip}/?"
            f"p={encoded_password}&v=1&ip=993&sp=465&ic=3&ss=default"
        )
    return f"dcaccount:{domain_or_ip}"


def get_relay_account(dc, relay, verbose=0):
    """Return (account, created) for the relay, reusing cached profiles."""
    accounts = dc.get_all_accounts()
    for account in accounts:
        addr = account.get_config("configured_addr")
        if addr and "@" in addr and addr.split("@")[1] == relay:
            if verbose:
                log(f"# {relay}: reusing cached profile {addr}")
            return account, False

    # Reuse a leftover unconfigured account before creating a new one.
    # Note that retrying credentials from a previously failed setup is
    # not possible: core only persists the transport after a successful
    # configuration, so a failed relay costs one fresh registration per
    # run for as long as it stays broken.
    account = None
    for candidate in accounts:
        if not candidate.get_config("configured_addr"):
            account = candidate
            break
    if account is None:
        account = dc.add_account()
    if verbose:
        log(f"# {relay}: creating new profile")
    account.set_config_from_qr(create_qr_url(relay))
    account.configure()
    if verbose:
        log(f"# {relay}: configured as {account.get_config('configured_addr')}")
    return account, True


# ---------------------------------------------------------------------------
# IMAP METADATA and QUOTA
# ---------------------------------------------------------------------------


def _tokenize(segment):
    """Tokenize a piece of an IMAP response line.

    Yields ("atom", s), ("quoted", s) and ignores parentheses.
    Literal markers {n} at line ends are handled by the caller.
    """
    tokens = []
    i = 0
    n = len(segment)
    while i < n:
        c = segment[i : i + 1]
        if c in b" \r\n":
            i += 1
        elif c in b"()":
            i += 1
        elif c == b'"':
            i += 1
            out = bytearray()
            while i < n and segment[i : i + 1] != b'"':
                if segment[i : i + 1] == b"\\" and i + 1 < n:
                    i += 1
                out += segment[i : i + 1]
                i += 1
            i += 1
            tokens.append(("quoted", out.decode("utf-8", "replace")))
        else:
            j = i
            while j < n and segment[j : j + 1] not in b' ()"\r\n':
                j += 1
            tokens.append(("atom", segment[i:j].decode("utf-8", "replace")))
            i = j
    return tokens


def read_metadata_response(readline, read, tag):
    """Read raw IMAP lines until the tagged completion, handling literals.

    Returns (status_line, tokens).
    """
    tokens = []
    while True:
        line = readline()
        if not line:
            raise ConnectionError("connection closed while reading METADATA response")
        if line.startswith(tag):
            return line.decode("utf-8", "replace").strip(), tokens
        while True:
            m = re.search(rb"\{(\d+)\}\r?\n$", line)
            if not m:
                break
            tokens.extend(_tokenize(line[: m.start()]))
            literal = read(int(m.group(1)))
            tokens.append(("literal", literal.decode("utf-8", "replace")))
            line = readline()
        tokens.extend(_tokenize(line))


def parse_metadata_tokens(tokens):
    """Pair up /key value tokens from a METADATA response token stream."""
    result = {}
    i = 0
    while i < len(tokens):
        kind, value = tokens[i]
        if kind == "atom" and value.startswith("/"):
            if i + 1 < len(tokens):
                vkind, vvalue = tokens[i + 1]
                if vkind == "atom" and vvalue.startswith("/"):
                    i += 1
                    continue
                if vkind == "atom" and vvalue == "NIL":
                    result[value] = None
                else:
                    result[value] = vvalue
                i += 2
                continue
        i += 1
    return result


def fetch_imap_info(host, port, user, password, timeout, verbose=0):
    """Log in via IMAP and return (metadata_dict, quota_dict_or_None)."""
    if verbose:
        log(f"# imap connect {host}:{port} as {user}")
    conn = imaplib.IMAP4_SSL(host, port, timeout=timeout)
    try:
        conn.login(user, password)
        tag = conn._new_tag()
        keys = " ".join(METADATA_KEYS)
        conn.send(tag + f' GETMETADATA "" ({keys})'.encode("ascii") + b"\r\n")
        status_line, tokens = read_metadata_response(conn.readline, conn.read, tag)
        metadata = parse_metadata_tokens(tokens)
        if " OK " not in status_line and not status_line.endswith("OK"):
            if verbose:
                log(f"# GETMETADATA failed: {status_line}")
            metadata = {}

        quota = None
        with contextlib.suppress(Exception):
            typ, data = conn.getquotaroot("INBOX")
            if typ == "OK":
                for item in data[1] or []:
                    if isinstance(item, bytes):
                        m = re.search(rb"STORAGE (\d+) (\d+)", item)
                        if m:
                            quota = {
                                "storage_used_kb": int(m.group(1)),
                                "storage_limit_kb": int(m.group(2)),
                            }
        with contextlib.suppress(Exception):
            conn.logout()
        return metadata, quota
    except Exception:
        with contextlib.suppress(Exception):
            conn.shutdown()
        raise


# ---------------------------------------------------------------------------
# service reachability checks
# ---------------------------------------------------------------------------


def short_neterr(e):
    """Map network exceptions to a compact reason string."""
    if isinstance(e, (socket.timeout, TimeoutError)):
        return "timeout"
    if isinstance(e, socket.gaierror):
        return "dns"
    if isinstance(e, ConnectionRefusedError):
        return "refused"
    if isinstance(e, ssl.SSLError):
        return "tls"
    if isinstance(e, urllib.error.URLError) and isinstance(e.reason, Exception):
        return short_neterr(e.reason)
    text = str(e) or e.__class__.__name__
    return text[:16]


def check_iroh_relay(url, timeout=SERVICE_TIMEOUT):
    """Probe the iroh relay behind the advertised URL.

    cmdeploy proxies /relay/probe and /generate_204 to iroh-relay.
    Returns (ok, short_error_or_None).
    """
    last_error = "unreachable"
    for path in ("/relay/probe", "/generate_204"):
        try:
            req = urllib.request.Request(
                url.rstrip("/") + path,
                headers={"User-Agent": f"cmrelayinfo/{__version__}"},
            )
            with urllib.request.urlopen(req, timeout=timeout):
                return True, None
        except urllib.error.HTTPError as e:
            last_error = f"http {e.code}"
        except Exception as e:
            return False, short_neterr(e)
    return False, last_error


def check_turn(host, port, timeout=SERVICE_TIMEOUT, tries=2):
    """Send a STUN binding request (RFC 5389) to the TURN server via UDP.

    Returns (ok, short_error_or_None).
    """
    transaction_id = random.randbytes(12)
    request = struct.pack("!HHI", 0x0001, 0, 0x2112A442) + transaction_id
    try:
        family, socktype, proto, _, addr = socket.getaddrinfo(
            host, port, type=socket.SOCK_DGRAM
        )[0]
        for attempt in range(tries):
            with socket.socket(family, socktype, proto) as sock:
                sock.settimeout(timeout / tries)
                sock.sendto(request, addr)
                try:
                    data, _ = sock.recvfrom(2048)
                except (socket.timeout, TimeoutError):
                    continue
            if (
                len(data) >= 20
                and data[0:2] == b"\x01\x01"
                and data[8:20] == transaction_id
            ):
                return True, None
            return False, "bad stun reply"
        return False, "timeout"
    except Exception as e:
        return False, short_neterr(e)


def fetch_smtp_max_size(host, port=465, timeout=SERVICE_TIMEOUT):
    """Read the SIZE capability from the SMTP EHLO response.

    The max message size is not in IMAP METADATA; relays advertise it
    as postfix message_size_limit via the standard ESMTP SIZE extension.
    Returns (size_in_bytes_or_None, short_error_or_None).
    """
    try:
        with smtplib.SMTP_SSL(host, port, timeout=timeout) as smtp:
            smtp.ehlo()
            size = smtp.esmtp_features.get("size")
        if size:
            return int(size), None
        return None, None
    except Exception as e:
        return None, short_neterr(e)


# ---------------------------------------------------------------------------
# per relay query
# ---------------------------------------------------------------------------


@dataclass
class RelayResult:
    relay: str
    via: str = None  # hub the relay was discovered through
    ok: bool = False
    error: str = None
    profile_created: bool = None  # None: no profile was set up at all
    iroh_relay: str = NOTEXISTS
    iroh_ok: bool = False
    iroh_error: str = None
    turn: dict = None
    turn_ok: bool = False
    turn_error: str = None
    maxsmtprecipients: str = NOTEXISTS
    max_message_size: int = None
    max_message_error: str = None
    quota: dict = None
    comment: str = None
    admin: str = None
    fetched_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )


def format_error(exc):
    """Reduce an exception to one short human readable line."""
    message = None
    if exc.args and isinstance(exc.args[0], dict):
        message = exc.args[0].get("message")
    if message is None:
        message = str(exc)
    # well-known failures are not worth showing in detail
    for pattern, short in (
        ("RPC server failed to start", "can not connect"),
        ("RPC server closed", "can not connect"),
        ("Cannot login", "can not login"),
    ):
        if pattern in message:
            return short
    # rpc errors often start with "Error:" and blank lines
    lines = [ln.strip() for ln in message.splitlines()]
    lines = [ln for ln in lines if ln and ln != "Error:"]
    message = lines[0] if lines else exc.__class__.__name__
    message = message.strip("“”\"")
    if len(message) > 60:
        message = message[:57] + "..."
    return message


def parse_turn_value(value):
    """Parse 'host:port:timestamp:password', never returning the password."""
    parts = value.split(":")
    if len(parts) < 4:
        return None
    host = ":".join(parts[:-3])
    port, ts = parts[-3], parts[-2]
    try:
        return {"host": host, "port": int(port), "expiry": int(ts)}
    except ValueError:
        return None


def relay_label(relay, via):
    return f"{relay} [{via}]" if via else relay


def query_relay(relay, timeout, verbose=0, via=None):
    result = RelayResult(relay=relay, via=via)
    accounts_dir = get_cache_dir().joinpath("accounts", relay)
    accounts_dir.mkdir(parents=True, exist_ok=True)
    try:
        with Rpc(accounts_dir=str(accounts_dir)) as rpc:
            dc = DeltaChat(rpc)
            account, result.profile_created = get_relay_account(
                dc, relay, verbose=verbose
            )
            transports = rpc.list_transports(account.id)
            if not transports:
                raise ValueError("no transport configured in profile")
            transport = transports[0]
            user = transport["addr"]
            password = transport["password"]
            host = transport["imapServer"] or relay
            port = int(transport["imapPort"] or 993)
            smtp_host = transport["smtpServer"] or relay
            smtp_port = int(transport["smtpPort"] or 465)
        if not user or not password:
            raise ValueError("no configured credentials in profile")

        metadata, quota = fetch_imap_info(
            host, port, user, password, timeout, verbose=verbose
        )

        iroh = metadata.get("/shared/vendor/deltachat/irohrelay")
        result.iroh_relay = iroh if iroh else NOTEXISTS
        turn_raw = metadata.get("/shared/vendor/deltachat/turn")
        result.turn = parse_turn_value(turn_raw) if turn_raw else None
        maxrcpt = metadata.get("/shared/vendor/deltachat/maxsmtprecipients")
        result.maxsmtprecipients = maxrcpt if maxrcpt else NOTEXISTS
        result.comment = metadata.get("/shared/comment")
        result.admin = metadata.get("/shared/admin")
        result.quota = quota

        if result.iroh_relay != NOTEXISTS:
            if verbose:
                log(f"# {relay}: probing iroh relay {result.iroh_relay}")
            result.iroh_ok, result.iroh_error = check_iroh_relay(result.iroh_relay)
        if result.turn:
            if verbose:
                log(f"# {relay}: stun probing {result.turn['host']}:{result.turn['port']}")
            result.turn_ok, result.turn_error = check_turn(
                result.turn["host"], result.turn["port"]
            )
        if verbose:
            log(f"# {relay}: reading SMTP SIZE from {smtp_host}:{smtp_port}")
        result.max_message_size, result.max_message_error = fetch_smtp_max_size(
            smtp_host, smtp_port
        )
        result.ok = True
    except Exception as e:
        result.error = format_error(e)
    return result


# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------


def clip(text, limit):
    if len(text) <= limit:
        return text
    return text[: limit - 2] + ".."


def format_service(exists, ok, error):
    """Compact cell: YES, NOTEXISTS, or a short ERROR."""
    if not exists:
        return NOTEXISTS
    if ok:
        return "YES"
    return f"ERROR: {clip(error or 'unreachable', 8)}"


def format_admin(admin):
    if not admin:
        return NOTEXISTS
    return clip(admin.removeprefix("mailto:"), ADMIN_WIDTH)


def format_max_message(size, error):
    if size:
        return f"{size / (1024 * 1024):.0f}MB"
    if error:
        return f"ERROR: {clip(error, 8)}"
    return NOTEXISTS


def format_quota(quota):
    """Show only the storage limit; the probe accounts are always empty,
    so displaying usage would be noise. Usage stays in the JSON output."""
    if not quota:
        return NOTEXISTS
    return f"{quota['storage_limit_kb'] / 1024:.0f}MB"


TABLE_HEADER = [
    "RELAY",
    "IROH",
    "TURN",
    "MAXRCPT",
    "MAXMSG",
    "QUOTA",
    "ADMIN",
]

ADMIN_WIDTH = 26


def table_widths(relays):
    """Column widths precomputed upfront, so rows can stream out
    as soon as each result is in, without collecting them first."""
    longest_relay = max((len(r) for r in relays), default=0)
    service_width = max(len(NOTEXISTS), len("ERROR: ") + 8)
    return [
        max(len(TABLE_HEADER[0]), longest_relay),
        max(len(TABLE_HEADER[1]), service_width),
        max(len(TABLE_HEADER[2]), service_width),
        max(len(TABLE_HEADER[3]), len(NOTEXISTS)),
        max(len(TABLE_HEADER[4]), len(NOTEXISTS)),
        max(len(TABLE_HEADER[5]), len(NOTEXISTS)),
        max(len(TABLE_HEADER[6]), ADMIN_WIDTH),
    ]


def format_table_row(r, widths):
    label = relay_label(r.relay, r.via)
    if not r.ok:
        return f"{label.ljust(widths[0])}  ERROR: {r.error}"
    cells = [
        label,
        format_service(r.iroh_relay != NOTEXISTS, r.iroh_ok, r.iroh_error),
        format_service(bool(r.turn), r.turn_ok, r.turn_error),
        str(r.maxsmtprecipients),
        format_max_message(r.max_message_size, r.max_message_error),
        format_quota(r.quota),
        format_admin(r.admin),
    ]
    return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells)).rstrip()


def format_json_line(r):
    obj = {"relay": r.relay, "via": r.via, "ok": r.ok, "fetched_at": r.fetched_at}
    if r.ok:
        obj.update(
            {
                "iroh_relay": None if r.iroh_relay == NOTEXISTS else r.iroh_relay,
                "iroh_reachable": r.iroh_ok,
                "iroh_error": r.iroh_error,
                "turn": r.turn,
                "turn_reachable": r.turn_ok,
                "turn_error": r.turn_error,
                "maxsmtprecipients": (
                    None
                    if r.maxsmtprecipients == NOTEXISTS
                    else int(r.maxsmtprecipients)
                ),
                "max_message_size": r.max_message_size,
                "max_message_error": r.max_message_error,
                "quota": r.quota,
                "comment": r.comment,
                "admin": r.admin,
            }
        )
    else:
        obj["error"] = r.error
    return json.dumps(obj)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="cmrelayinfo",
        description="report chatmail relay capabilities (iroh relay, TURN, "
        "maxsmtprecipients, quota)",
    )
    parser.add_argument(
        "relays",
        nargs="*",
        help="relay domains or IP addresses; default: scrape chatmail.at/relays",
    )
    parser.add_argument(
        "--refresh", action="store_true", help="refetch the relay list, ignore cache"
    )
    parser.add_argument("--json", action="store_true", help="output jsonlines")
    parser.add_argument(
        "--timeout", type=int, default=60, help="per relay timeout in seconds"
    )
    parser.add_argument("--jobs", type=int, default=4, help="concurrent relay queries")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args(argv)

    if not args.verbose:
        # deltachat_rpc_client logs tracebacks (e.g. when an rpc server
        # dies) through the root logger; keep them out of normal output
        logging.disable(logging.CRITICAL)

    if args.relays:
        relays, via, failed_hubs = expand_relay_args(args.relays, args.verbose)
    else:
        relays, via = get_relay_list(refresh=args.refresh, verbose=args.verbose)
        failed_hubs = []

    labels = [relay_label(r, via.get(r)) for r in relays]
    labels += [host for host, _ in failed_hubs]
    widths = table_widths(labels)
    if not args.json:
        header = "  ".join(
            h.ljust(widths[i]) for i, h in enumerate(TABLE_HEADER)
        ).rstrip()
        print(header, flush=True)

    all_ok = True
    for host, error in failed_hubs:
        all_ok = False
        result = RelayResult(relay=host, error=error)
        line = format_json_line(result) if args.json else format_table_row(result, widths)
        print(line, flush=True)
    created = reused = 0
    with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        futures = {
            pool.submit(
                query_relay, relay, args.timeout, args.verbose, via.get(relay)
            ): relay
            for relay in relays
        }
        for future in as_completed(futures):
            relay = futures[future]
            try:
                result = future.result(timeout=args.timeout * 2)
            except Exception as e:
                result = RelayResult(
                    relay=relay, via=via.get(relay), error=format_error(e)
                )
            all_ok = all_ok and result.ok
            if result.profile_created is True:
                created += 1
            elif result.profile_created is False:
                reused += 1
            line = (
                format_json_line(result)
                if args.json
                else format_table_row(result, widths)
            )
            print(line, flush=True)

    log(f"# profiles: {created} created, {reused} reused from cache")
    sys.stdout.flush()
    sys.stderr.flush()
    # a failed deltachat-rpc-server can leave non-daemon threads behind
    # which would block normal interpreter exit; leave decisively
    os._exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
