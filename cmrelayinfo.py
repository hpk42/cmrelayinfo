"""
cmrelayinfo: report chatmail relay capabilities.

For each relay, log in with a cached chat profile (created once,
then reused) and report what the relay advertises to clients:
iroh relay URL, TURN server and maxsmtprecipients (IMAP METADATA),
storage quota (GETQUOTAROOT) and max message size (ESMTP SIZE).
The relay list comes from scraping https://chatmail.at/relays
(cached), or from domains/IPs given on the command line.
"""

import argparse
import contextlib
import errno
import http.client
import imaplib
import ipaddress
import json
import logging
import os
import random
import re
import selectors
import shutil
import smtplib
import socket
import ssl
import string
import struct
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from importlib.metadata import PackageNotFoundError, version

from deltachat_rpc_client import DeltaChat, Rpc
from xdg_base_dirs import xdg_cache_home

try:
    # the version comes from the git tag the release was built from
    __version__ = version("cmrelayinfo")
except PackageNotFoundError:  # running from a source checkout
    __version__ = "0.0.0"

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

# seconds for the IMAP login, METADATA and QUOTA round trips
IMAP_TIMEOUT = 15

# how long one connect attempt gets before the next address is raced
CONNECT_STAGGER = 0.25

# domains on the relay list page that are not relays themselves;
# chat.sus.fr is a link hub pointing to further relays, see HUB_URLS
NON_RELAY_HOSTS = {
    "chatmail.at",
    "www.chatmail.at",
    "delta.chat",
    "support.delta.chat",
    "github.com",
    "codeberg.org",
    "chat.sus.fr",
}

# link hubs listing further relays as dcaccount: links
HUB_URLS = [
    "https://chat.sus.fr/",
]


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def budget(deadline, cap):
    """Seconds one probe may take, never past the global deadline."""
    return max(0.5, min(cap, deadline - time.monotonic()))


# --- connecting


def happy_connect(host, port, timeout, stagger=CONNECT_STAGGER):
    """Connect to whichever resolved address answers first, RFC 8305 style.

    socket.create_connection() instead tries them strictly
    in order and burns the whole timeout on a black holed AAAA record.
    """
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    deadline = time.monotonic() + timeout
    selector = selectors.DefaultSelector()
    pending = set()
    last_error = None
    next_attempt = 0.0
    try:
        while True:
            now = time.monotonic()
            if infos and now >= next_attempt:
                family, socktype, proto, _, addr = infos.pop(0)
                sock = socket.socket(family, socktype, proto)
                sock.setblocking(False)
                code = sock.connect_ex(addr)
                if code in (0, errno.EINPROGRESS, errno.EWOULDBLOCK):
                    selector.register(sock, selectors.EVENT_WRITE)
                    pending.add(sock)
                else:
                    last_error = OSError(code, os.strerror(code))
                    sock.close()
                next_attempt = now + stagger
                continue
            if not pending and not infos:
                raise last_error or OSError(f"no address for {host}")
            wait = deadline - now
            if wait <= 0:
                raise TimeoutError(f"connecting to {host}:{port} timed out")
            if infos:
                wait = min(wait, max(0.0, next_attempt - now))
            for key, _mask in selector.select(wait):
                sock = key.fileobj
                selector.unregister(sock)
                pending.discard(sock)
                code = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if code:
                    last_error = OSError(code, os.strerror(code))
                    sock.close()
                    continue
                sock.setblocking(True)
                sock.settimeout(timeout)
                return sock
    finally:
        selector.close()
        for sock in pending:
            sock.close()


def happy_http_connection(connection_class):
    """http.client connection factory connecting via happy_connect()."""

    def make(*args, **kwargs):
        connection = connection_class(*args, **kwargs)
        connection._create_connection = lambda address, timeout, _source: happy_connect(
            address[0], address[1], timeout
        )
        return connection

    return make


class _HTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, req):
        return self.do_open(happy_http_connection(http.client.HTTPConnection), req)


class _HTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(
            happy_http_connection(http.client.HTTPSConnection),
            req,
            context=self._context,
        )


_opener = urllib.request.build_opener(_HTTPHandler(), _HTTPSHandler())


# --- relay list scraping and caching


def get_cache_dir():
    d = xdg_cache_home().joinpath("cmrelayinfo")
    d.mkdir(parents=True, exist_ok=True)
    return d


def fetch_url(url, timeout=30):
    """GET a URL (following redirects) and return the decoded body."""
    req = urllib.request.Request(
        url, headers={"User-Agent": f"cmrelayinfo/{__version__}"}
    )
    with _opener.open(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


class _LinkParser(HTMLParser):
    """Collect (href, link text) pairs of all links in a page."""

    def __init__(self):
        super().__init__()
        self.links = []
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
            self.links.append((self._href, "".join(self._text).strip()))
            self._href = None


def parse_relay_list(html_text):
    """Extract relay domains: links using their own domain as link
    text, plus dcaccount: account creation links."""
    parser = _LinkParser()
    parser.feed(html_text)
    domains = []
    for raw_href, raw_text in parser.links:
        href, text = raw_href, raw_text
        if href.startswith("dcaccount:"):
            href, text = href.removeprefix("dcaccount:"), None
        try:
            url = urllib.parse.urlparse(href)
        except ValueError:
            continue
        host = (url.netloc or "").lower()
        if url.scheme not in ("http", "https") or not host:
            continue
        if text is not None and host != text.lower():
            continue
        if host not in NON_RELAY_HOSTS and host not in domains:
            domains.append(host)
    return domains


def get_relay_list(refresh=False, verbose=0, timeout=30):
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
    log(f"# fetching relay list from {RELAYS_URL} ...")
    # the hubs are fetched next to the main list instead of after it:
    # one slow hub used to hold back the whole run
    # before a single row could be printed
    pool = ThreadPoolExecutor(max_workers=1 + len(HUB_URLS))
    try:
        main = pool.submit(fetch_url, RELAYS_URL, timeout)
        hubs = [(url, pool.submit(fetch_url, url, timeout)) for url in HUB_URLS]
        relays = parse_relay_list(main.result())
        if not relays:
            raise ValueError(f"no relay domains parsed from {RELAYS_URL}")
        via = {}
        for hub_url, hub_page in hubs:
            hub_host = urllib.parse.urlparse(hub_url).netloc
            try:
                found = parse_relay_list(hub_page.result())
            except Exception as e:
                log(f"# warning: fetching hub {hub_url} failed ({e})")
                continue
            for domain in found:
                if domain not in relays:
                    relays.append(domain)
                    via[domain] = hub_host
                    if verbose:
                        log(f"# found {domain} via hub {hub_host}")
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
    finally:
        pool.shutdown(wait=False)


def expand_relay_args(relay_args, verbose=0, timeout=30):
    """Expand explicitly given relays, replacing known hub hosts by the
    relays they link. Returns (relays, via, failed_hub_pairs)."""
    hub_hosts = {urllib.parse.urlparse(u).netloc for u in HUB_URLS}
    relays, via, failed = [], {}, []
    for arg in relay_args:
        host = arg.lower().rstrip("/")
        if host in hub_hosts:
            try:
                found = parse_relay_list(fetch_url(f"https://{host}/", timeout))
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


# --- account setup (one cached profile per relay, cmping style)


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
        password = urllib.parse.quote("".join(random.choices(chars, k=20)), safe="")
        return (
            f"dclogin:{username}@{domain_or_ip}/?"
            f"p={password}&v=1&ip=993&sp=465&ic=3&ss=default"
        )
    return f"dcaccount:{domain_or_ip}"


def get_relay_account(dc, relay, verbose=0):
    """Return (account, created) for the relay, reusing cached profiles."""
    accounts = dc.get_all_accounts()
    for account in accounts:
        addr = account.get_config("configured_addr") or ""
        if addr.endswith(f"@{relay}"):
            if verbose:
                log(f"# {relay}: reusing cached profile {addr}")
            return account, False
    # reuse a leftover unconfigured account before creating a new one;
    # retrying credentials from a failed setup is impossible as core
    # only persists the transport after a successful configuration
    account = (
        next((a for a in accounts if not a.get_config("configured_addr")), None)
        or dc.add_account()
    )
    if verbose:
        log(f"# {relay}: creating new profile")
    account.set_config_from_qr(create_qr_url(relay))
    account.configure()
    if verbose:
        log(f"# {relay}: configured as {account.get_config('configured_addr')}")
    return account, True


# --- IMAP METADATA and QUOTA


class _IMAP4_SSL(imaplib.IMAP4_SSL):
    """IMAP4_SSL connecting via happy_connect()."""

    def _create_socket(self, timeout):
        sock = happy_connect(self.host, self.port, timeout)
        return self.ssl_context.wrap_socket(sock, server_hostname=self.host)


_TOKEN_RE = re.compile(rb'"((?:\\.|[^"\\])*)"|([^ ()"\r\n]+)')


def _tokenize(segment):
    """Return ("atom", s) / ("quoted", s) tokens, ignoring parentheses.
    Literal markers {n} at line ends are handled by the caller."""
    tokens = []
    for m in _TOKEN_RE.finditer(segment):
        if m[2] is not None:
            tokens.append(("atom", m[2].decode("utf-8", "replace")))
        else:
            unescaped = re.sub(rb"\\(.)", rb"\1", m[1])
            tokens.append(("quoted", unescaped.decode("utf-8", "replace")))
    return tokens


def read_metadata_response(readline, read, tag):
    """Read raw IMAP lines until the tagged completion, handling literals.
    Returns (status_line, tokens)."""
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
            tokens.append(("literal", read(int(m.group(1))).decode("utf-8", "replace")))
            line = readline()
        tokens.extend(_tokenize(line))


def parse_metadata_tokens(tokens):
    """Pair up /key value tokens from a METADATA response token stream."""
    result = {}
    i = 0
    while i + 1 < len(tokens):
        kind, key = tokens[i]
        vkind, value = tokens[i + 1]
        if kind != "atom" or not key.startswith("/"):
            i += 1
        elif vkind == "atom" and value.startswith("/"):
            i += 1
        else:
            result[key] = None if (vkind, value) == ("atom", "NIL") else value
            i += 2
    return result


def fetch_imap_info(host, port, user, password, timeout, verbose=0):
    """Log in via IMAP and return (metadata_dict, quota_dict_or_None)."""
    if verbose:
        log(f"# imap connect {host}:{port} as {user}")
    conn = _IMAP4_SSL(host, port, timeout=timeout)
    try:
        conn.login(user, password)
        tag = conn._new_tag()
        keys = " ".join(METADATA_KEYS)
        conn.send(tag + f' GETMETADATA "" ({keys})'.encode("ascii") + b"\r\n")
        # the tagged status is ignored on purpose, mirroring core/async-imap
        _status_line, tokens = read_metadata_response(conn.readline, conn.read, tag)
        metadata = parse_metadata_tokens(tokens)

        quota = None
        with contextlib.suppress(Exception):
            typ, data = conn.getquotaroot("INBOX")
            blob = b" ".join(x for x in (data[1] or []) if isinstance(x, bytes))
            m = re.search(rb"STORAGE (\d+) (\d+)", blob) if typ == "OK" else None
            if m:
                quota = {"storage_used_kb": int(m[1]), "storage_limit_kb": int(m[2])}
        with contextlib.suppress(Exception):
            conn.logout()
        return metadata, quota
    except Exception:
        with contextlib.suppress(Exception):
            conn.shutdown()
        raise


# --- service reachability checks


def short_neterr(e):
    """Map a network exception to a compact reason string."""
    if isinstance(e, urllib.error.URLError) and isinstance(e.reason, Exception):
        return short_neterr(e.reason)
    for types, short in (
        ((socket.timeout, TimeoutError), "timeout"),
        (socket.gaierror, "dns"),
        (ConnectionRefusedError, "refused"),
        (ssl.SSLError, "tls"),
    ):
        if isinstance(e, types):
            return short
    return (str(e) or e.__class__.__name__)[:16]


def check_iroh_relay(url, timeout=SERVICE_TIMEOUT):
    """Probe the advertised iroh relay via /generate_204, which core
    probes and which both iroh-relay 0.35 and the 1.0 line serve."""
    try:
        req = urllib.request.Request(
            url.rstrip("/") + "/generate_204",
            headers={"User-Agent": f"cmrelayinfo/{__version__}"},
        )
        with _opener.open(req, timeout=timeout):
            return True, None
    except urllib.error.HTTPError as e:
        return False, f"http {e.code}"
    except Exception as e:
        return False, short_neterr(e)


def check_turn(host, port, timeout=SERVICE_TIMEOUT, tries=2):
    """Send a STUN binding request (RFC 5389) to the TURN server via UDP."""
    transaction_id = random.randbytes(12)
    request = struct.pack("!HHI", 0x0001, 0, 0x2112A442) + transaction_id
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_DGRAM)
    except Exception as e:
        return False, short_neterr(e)
    # every address gets its tries:
    # a TURN host may well publish an AAAA record that goes nowhere.
    # UDP has no connect to race, so the attempts share the timeout instead
    attempts = [info for info in infos for _ in range(tries)]
    per_try = timeout / max(1, len(attempts))
    for family, socktype, proto, _, addr in attempts:
        try:
            with socket.socket(family, socktype, proto) as sock:
                sock.settimeout(per_try)
                sock.sendto(request, addr)
                data, _ = sock.recvfrom(2048)
        except (socket.timeout, TimeoutError):
            continue
        except Exception as e:
            return False, short_neterr(e)
        if data[0:2] == b"\x01\x01" and data[8:20] == transaction_id:
            return True, None
        return False, "bad stun reply"
    return False, "timeout"


class _SMTP_SSL(smtplib.SMTP_SSL):
    """SMTP_SSL connecting via happy_connect()."""

    def _get_socket(self, host, port, timeout):
        sock = happy_connect(host, port, timeout)
        return self.context.wrap_socket(sock, server_hostname=self._host)


def fetch_smtp_max_size(host, port=465, timeout=SERVICE_TIMEOUT):
    """Read the max message size from the ESMTP SIZE extension; it is
    not in IMAP METADATA but advertised as postfix message_size_limit."""
    try:
        with _SMTP_SSL(host, port, timeout=timeout) as smtp:
            smtp.ehlo()
            size = smtp.esmtp_features.get("size")
        return (int(size), None) if size else (None, None)
    except Exception as e:
        return None, short_neterr(e)


# --- per relay query


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
    message = (lines[0] if lines else exc.__class__.__name__).strip('“”"')
    return message if len(message) <= 60 else message[:57] + "..."


def parse_turn_value(value):
    """Parse 'host:port:timestamp:password', never returning the password.
    Split off only the leading three fields, as core does, so a password
    carrying colons of its own stays in the part we drop."""
    parts = value.split(":", 3)
    if len(parts) != 4:
        return None
    host, port, expiry, _password = parts
    try:
        port, expiry = int(port), int(expiry)
    except ValueError:
        return None
    # core parses the port as u16 and gets no TURN server if it overflows
    if not 0 <= port <= 65535:
        return None
    return {"host": host, "port": port, "expiry": expiry}


def relay_label(relay, via):
    return f"{relay} [{via}]" if via else relay


@dataclass
class Transport:
    """Where to reach a relay and as whom, out of a cached profile."""

    addr: str
    password: str
    imap_host: str
    imap_port: int
    smtp_host: str
    smtp_port: int


def read_profile_transport(relay, verbose=0):
    """Return (transport, profile_created) for the relay."""
    accounts_dir = get_cache_dir().joinpath("accounts", relay)
    accounts_dir.mkdir(parents=True, exist_ok=True)
    with Rpc(accounts_dir=str(accounts_dir)) as rpc:
        account, created = get_relay_account(DeltaChat(rpc), relay, verbose=verbose)
        transports = rpc.list_transports(account.id)
        if not transports:
            raise ValueError("no transport configured in profile")
        transport = transports[0]
        if not transport["addr"] or not transport["password"]:
            raise ValueError("no configured credentials in profile")
        return Transport(
            addr=transport["addr"],
            password=transport["password"],
            imap_host=transport["imapServer"] or relay,
            imap_port=int(transport["imapPort"] or 993),
            smtp_host=transport["smtpServer"] or relay,
            smtp_port=int(transport["smtpPort"] or 465),
        ), created


def apply_metadata(result, metadata, quota):
    """Copy what the relay advertises into the result."""
    result.iroh_relay = metadata.get("/shared/vendor/deltachat/irohrelay") or NOTEXISTS
    turn_raw = metadata.get("/shared/vendor/deltachat/turn")
    result.turn = parse_turn_value(turn_raw) if turn_raw else None
    result.maxsmtprecipients = (
        metadata.get("/shared/vendor/deltachat/maxsmtprecipients") or NOTEXISTS
    )
    result.comment = metadata.get("/shared/comment")
    result.admin = metadata.get("/shared/admin")
    result.quota = quota


def submit_advertised_probes(pool, result, deadline, verbose=0):
    """Start the iroh and TURN probes the METADATA answer just named."""
    iroh_probe = turn_probe = None
    if result.iroh_relay != NOTEXISTS:
        if verbose:
            log(f"# {result.relay}: probing iroh relay {result.iroh_relay}")
        iroh_probe = pool.submit(
            check_iroh_relay, result.iroh_relay, budget(deadline, SERVICE_TIMEOUT)
        )
    if result.turn:
        if verbose:
            log(
                f"# {result.relay}: stun probing "
                f"{result.turn['host']}:{result.turn['port']}"
            )
        turn_probe = pool.submit(
            check_turn,
            result.turn["host"],
            result.turn["port"],
            budget(deadline, SERVICE_TIMEOUT),
        )
    return iroh_probe, turn_probe


def probe_services(result, transport, deadline, verbose=0):
    """Fill in everything the relay has to be asked over the network for."""
    # SMTP knows nothing of IMAP, and neither iroh nor TURN wait
    # for each other, so a relay costs about as long as
    # its slowest probe instead of the sum of all four
    pool = ThreadPoolExecutor(max_workers=3)
    try:
        if verbose:
            log(
                f"# {result.relay}: reading SMTP SIZE from "
                f"{transport.smtp_host}:{transport.smtp_port}"
            )
        smtp_size = pool.submit(
            fetch_smtp_max_size,
            transport.smtp_host,
            transport.smtp_port,
            budget(deadline, SERVICE_TIMEOUT),
        )
        metadata, quota = fetch_imap_info(
            transport.imap_host,
            transport.imap_port,
            transport.addr,
            transport.password,
            budget(deadline, IMAP_TIMEOUT),
            verbose=verbose,
        )
        apply_metadata(result, metadata, quota)

        iroh_probe, turn_probe = submit_advertised_probes(
            pool, result, deadline, verbose=verbose
        )
        if iroh_probe:
            result.iroh_ok, result.iroh_error = iroh_probe.result()
        if turn_probe:
            result.turn_ok, result.turn_error = turn_probe.result()
        result.max_message_size, result.max_message_error = smtp_size.result()
    finally:
        # an error row should not have to wait
        # for probes nobody will read any more
        pool.shutdown(wait=False, cancel_futures=True)


def query_relay(relay, deadline, verbose=0, via=None):
    result = RelayResult(relay=relay, via=via)
    try:
        transport, result.profile_created = read_profile_transport(
            relay, verbose=verbose
        )
        probe_services(result, transport, deadline, verbose=verbose)
        result.ok = True
    except Exception as e:
        result.error = format_error(e)
    return result


# --- output


def clip(text, limit):
    return text if len(text) <= limit else text[: limit - 2] + ".."


def format_service(exists, ok, error):
    """Compact cell: YES, NOTEXISTS, or a short ERROR."""
    if not exists:
        return NOTEXISTS
    return "YES" if ok else f"ERROR: {clip(error or 'unreachable', 8)}"


def format_admin(admin):
    return clip(admin.removeprefix("mailto:"), ADMIN_WIDTH) if admin else NOTEXISTS


def format_max_message(size, error):
    if size:
        return f"{size / (1024 * 1024):.0f}MB"
    return f"ERROR: {clip(error, 8)}" if error else NOTEXISTS


def format_quota(quota):
    # only the limit; the probe accounts are always empty, so showing
    # usage would be noise (it stays in the JSON output)
    return f"{quota['storage_limit_kb'] / 1024:.0f}MB" if quota else NOTEXISTS


# relays are queried all at once up to this many, each of them being little
# more than one short lived deltachat-rpc-server and four network round trips
MAX_JOBS = 32

TABLE_HEADER = ["RELAY", "IROH", "TURN", "MAXRCPT", "MAXMSG", "QUOTA", "ADMIN"]

ADMIN_WIDTH = 26


def table_widths(labels):
    """Column widths precomputed upfront, so rows can stream out
    as soon as each result is in, without collecting them first."""
    service = max(len(NOTEXISTS), len("ERROR: ") + 8)
    longest = max((len(x) for x in labels), default=0)
    ne = len(NOTEXISTS)
    mins = [longest, service, service, ne, ne, ne, ADMIN_WIDTH]
    return [max(len(h), w) for h, w in zip(TABLE_HEADER, mins)]


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


class Progress:
    """Result rows, plus a terminal line naming what is still outstanding."""

    def __init__(self, pending, enabled):
        self.pending = list(pending)
        self.total = len(self.pending)
        self.enabled = enabled and bool(self.pending)
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def start(self):
        if self.enabled:
            threading.Thread(target=self._redraw_loop, daemon=True).start()

    def _redraw_loop(self):
        while not self._stop.wait(0.3):
            with self._lock:
                if self._stop.is_set():
                    return
                self._draw()

    def _draw(self):
        waiting = ", ".join(self.pending[:5])
        if len(self.pending) > 5:
            waiting += f" +{len(self.pending) - 5} more"
        done = self.total - len(self.pending)
        line = f"# {done}/{self.total} done, waiting: {waiting}"
        self._write(line[: shutil.get_terminal_size().columns - 1])

    def _write(self, line=""):
        sys.stderr.write("\r\033[K" + line)
        sys.stderr.flush()

    def emit(self, label, line):
        """Print one result row above the status line."""
        with self._lock:
            if label in self.pending:
                self.pending.remove(label)
            if self.enabled:
                self._write()
            print(line, flush=True)
            if self.enabled and self.pending:
                self._draw()

    def finish(self):
        self._stop.set()
        with self._lock:
            if self.enabled:
                self._write()


# --- main


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
        "--timeout",
        type=float,
        default=30,
        help="wall clock budget for the whole run in seconds (default: 30)",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=0,
        help=f"concurrent relay queries (default: up to {MAX_JOBS})",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args(argv)
    deadline = time.monotonic() + args.timeout

    if not args.verbose:
        # deltachat_rpc_client logs tracebacks (e.g. when an rpc server
        # dies) through the root logger; keep them out of normal output
        logging.disable(logging.CRITICAL)

    if args.relays:
        relays, via, failed_hubs = expand_relay_args(
            args.relays, args.verbose, budget(deadline, args.timeout)
        )
    else:
        relays, via = get_relay_list(
            refresh=args.refresh,
            verbose=args.verbose,
            timeout=budget(deadline, args.timeout),
        )
        failed_hubs = []

    relay_labels = [relay_label(r, via.get(r)) for r in relays]
    widths = table_widths(relay_labels + [host for host, _ in failed_hubs])

    # verbose logging writes full stderr lines,
    # which the status line would only garble
    progress = Progress(relay_labels, enabled=sys.stderr.isatty() and not args.verbose)

    def emit(result):
        line = (
            format_json_line(result) if args.json else format_table_row(result, widths)
        )
        progress.emit(relay_label(result.relay, result.via), line)

    if not args.json:
        header = "  ".join(h.ljust(widths[i]) for i, h in enumerate(TABLE_HEADER))
        print(header.rstrip(), flush=True)

    all_ok = True
    for host, error in failed_hubs:
        all_ok = False
        emit(RelayResult(relay=host, error=error))

    created = reused = 0
    jobs = args.jobs or min(MAX_JOBS, max(1, len(relays)))
    pool = ThreadPoolExecutor(max_workers=jobs)
    futures = {
        pool.submit(query_relay, r, deadline, args.verbose, via.get(r)): r
        for r in relays
    }
    progress.start()
    try:
        for future in as_completed(futures, timeout=budget(deadline, args.timeout)):
            relay = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = RelayResult(
                    relay=relay, via=via.get(relay), error=format_error(e)
                )
            all_ok = all_ok and result.ok
            created += result.profile_created is True
            reused += result.profile_created is False
            emit(result)
    except TimeoutError:
        # the budget is a promise to the caller:
        # report whoever did not make it and stop waiting for them
        for future, relay in futures.items():
            if not future.done():
                all_ok = False
                emit(RelayResult(relay=relay, via=via.get(relay), error="timeout"))
    progress.finish()

    log(f"# profiles: {created} created, {reused} reused from cache")
    sys.stdout.flush()
    sys.stderr.flush()
    # a failed deltachat-rpc-server can leave non-daemon threads behind
    # which would block normal interpreter exit; leave decisively.
    # Servers still busy at the deadline exit on their own,
    # seeing EOF on the stdin pipe our exit closes
    os._exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
