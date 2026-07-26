cmrelayinfo
===========

Report chatmail relay capabilities: iroh relay URL, TURN server,
maxsmtprecipients and storage quota.

For each relay, cmrelayinfo logs in with a chat profile that is
created once and then cached under `~/.cache/cmrelayinfo/`,
so repeated runs do not create new accounts on relays.

Usage
-----

```
cmrelayinfo                     # all relays from chatmail.at/relays (cached)
cmrelayinfo nine.testrun.org    # one specific relay
cmrelayinfo 116.203.128.59      # relay given as IP address
cmrelayinfo --json --refresh    # jsonlines output, refetch relay list
```

Development
-----------

```
uv venv && uv pip install -e ".[dev]"
uv run pytest
```
