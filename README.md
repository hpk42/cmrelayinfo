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

Installation
------------

```
uv tool install cmrelayinfo
```

Development
-----------

```
uv venv && uv pip install -e . --group test
uv run pytest
```

CI checks come from the shared
[chatmail/workflows](https://github.com/chatmail/workflows)
repository; run them locally with `uvx ruff check .` and
`uvx ruff format --check .`

To release, run the shared release script from a checkout of
chatmail/workflows:

```
python ../workflows/scripts/make_new_release.py
```

It runs the checks, tests the built wheel, writes the CHANGELOG.md
entry with git-cliff, then tags and pushes. The release.yml workflow
publishes to PyPI via trusted publishing (OIDC).
