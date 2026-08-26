
## [0.2.0] - 2026-08-26

### Features / Changes

- parallelize queries and timeout everything after 30 seconds by default.

- various simplifications in the code and comments.


## [0.1.1] - 2026-07-29

### Fixes

- when querying metadata behave more like core/async-imap.

## [0.1.0] - 2026-07-28

- initial implementation: scrape and cache the relay list from
  https://chatmail.at/relays, cache one chat profile per relay,
  report irohrelay, turn, maxsmtprecipients (IMAP METADATA)
  and storage quota (GETQUOTAROOT) as table or jsonlines.

- adapt to chatmail/workflows standard.

