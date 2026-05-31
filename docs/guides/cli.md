# The `fom-log` CLI

`fom-log` is a standalone command-line tool for inspecting, diagnosing, and
(eventually) migrating local `LocalFileLogBackend` files. It's handy for
debugging a production log offline.

## Commands

```text
fom-log inspect  <path>                         # summary + event counts by type
fom-log diagnose <path>                         # verify CRCs frame-by-frame; report truncation
fom-log migrate  --from <v> --to <v> <in> <out> # cross-major migration (stub)
```

### inspect

Prints the log id, length, current leader, last event timestamp, and a
per-type event count:

```text
$ fom-log inspect /var/lib/fom/orders.bin
Log: /var/lib/fom/orders.bin
Length: 128 event(s)
Current leader: fom-7f3c…
Last event timestamp: 1737045123456
Counts by type:
  LogLeader                 3
  LogChangeGraph            2
  LogInitialized           40
  LogLoaded                40
  LogDead                  20
  …
```

### diagnose

Opens the file (which truncates any corrupt tail on open), then walks every
event verifying it round-trips. Reports the truncation point and the count of
faulty events; exits non-zero if any are found.

```text
$ fom-log diagnose /var/lib/fom/orders.bin
Opened: /var/lib/fom/orders.bin
Length after open (any corrupt tail was truncated): 128
Diagnose OK: all 128 events round-tripped.
```

### migrate

A placeholder for cross-major-version format migration. The current format is
the only released major, so there is no migration target yet; the command exits
non-zero and explains as much.

## Running it

`fom-log` builds a runnable distribution (via the Gradle `application` plugin):

```bash
./gradlew :fom-log:installDist
./fom-log/build/install/fom-log/bin/fom-log inspect /path/to/log.bin
```

!!! danger "Treat input files as trusted"
    `inspect`/`diagnose` deserialize the log payloads. The file backend applies
    an allowlist deserialization filter (only `io.fom.*` + JDK types), and
    `diagnose` primarily verifies CRCs — but you should still run the CLI only
    on log files you trust. See [Security](../security.md).
