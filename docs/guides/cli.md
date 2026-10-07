# The `fom-log` CLI

`fom-log` is a standalone command-line tool for inspecting, diagnosing and
compacting local `FileLogBackend` files. It's handy for debugging or
trimming a production log offline.

## Commands

```text
fom-log inspect  <path>   # summary + first/last clock + event counts by type
fom-log events   <path>   # one line per event: position, clock, type, key fields
fom-log diagnose <path>   # verify CRCs frame-by-frame; report corruption
fom-log compact  <path>   # offline snapshot: keep live state, archive the rest
fom-log help     <command> # usage of a command (same as fom-log <command> --help)
```

Every command accepts `-h`/`--help` and `-V`/`--version`. Exit codes: `0`
success, `1` the log is corrupt or ends in an incomplete frame, `2` the path is
missing or unreadable, or an option is invalid. A path that does not exist is
reported as `No such file: <path>`; one the tool cannot even check — typically
because a parent directory is not searchable for your user — as `Cannot access
<path> (permission denied?)`.

### inspect

Prints the log id, length, current leader, the clocks of the first and last
event (the last one is the highest clock), the highest event timestamp (the
largest timestamp of any event — not necessarily the last event's, since the
wall clock can step back and a compaction keeps old timestamps), and a
per-type event count. After a compaction the log no longer holds every clock
between the first and the last; `inspect` then says how many are missing:

```text
$ fom-log inspect /var/lib/fom/weather.bin
Log: /var/lib/fom/weather.bin
Length: 128 event(s)
Current leader: fom-7f3c…
First clock: 0 (position 0)
Last clock: 12967 (position 127)
Clock gaps: 12840 clock(s) between first and last are absent (compacted away)
Highest event timestamp: 1737045123456
Counts by type:
  LogLeader                 3
  LogChangeGraph            2
  LogInitialized           40
  LogLoaded                40
  LogDead                  20
  …
```

### events

Lists the events one per line: **position** (index in the file), **clock**,
timestamp, type and the fields that matter for that type. Position and clock
are equal in a log that was never compacted and differ after a compaction —
look up a `Sid` from `Engine.introspect()` (`processName@clock`) by its clock.

After a compaction the `TIMESTAMP` column is **not** ascending: the events the
snapshot kept keep their original, older timestamps, while the synthetic
`LogLeader` at clock `0` (and the trailing `LogTrigger`/`LogPaused`/`LogSnapshot`)
carry the wall-clock time of the compaction — so the first line can easily be
newer than the lines under it. The listing is ordered by position and clock, as
the log itself is; the timestamps are per-event metadata, not a sort key. See
[Snapshots & compaction](../concepts/snapshots.md#what-a-snapshot-contains).

```text
fom-log events <path> [--from POSITION] [--limit N] [--type TYPE[,TYPE…]] [--process NAME] [--full]
```

```text
$ fom-log events /var/lib/fom/weather.bin --type LogPaused,LogResumed
POSITION  CLOCK       TIMESTAMP                 TYPE                  DETAILS
41        12901       2025-01-16T16:31:10.120Z  LogPaused             process=Alerts stale=false forDependency=false (operator pause)
42        12902       2025-01-16T16:31:10.121Z  LogPaused             process=Quotes stale=true forDependency=true (paused because a dependency is paused)
57        12950       2025-01-16T16:40:02.004Z  LogResumed            process=Alerts
```

| Type | Details shown |
|---|---|
| `LogLeader` | `instance=` leader instance id |
| `LogChangeGraph` | node count and node names |
| `LogInitialized` | `sid=name@clock`, `replaces=name@clock` when it replaces a still-serving version, property keys — up to 10 in full; more as the count and the first 10 (`properties=12345 keys [a, b, … +12335 more]`), unless `--full` |
| `LogLoaded`, `LogDead` | `sid=name@clock` |
| `LogCleanedUp` | `sid=name@clock`, `ok=` |
| `LogTrigger` | triggered process names |
| `LogDependencyChanged` | `sid=`, `dep=` dependency name, `depClock old -> new` |
| `LogSnapshot` | `checkpointClock=` |
| `LogPaused` | `process=`, `stale=`, `forDependency=` — an operator pause or one caused by a paused dependency |
| `LogResumed` | `process=` |

- `--from POSITION` — start scanning at this position (default `0`).
- `--limit N` — print at most `N` events (default `1000`). When more match, a
  `NOTE` on stderr tells you the `--from` value to continue with.
- `--type` — only these event types (comma-separated or repeated). An unknown
  type name exits `2` and lists the valid ones.
- `--process NAME` — only events about that process: its init, load, death,
  cleanup, pauses and resumes, triggers naming it, and dependency changes of it
  or of a dependency named `NAME`. Leader, graph and snapshot events are left out.
- `--full` — print every property key of a `LogInitialized` (by default a
  process with more than 10 keys shows the count and the first 10, sorted, so a
  large process does not produce a multi-megabyte line).

Like `inspect`, it reads a temporary copy; on a log with an incomplete last
frame it prints a `WARNING`, lists the readable prefix and exits `1`. On a log
damaged **inside** (one an engine refuses to open) it does the same: the
`WARNING` gives the byte offset of the bad frame and how many events are
readable before it, the listing covers those events (filters and `--from` apply
to them), and the exit code is `1`. Nothing after the damage is listed — use
`diagnose` for the cause. Only the temporary copy is cut at the offset; your
file is not touched.

### diagnose

Walks every frame verifying its CRC and that the event round-trips. If a
frame is corrupt or partial, reports its byte offset and how many events are
readable before it, and exits `1`. An event that cannot be read is reported
by its position (plus the clock of the last readable event before it). A missing
or unreadable path exits `2`.

```text
$ fom-log diagnose /var/lib/fom/weather.bin
Opened: /var/lib/fom/weather.bin (40213 bytes, read via a temporary copy)
Readable events: 128
Diagnose OK: all 128 events round-tripped.
```

An **empty** (0-byte) file is not damage: nothing was ever written to it, not
even the header, and an engine would treat it as a new, empty log. `diagnose`
says so and exits `0` (`Diagnose OK: the file is empty (0 bytes, no header
yet); an engine would treat it as a new, empty log; nothing to check.`);
`inspect` adds the same note to its summary, and `events` prints it instead of
`No matching events`.

`diagnose` also tells the two kinds of damage apart. A **torn last frame** — an
incomplete final append, what a crash mid-append leaves behind — is reported as

```text
Diagnose FAIL: log is corrupt at byte offset 40209 of 40213: 128 event(s) readable before it, ...
Cause: torn last frame — the final 4 byte(s) are an incomplete append, which is what a crash mid-append leaves behind. The next engine open cuts them off and saves them next to the log as weather.bin.truncated.<millis>; all 128 event(s) before it are intact and nothing else is damaged.
```

so no action is needed: the engine recovers on its own. Any **other** damage (a bad
frame followed by more data, an intact frame that cannot be decoded, an event
whose clock is not greater than the previous one — a log spliced from two
timelines) prints the
`LogCorruptedException` cause and the line `This is damage inside the log, not a
torn last frame: an engine refuses to open it and leaves the file untouched.`

!!! note "`inspect`, `events` and `diagnose` never modify your file"
    They copy the log to a temporary file and open the copy,
    so they neither take the leader lock nor truncate the original at a corrupt
    frame, and they work on a log that a running engine holds open. (A copy of a
    live log can end in a half-written frame, which is reported as corruption.)
    When the log ends in an incomplete last frame, `inspect` prints a `WARNING`
    to stderr, its summary covers only the readable prefix, and it exits `1`. When the log has
    any other damage — one an engine refuses to open with `LogCorruptedException`
    (see [Persistence backends](persistence-backends.md#filelogbackend)) —
    `inspect` prints a `WARNING` with the byte offset instead of a summary and
    exits `1`, `events` prints the same `WARNING` and lists the events before
    the offset (exit `1`), and `diagnose` prints `Diagnose FAIL` with the offset followed by
    the cause. Every path in the output names **your** file, never the temporary
    copy — including when the file turns out not to be a fom log at all and the
    command fails with `Failed to open <your path>: ...` (exit `2`).

!!! note "The temporary copy needs free space"
    Each of these commands (and `compact`, for its pre-check) first copies the
    **whole** log into a fresh directory under the JVM's `java.io.tmpdir`
    (`/tmp` by default on Linux), so that directory needs at least the log's
    size free; the copy is deleted when the command ends. If the copy cannot be
    made — a full disk, no write permission — the command exits `2` with
    `Failed to open <your path>: could not make the temporary working copy of the
    log in <temp dir> (...)`, which names the temp directory; your log was not
    modified. When the cause is a full disk or an exhausted quota, the message
    gives the number of bytes needed and says to free space there; for any other
    cause (`Read-only file system`, `AccessDeniedException`, ...) it says the
    directory is not writable. Either way you can point the copy elsewhere,
    e.g. `JAVA_OPTS=-Djava.io.tmpdir=/var/tmp fom-log inspect ...`.

### compact

Compacts the log offline — what [`Engine.snapshot()`](../concepts/snapshots.md)
does in a running engine. It keeps the leader record, every process's live
state (each preceded by the graph it was written under, the current graph
last) and every pause, and moves the previous contents to a sibling
`<file>.archived.<timestamp>`.

```text
$ fom-log compact /var/lib/fom/weather.bin
Compacted /var/lib/fom/weather.bin: 12840 -> 43 event(s)
Previous contents archived to /var/lib/fom/weather.bin.archived.1737045123456
```

- Run it while the application is **stopped**. A log that a running engine
  holds open is refused (exit `2`): the backend's file lock is taken.
- An **empty** (0-byte) file is refused (exit `2`) with `the file is empty (0
  bytes); nothing to compact; it was not modified`, and left exactly as it is —
  no header is written and no `.lock` file is created.
- Any other failure — a directory you cannot write to, a full disk — exits `2`
  with `Cannot compact <your path>: ...` followed by the whole cause chain, so
  you see what actually went wrong (for example
  `java.nio.file.AccessDeniedException: /var/lib/fom/weather.bin.tmp`) and on
  which file. Compaction writes a sibling `<file>.tmp` and needs the log's
  **directory** to be writable, not just the file.
- A **corrupt** log — including one that merely ends in an incomplete last
  frame — is refused (exit `1`) and left untouched, so nothing is cut off behind
  your back. Run `diagnose` first; for damage other than a torn last frame,
  back the file up and restore it or truncate it to the reported offset.
- After compaction the application restarts exactly as before: live state is
  warm-loaded and paused processes stay paused. The same logic is available in
  code as `LogCompaction.compact(backend)`.

## Running it

`fom-log` builds a runnable distribution (via the Gradle `application` plugin):

```bash
./gradlew :fom-log:installDist
./fom-log/build/install/fom-log/bin/fom-log inspect /path/to/log.bin
```

!!! danger "Treat input files as trusted"
    `inspect`/`events`/`diagnose`/`compact` deserialize the log payloads. The file backend applies
    an allowlist deserialization filter (only `io.fom.*` + JDK types), and
    `diagnose` primarily verifies CRCs — but you should still run the CLI only
    on log files you trust. See [Security](../security.md).
