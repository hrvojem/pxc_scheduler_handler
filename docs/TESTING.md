# Testing PXC Scheduler Handler

This document describes the Go test suite under `internal/` and how to run it on your own machine.

The suite has three kinds of tests:

- **Example-based unit tests**: ordinary Go tests with fixed inputs.
- **Property-based tests** written with [Hegel](https://hegel.dev). They generate many random inputs (random clusters, comments, lock files, configurations), check that a rule always holds, and shrink any failure to a small example. Their names start with `TestProperty_`.
- **Fuzz targets** using Go's built-in fuzzer. Their names start with `Fuzz`.

None of the tests need a running MySQL, PXC or ProxySQL. Where the scheduler talks to the ProxySQL admin interface, the tests use an in-memory stand-in (see [The ProxySQL stand-in](#the-proxysql-stand-in)).

> **Some tests fail on purpose.** About 25 tests reproduce known defects in the scheduler and fail until those defects are fixed. See [Known failing tests](#known-failing-tests) for the list and how to skip them.

## Requirements

- **Go** at the version in `go.mod` (currently `go 1.26.9`). With an older Go and the default `GOTOOLCHAIN=auto`, `go` downloads the right toolchain the first time.
- **No C compiler or cgo.** SQLite comes from `modernc.org/sqlite`, which is pure Go.
- **Hegel's engine** is a native library embedded in `hegel.dev/go/hegel` for Linux (amd64, arm64), macOS on Apple Silicon (arm64) and Windows (amd64, arm64). It is written to a cache directory on first use (`~/.cache/hegel-go` on Linux). On any other platform, including macOS on Intel, build or download `libhegel` yourself and point `HEGEL_LIBHEGEL_PATH` at it.
- **No services.** Nothing listens on a port and nothing needs to be running.

The dependencies are in `go.mod`, so the first `go test` downloads them.

## Quick start

From the repository root:

```bash
# everything, including the tests that are expected to fail
go test -count=1 ./internal/...

# only the tests expected to pass (see "Known failing tests")
go test -count=1 -skip "$(cat test/known-failing-tests.txt)" ./internal/...
```

`-count=1` turns off Go's test cache, so property tests generate fresh inputs every time.

A full run takes about two to three minutes. Most of that time is the multi-run simulation in `internal/proxysql`, and failing property tests take longer while Hegel shrinks their examples. With the known failures skipped, the suite runs in about 20 seconds, and in a few seconds with `PXC_SIM_CASES=30` as well (see [Settings](#settings)).

## What is tested where

The tests live next to the code they test. Files ending in `_test_rules.go` (`internal/cluster`, `internal/locker`) are table-driven tests that were in the project before this suite.

### `internal/global`: configuration and utilities

| File | What it covers |
|---|---|
| `configuration_test.go` | Loading a TOML config (`GetConfig`), defaults, `SanityCheck` accepting, rejecting and correcting values, log setup and help text. Property tests check `SanityCheck` rules over random configurations: single-primary and multi-writer combinations, backup hostgroups, lock times, ping timeouts, and that a second `SanityCheck` changes nothing. |
| `boundaries_test.go` | Exact boundary values for `SanityCheck` and the performance report. |
| `utils_test.go` | `FromStringToMAp`, `ToInt`, `ToBool`, slice helpers, reflection helpers, `MyWaitGroup` and performance tracking. Property tests for `FromStringToMAp` round-trips and arbitrary input. |
| `fuzz_test.go` | `FuzzFromStringToMAp`, and `FuzzConfigSanityCheck`, which decodes and checks arbitrary config text. |

### `internal/cluster`: PXC node evaluation and the action list

| File | What it covers |
|---|---|
| `dataCluster_test.go` | Node parsing (`setParameters`), retry counters in comments (`getRetry`), consolidation of nodes and hostgroups, each node check, the up/down/failover/failback action maps, readers and writers, `GetActionList` end to end, and connection error handling without a server. Property tests build random clusters (1 to 5 nodes with random health, weights, segments and ProxySQL states) and check that `GetActionList` is deterministic, never picks an unusable failover node, never exceeds `maxNumWriters`, and does nothing on a healthy cluster. |
| `fuzz_test.go` | `FuzzGetRetry` (any comment) and `FuzzSetParameters` (any `SHOW VARIABLES` / `SHOW STATUS` values). |

### `internal/locker`: lock file and ProxySQL cluster lock

| File | What it covers |
|---|---|
| `fileLock_test.go` | The scheduler's lock file: creating, reading and removing it, and when a lock owned by another process may be removed. Property tests write random lock contents, PIDs and times. |
| `clusterLock_test.go` | The lock kept in `proxysql_servers.comment` when `clustered = true`: taking, refreshing, expiring and handing over the lock, two PXC clusters on one ProxySQL, odd comment text, and a lost connection. `TestProperty_ClusterLock_OneSchedulerActs` simulates two or three schedulers sharing one `proxysql_servers` table while time passes, in cron and daemon mode, and checks that only one acts at a time. |
| `fuzz_test.go` | `FuzzCheckLockFileExists`: reading any lock file content. |

### `internal/proxysql`: SQL sent to ProxySQL and multi-run behaviour

| File | What it covers |
|---|---|
| `proxySQLObjects_test.go` | Every SQL builder (`InsertRead`, `InsertWrite`, `SaveRetry`, the move and reset statements) run against the ProxySQL stand-in. `ProcessChanges` with retry gating. Property tests seed `mysql_servers` from a random cluster, run `GetActionList` and `ProcessChanges`, and check that the SQL applies, that the config hostgroups (8000 range) never change, and that no node ends up both active and in maintenance. |
| `multiRun_test.go` | A simulation of many scheduler runs against a changing cluster. Each run copies `mysql_servers` to `runtime_mysql_servers` the way `LOAD MYSQL SERVERS TO RUNTIME` does, injects each node's wsrep state, runs the scheduler, and applies its changes. Random fault sequences (unreachable, non-Primary, joining, read only, maintenance, rejecting queries, repaired) are followed by quiet runs. The properties: the cluster settles with the right writers and readers, there are never more ONLINE writers than `maxNumWriters`, and `retryUp` / `retryDown` delays are honoured. |
| `clusterMirror_test.go` | Test helpers, not tests. See the note below. |
| `fuzz_test.go` | `FuzzSaveRetrySQL`: the retry SQL runs for any existing comment. |

**About `clusterMirror_test.go`.** The proxysql tests need clusters loaded the way the scheduler loads them, but `loadNodes`, `getParametersFromProxySQL`, `checkMissingForNodes`, the consolidation steps, `setParameters` and `getRetry` are unexported in `internal/cluster`. The file holds test copies of them. `GetActionList` and `ProcessChanges` are always the real code. If you change one of those functions in `cluster.go`, change its copy too; the file's comments say which function each one mirrors.

### The ProxySQL stand-in

ProxySQL stores its configuration in SQLite, so `internal/testutil/proxyadmin.go` creates an SQLite database with ProxySQL's own `mysql_servers` table definition (`proxysql_servers` and `runtime_mysql_servers` are added by the tests that need them). The scheduler's SQL runs against it as it would against the admin interface. `LOAD …` and `SAVE …` admin commands are accepted and do nothing. Every statement is recorded, so tests can check what the scheduler sent.

The stand-in checks SQL syntax and the table constraints. It does not reproduce ProxySQL's runtime behaviour, such as health checks or how connections are moved when a row changes.

## Common commands

```bash
# one package
go test -count=1 ./internal/locker/

# one test, with output
go test -count=1 -v -run '^TestProperty_GetActionList_WriterCap$' ./internal/cluster/

# all property tests in a package
go test -count=1 -run 'TestProperty_' ./internal/cluster/

# only the example-based tests (fast)
go test -count=1 -skip 'TestProperty_|Fuzz' ./internal/...
```

### Fuzzing

Without `-fuzz`, each fuzz target runs its seeds and every input saved under `testdata/fuzz/`, as part of the normal `go test` run. To search for new failing inputs, fuzz one target at a time:

```bash
go test -run '^$' -fuzz '^FuzzConfigSanityCheck$' -fuzztime 2m ./internal/global/
go test -run '^$' -fuzz '^FuzzCheckLockFileExists$' -fuzztime 2m ./internal/locker/
go test -run '^$' -fuzz '^FuzzGetRetry$' -fuzztime 2m ./internal/cluster/
go test -run '^$' -fuzz '^FuzzSetParameters$' -fuzztime 2m ./internal/cluster/
go test -run '^$' -fuzz '^FuzzSaveRetrySQL$' -fuzztime 2m ./internal/proxysql/
go test -run '^$' -fuzz '^FuzzFromStringToMAp$' -fuzztime 2m ./internal/global/
```

A failing input is saved to `<package>/testdata/fuzz/<FuzzName>/` and replayed on every later run. Commit it together with the fix, so the bug can't come back unnoticed.

### Coverage

```bash
go test -count=1 -coverpkg=./internal/... -coverprofile=cover.out ./internal/...
go tool cover -func=cover.out | tail -1      # total
go tool cover -html=cover.out                # browse it
```

`-coverpkg=./internal/...` counts code run by another package's tests. For example, the proxysql tests run `GetActionList` in `internal/cluster`. The profile is written even when tests fail. The `_test_rules.go` files and `internal/testutil` are compiled into the packages, so they appear in the totals; leave them out when you report a coverage figure.

### Race detector

```bash
go test -race -count=1 ./internal/...
```

`-race` is the one command that needs cgo and a C compiler (Xcode command line tools on macOS). The code that checks nodes in parallel (`getNodesInfo`) needs live servers, so the race detector doesn't reach it.

## Settings

| Setting | Effect |
|---|---|
| `PXC_SIM_CASES=<n>` | Number of random cases for each multi-run simulation property (default 100–200). Use a small value such as 30 for quick runs and mutation testing. |
| `CI=true` | Turns off Hegel's example database (see below). Use it for mutation testing, and in CI. |
| `HEGEL_LIBHEGEL_PATH=<file>` | Path to the `libhegel` library on platforms without an embedded one. |

Other property tests set their number of cases in the code (`hegel.WithTestCases(...)`).

### Hegel's example database

When a property test fails, Hegel saves the failing example in a `.hegel/` directory inside the package and replays it first on the next run, so a failure keeps reproducing until it is fixed. `.hegel/` is in `.gitignore`.

When a saved example stops failing, Hegel may still report the test as a flaky failure for that run. If a property test fails in a way you can't reproduce, delete the directory and run again:

```bash
find internal -name .hegel -type d -prune -exec rm -rf {} +
```

## Known failing tests

These tests reproduce known defects. Each one is written to fail until its defect is fixed, and its failure message describes the defect. Fixing the code, not changing the test, is what should make it pass.

| Test | Package | Defect |
|---|---|---|
| `TestProperty_FileLock_LiveOwnerWithinTimeoutNotRemovable` | locker | The lock of a running process is treated as stale (`Signal(0)` result read the wrong way round) |
| `TestProperty_FileLock_ReadAnyContent`, `FuzzCheckLockFileExists` | locker | A short line in the lock file panics |
| `TestSetLock_ShorterLockLeavesNoTail` | locker | `SetLock` doesn't truncate the file, so old bytes stay behind |
| `TestProperty_ClusterLock_OneSchedulerActs` | locker | In daemon mode two schedulers can act at once after a lock takeover |
| `TestClusterLock_TwoClustersOnOneProxySQL`, `TestProperty_ClusterLock_AnyComment` | locker | Two clusters' locks in one comment make `findLock` panic |
| `TestClusterLock_QuoteInComment` | locker | A `'` in a `proxysql_servers` comment stops the lock from being written |
| `TestPushSchedulerLock_ConnectionLost` | locker | A failed `BeginTx` leads to a nil-pointer panic |
| `TestProperty_GetRetry_RoundTrip` | cluster | Retry counters of 10 or more are not read back |
| `TestGetRetry_WriterHostgroupSuffix`, `TestProperty_GetRetry_OtherHostgroupsUntouched` | cluster | Retry tokens of another hostgroup pair are matched |
| `TestProperty_GetActionList_FailoverNodeUsable` | cluster | Failover can pick a non-Primary, desynced or maintenance node |
| `TestProperty_GetActionList_WriterCap` | cluster | One run can leave more ONLINE writers than `maxNumWriters` |
| `TestProperty_GetActionList_Deterministic` | cluster | Ties between equal-weight nodes depend on Go map order |
| `TestProperty_FromStringToMAp_ValueWithEquals` | global | Values containing `=` are cut off |
| `TestProperty_SanityCheck_LockTimes` | global | A negative `lockRefreshTime` is accepted |
| `TestProperty_SanityCheck_PingTimeouts` | global | `checkTimeOut` of 0 or 1 sets both ping timeouts to 0 |
| `FuzzConfigSanityCheck` | global | Some malformed TOML makes the TOML library panic |
| `TestProcessChanges_QuoteInCommentDoesNotBlockOtherChanges`, `TestProperty_SQLBuilders_StoreAnyComment`, `FuzzSaveRetrySQL` | proxysql | A `'` in a `mysql_servers` comment breaks the SQL and rolls back every change in the run |
| `TestResetNodeDefaults` | proxysql | Resetting defaults writes `max_replication_lag` into `max_latency_ms` |
| `TestProperty_SaveRetry_CommentDoesNotGrow` | proxysql | Every retry save adds a space to the comment |
| `TestProperty_ProcessChanges_NotActiveAndInMaintenance` | proxysql | A node can end up in its active and its maintenance hostgroup |
| `TestProperty_MultiRun_SettlesMultiWriterFailBack` | proxysql | With several writers and failback, a healthy writer is demoted on every run |

**Intermittent.** Some of these depend on Go's random map order or on Hegel happening to generate a particular input, so they don't fail on every run: `GetActionList_WriterCap`, `GetActionList_Deterministic`, `ProcessChanges_NotActiveAndInMaintenance`, `MultiRun_SettlesMultiWriterFailBack` and `GetRetry_OtherHostgroupsUntouched`. Two tests outside the table have also failed occasionally, with no cause found yet: `TestProperty_SanityCheck_Idempotent` and `TestProperty_MultiRun_Settles`. Before you call a failure new, compare with a run of the same tests on the previous commit, using `-count=1`.

**Skipping them.** `test/known-failing-tests.txt` holds one regular expression with every test in the table, plus `TestProperty_SanityCheck_Idempotent`:

```bash
go test -count=1 -skip "$(cat test/known-failing-tests.txt)" ./internal/...
```

When you fix a defect, remove its tests from that file, so they keep guarding the fix.

## Mutation testing

Mutation testing changes the code one small edit at a time and checks that some test fails. It shows which code the tests run but don't really check. It uses [gremlins](https://github.com/go-gremlins/gremlins) and takes about half an hour:

```bash
go install github.com/go-gremlins/gremlins/cmd/gremlins@latest

find internal -name .hegel -type d -prune -exec rm -rf {} +
CI=true PXC_SIM_CASES=30 \
GOFLAGS="-skip=$(cat test/known-failing-tests.txt)" \
gremlins unleash --integration --coverpkg ./internal/... --workers 1 -o gremlins.json .
```

Each setting matters:

- **`--integration --coverpkg ./internal/...`.** The proxysql tests exercise `internal/cluster` code. Without these flags gremlins runs only the mutated package's own tests, and those tests never get to catch a cluster mutant.
- **`GOFLAGS=-skip=…`.** gremlins needs a passing suite to start from and has no option to skip tests, so the skip list goes to `go test` through `GOFLAGS`.
- **`CI=true`.** Without it, Hegel's example database builds up in gremlins' working copy. Examples saved under one mutant are replayed under later ones and fail as flaky, so those mutants are wrongly counted as killed. A run without it reported 97.5% instead of about 85%.
- **`--workers 1`.** Parallel workers interfered with each other in earlier runs and gave nonsense scores.

Mutants reported as `TIMED OUT` are mostly caught too: some mutants make the multi-run simulation much slower than gremlins' time limit, which it bases on a short coverage run. Apply one by hand and run the suite to check.
