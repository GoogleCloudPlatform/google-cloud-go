# Go GCS SDK over a shared Rust core as a gRPC/UDS sidecar — proof of concept

**Not for production. Not part of the `cloud.google.com/go/storage` module.**

Evaluation write-up: [EVALUATION.md](./EVALUATION.md) (POC row #2 of the
"gRPC over UDS Approach" tab of the GCS SDK Consolidation worklog).

Layout:

- `gcs_uds/proto/gcscore.proto` — the local IPC contract (Ping, ReadObject,
  OpenObject/ReadRange/CloseObject, WriteObject, ListObjects, ReadObjectShm,
  CreateSession/UpdateToken/CloseSession, Stats, Shutdown).
- `gcs_uds/server/` — Rust sidecar (`gcs-core-server`) over the unmodified
  `google-cloud-storage` crate: tonic on a Unix socket, peer-UID check, idle
  exit, parent-death signal, stale-socket cleanup, per-caller credential
  sessions (`session.rs`), shared-memory data plane (`shm.rs`), Stats RPC.
  Enables the crate's appendable-object API via `.cargo/config.toml`.
- `gcs_uds/packaging_tests.sh` — FROM-scratch / noexec / memfd / read-only /
  UDS path length / peer-UID / orphan / stale socket / PID 1 / idle-exit /
  cross-compile experiments (run as root on a Debian 12 VM).
- `gothin/udsthin/` — pure-Go thin client (`CGO_ENABLED=0`), structured into
  production SDK files vs. POC measurement hooks:
  - **Core SDK API** ( mirrors `cloud.google.com/go/storage`):
    - `client.go` — `Client`, `Options`, `NewClient`, `Close`, `BucketHandle`,
      `ObjectHandle`, gRPC/UDS dialer & error mapping.
    - `reader.go` — `Reader` (`NewReader`, `NewRangeReader`, `Read`, `Remain`,
      `Close`) and `Opened` (`Open`, `ReadAt`, `Close` for multi-range/zonal).
    - `writer.go` — `Writer` (`NewWriter`, `Write`, `Close`, `Attrs`) with
      2 MiB client-stream chunking.
    - `list.go` — `ObjectIterator` (`Objects`, `Next`, `PageInfo`).
    - `session.go` — per-`Client` OAuth2 `TokenSource` lending over UDS
      (`CreateSession`, background `UpdateToken` refresh with backoff,
      `CloseSession`, and gRPC `x-gcs-session` metadata interceptors).
  - **Sidecar lifecycle & zero-copy transport**:
    - `sidecar.go` — process spawn, UDS readiness probe, parent-death signal,
      graceful shutdown (`sysproc_unix.go`, `sysproc_windows.go`).
    - `embed_linux.go` (`embed_other.go`, `embed_bin_*.go`) — `//go:embed`
      extraction with `memfd_create(2)` fallback on `noexec`/read-only disks.
    - `rawcodec.go` — custom gRPC codec that aliases the transport buffer on
      `ReadObjectChunk` to avoid protobuf copy overhead.
    - `shm_linux.go` (`shm_other.go`) — `SCM_RIGHTS` + `mmap` shared-memory
      data plane (`ReadShm` copy and `ReadShmZeroCopy` `LoanedView`).
  - **POC-only observability & test hooks** (isolated from SDK methods):
    - `observability.go` — `RPCs` counter (wired via gRPC client interceptors
      so `reader.go`/`writer.go`/`list.go` have zero counter calls),
      `SidecarStats()`, `Sidecar()`, `SessionID()`, and `ForceSession()`.
- `gothin/bench2/` — one W1R3 benchmark body compiled against both
  `*storage.Client` and `*udsthin.Client`; records latency, crossings/op,
  whole-process CPU per GiB (Go + sidecar) and peak RSS (both processes).
- `gothin/rapid/` + `gothin/cmd/rapid_bench` — the Open / Read50x2x4 /
  Random8K SLIs from go/gcs-sdk:continuous-benchmarks-for-acv2 against native
  `MultiRangeDownloader` and the sidecar's BidiRead handles.
- `gothin/cmd/bench2_native`, `gothin/cmd/bench2_uds` — binaries (`check`,
  `ping`, `session-tests`, `shm`, `hijack` subcommands on the UDS one).
- `vm_uds2.sh`, `vm_uds3.sh`, `vm_uds4.sh` — GCE startup scripts used for the
  published numbers; `gothin/results/` — raw logs.

`gothin` is its own Go module (`poc/gothin`); nothing here is imported by the
real SDK. The embedded sidecar file `gothin/udsthin/bin/gcs-core-server-linux-amd64`
is empty in git; the build pipeline copies the static musl build in.

## Design FAQ: Why a custom `gcscore.proto` instead of `google/storage/v2/storage.proto`?

1. **Shared across all target languages (Go, Python, C++, Java, Node.js):**
   `gcs_uds/proto/gcscore.proto` and the Rust sidecar (`gcs-core-server`) are
   language-agnostic and written **once**. Each language only generates its own
   `protoc` client stubs and writes a thin idiomatic wrapper (~1,000–2,000 LoC)
   that maps native language types (`io.Reader`/`io.Writer`, `context.Context`,
   `oauth2.TokenSource` in Go; file-like `BlobReader`/`BlobWriter` in Python;
   `ObjectReadStream` in C++) to the generated stubs. Multiple processes in
   different languages running under the same OS UID can even share a single
   running `gcs-core-server` daemon concurrently via per-client sessions.
2. **SDK-level operations vs. raw backend wire protocol:**
   `google/storage/v2/storage.proto` is the low-level GCS wire protocol
   (resumable upload session IDs, `BidiReadObject` redirect tokens and stream
   reopen state machines, manual CRC32C/MD5 verification, per-chunk offsets,
   and retry loops). Exposing `storage.proto` over UDS would force every thin
   language SDK to re-implement resumable uploads, retries, and checksums —
   defeating the purpose of a shared Rust core. `gcscore.proto` exposes
   high-level SDK operations so all retry, resumable session, stall/timeout,
   and checksum logic lives once inside the Rust sidecar.
3. **Local IPC primitives not present in the Cloud API:**
   `gcscore.proto` defines local-only RPCs for per-client credential lending
   (`CreateSession` / `UpdateToken` / `CloseSession`) and shared-memory transport
   (`ReadObjectShm`), as well as a flat `ReadObjectChunk` layout (`bytes data = 1`
   as field #1) that lets thin clients (`rawcodec.go`) slice the wire payload
   without protobuf copy overhead.
