# Paste-ready text for the worklog tab "gRPC over UDS Approach" — Go column

> Source of every number: [EVALUATION.md on branch `grpc-uds-go`](https://github.com/cpriti-os/google-cloud-go/blob/grpc-uds-go/storage/internal/rust-uds-poc/EVALUATION.md) and Rust sidecar PR [`googleapis/google-cloud-rust#7350`](https://github.com/googleapis/google-cloud-rust/pull/7350) (raw logs under `gothin/results/uds5-worklog/` and `uds{2,3,4}-{regional,zonal}/`).
> - **Updated Worklog Spec Run (`uds5-worklog`):** GCE `c4-standard-192` (192 vCPUs, 720 GiB RAM, Ubuntu 24.04 LTS, `GVNIC` + `TIER_1` 200 Gbps networking) in `us-central1-b` (`instance_id=3697964452583574513`) against standard regional bucket `gs://cpriti-thickthin-poc-use1` in `us-east1` (non-colocated CloudPath), with both Cloud Monitoring metrics (`compute.googleapis.com/instance/cpu/utilization` and `agent.googleapis.com/memory/percent_used` via `google-cloud-ops-agent` `2.73.0`) verified and exported.
> - **Same-Region / Zonal Reference Runs (`uds2`–`uds4`):** GCE `c3-standard-8` in `asia-southeast1-b` (same-region `asia-southeast1`) and `us-central1-b` (same-zone Rapid bucket `us-central1-b`).
> - "CPU" and "Peak Memory" always report **Go process plus Rust sidecar** (`getrusage` / `VmHWM` + sidecar `Stats` + Cloud Monitoring VM telemetry).

---

## POC row #2 — Go

**Go thin client (pure Go, `CGO_ENABLED=0`) → gRPC over UDS → Rust sidecar (`gcs-core-server`, tonic + `google-cloud-storage` 1.21, [PR #7350](https://github.com/googleapis/google-cloud-rust/pull/7350)).**
Sidecar is embedded in the Go module with `//go:embed`, extracted to `~/.cache/gcs-core/<hash>/` (memfd fallback), one sidecar per user shared by all Go processes (reference-counted `active_conns` + idle timeout), per-caller credential sessions (access tokens lent over the socket, bound to peer `uid+pid` via `SO_PEERCRED` with dead-PID reaping), tuned HTTP/2 flow-control windows (16 MiB stream / 64 MiB conn / 4 MiB frames + 8 MiB `SO_SNDBUF`/`SO_RCVBUF`), and optional shared-memory data plane (`memfd` + `SCM_RIGHTS` + `mmap`) for zero-copy reads.
Code + logs: https://github.com/cpriti-os/google-cloud-go/tree/grpc-uds-go/storage/internal/rust-uds-poc | Rust sidecar PR: https://github.com/googleapis/google-cloud-rust/pull/7350

---

## Challenges table — Go cells

**Zero copy across UDS boundary**
Over a gRPC/UDS socket every byte incurs dual kernel copies (~0.2 CPU‑s/GiB) plus HTTP/2 framing and protobuf serialization. With tuned 16 MiB/64 MiB HTTP/2 windows, 4 MiB frames, 8 MiB UDS socket buffers, and zero-copy protobuf framing (`RawCodec`), 128 MiB cross-region (`c4-standard-192`, `us-central1` → `us-east1`) transfers measure **2.17–2.29 CPU‑s/GiB** on UDS (`go-uds-json` 2.17–2.20, `go-uds-grpc` 2.28–2.29) vs **1.63–1.67 CPU‑s/GiB** for `go-json-h1` and **2.27–2.30 CPU‑s/GiB** for native `go-grpc-cp` (i.e. **at CPU parity with native gRPC** and +0.54 s/GiB over HTTP/1.1). On 48-thread 5-minute sustained BidiRead (`bidiread-16m-48t`), UDS achieves **1383.0 MiB/s at 3.20 CPU‑s/GiB** vs **1405.9 MiB/s at 2.43 CPU‑s/GiB** native (**98.4% throughput parity**, +0.77 CPU‑s/GiB). Shared memory (`memfd` + `SCM_RIGHTS` + `mmap`, gRPC as control plane) brings 128 MiB read CPU on `c4-standard-192` down from **2.30–2.39 s/GiB** (Go 0.61–0.63 + sidecar 1.70–1.77) to **1.27–1.47 s/GiB** with `copy()` (Go 0.11–0.12) and **1.14–1.33 s/GiB** with a loaned zero-copy slice (Go **0.00–0.02 s/GiB**, beating native HTTP/1.1!). **High effort (Blocker only if zero-copy over standard `io.Reader`/`io.Writer` without shm is required).**

**Packaging and Distribution**
`go get` / `go mod tidy` keep working — the main win over FFI. Cost: +6.9 MB per platform in the module zip (~41 MB for 6 platforms vs 1 MB today) and +21 MB in every linked Go binary (17 MB stripped static musl sidecar). Rust side still needs a C toolchain per target to cross-compile. The Go thin client itself cross-compiles to linux/arm64, darwin, windows, s390x, freebsd. **High effort.**

**Deployment in major customer environments**
Measured: distroless/`FROM scratch` works only with CA certs bundled (otherwise sidecar panics); `noexec` /tmp fails to exec the extracted binary, and the `memfd_create` fallback is blocked on hardened kernels (`vm.memfd_noexec=1/2`/MDWE) and flagged by enterprise EDR (Falcon/Defender/OSSEC) as fileless execution; read-only socket dir fails; UDS path >108 bytes fails; glibc build fails in scratch. Cloud Run/Functions/GKE pay a second process (~60–100 MB idle RSS, 8–37 ms spawn) per instance; sharing only pays off on VMs with several Go processes per user. Peer-UID check, dead-PID session/handle reaping, stale socket recovery, connection-refcounted idle exit, PID 1: all pass. **High effort, hard failures unless mitigated.**

**Mono Repo Concerns**
`google-cloud-go` would ship prebuilt binaries built from `google-cloud-rust` with separate cadence, CVE/SBOM/provenance; a sidecar CVE forces a Go release and vice-versa. Shared sidecar ⇒ version skew between two Go binaries on one box unless sockets are per-version. **High effort.**

**Tool chain, coordination and maintenance costs**
Go + Rust + C toolchains, a proto contract that must stay compatible both ways, every new option lands in proto + Rust + Go. Rust release build 4 min on an 8-vCPU VM, 9 min cold on Cloudtop. Error mapping maps HTTP/gRPC status codes and expired/invalid auth (`401 Unauthenticated`), though detailed structured error bodies (`google.rpc.Status.details`) need explicit plumbing. **High effort.**

**Impedance mismatch**
Reads/range reads/writes/appendable writes/list/preconditions map cleanly (appendable writes within 5% of native up to 128 MiB and 8–9% at 512 MiB on zonal; both SDKs compute/verify CRC32C over the network). **Unfixable across processes:** caller-process hooks (`option.WithHTTPClient`/custom `RoundTripper`, `option.WithGRPCConn`/custom `grpc.DialOption`s, in-process OTel exporters/span context, `context` values) cannot cross the socket, breaking customers that use custom transports/proxies. Also:
1. **The Rust crate lacks DirectPath today** (major GCE/GKE throughput/CPU regression vs Go's native gRPC DirectPath client).
2. **Non-zonal 200 MiB writes (`Workload 5`):** On a standard regional bucket (`gs://cpriti-thickthin-poc-use1`), Go's `storage.Writer` streams 16 MiB chunks over gRPC `BidiWriteObject` (`6195.6 MiB/s`, `2579 MiB` Peak RSS across 48 threads), whereas `google-cloud-storage` 1.21.0 only supports gRPC `open_appendable_object` on Zonal buckets (`UNIMPLEMENTED` on regional) and buffers each 200 MiB payload in memory on the JSON `write_object` fallback (`1534.8 MiB/s`, `16,928 MiB` Peak RSS / `3.09%` VM memory across 48 × 200 MiB concurrent uploads).
Credentials cross as lent 1 h access tokens bound to `(uid, pid)` via `SO_PEERCRED` with TTL and dead-PID reaping. **Blocker for 100% drop-in backwards compatibility.**

**Size of the Binary**
+21 MB per Go binary (17 MB stripped sidecar), +6.9 MB per platform in module zip; sidecar RSS 57–309 MB after large transfers; peak RSS of the pair at parity with native for streaming transfers (e.g., 649–740 MiB vs 476–558 MiB at 128 MiB sequential; 2755 MiB vs 1945 MiB on 48-thread sustained BidiRead), though buffered JSON `write_object` in Rust spikes RSS to 16.9 GiB under 48 × 200 MiB concurrent writes (`48 × 200 MiB = 9.6 GiB` in-flight buffers). **OK / High effort.**

**Harder debugging experience**
Go stack traces, pprof, `-race`, HTTP tracing stop at the socket; sidecar needs its own logs/traces/core dumps. Observed: sidecar start-up panic (missing CA bundle) and concurrency bottlenecks were invisible from Go without the sidecar log/Stats. **High effort.**

**Direct copy of data into GPU buffers**
Today's Go SDK has no GPU/CUDA buffer API. If ever added, a separate process cannot write into the caller's CUDA buffers without CUDA IPC handles and the sidecar linking CUDA; shm gives host memory at best. **N/A for Go today (Blocker across processes if ever added).**

---

## Experimental results — Go (Worklog `c4-standard-192` Spec Run + Same-Region/Zonal Reference)

### A. Worklog Specification Run: `c4-standard-192` (`us-central1-b` VM → `us-east1` Bucket, 200 Gbps GVNIC `TIER_1`, Ubuntu 24.04)

#### 1. Workloads 1–3 (Rapid SLIs: `open`, `read50x2x4/serial`, `read50x2x4/concurrent`, `random8k`) & Workload 6 (`list-objects-100`, `list-buckets-100`)
*Two rounds (`r1 / r2`), `-iters=10` (`random8k`: 2,000 × 8 KiB reads). Cross-region RTT (`us-central1` ↔ `us-east1`) is ~31 ms.*

| Workload / SLI | Metric | Native Go gRPC (`go-grpc-mrd`) | Go→UDS→Rust gRPC (`go-uds-grpc`) | Native Go JSON (`go-json-range`) | Go→UDS→Rust JSON (`go-uds-json`) |
|---|---|---|---|---|---|
| **W1: `open` (200 MiB)** | P50 / P90 / P99 | 178.0–183.0 / 221.9–222.0 / 221.9–222.0 ms | 181.1–187.8 / 218.1–243.4 / 218.1–243.4 ms | 70.9–107.7 / 167.0–172.9 / 167.0–172.9 ms | 183.8–207.1 / 222.6–231.0 / 222.6–231.0 ms |
| | Max / Mean | 224.4–254.2 / 194.5–198.7 ms | 236.4–259.6 / 171.2–208.1 ms | 169.0–248.1 / 116.7–118.6 ms | 237.9–238.3 / 182.6–207.5 ms |
| | CPU ms/op / Peak RSS | 4.60–25.56 ms / 86–88 MiB | 15.34–16.34 ms / 74–83 MiB | 2.77–3.02 ms / 39–40 MiB | 15.11–15.22 ms / 77–82 MiB |
| **W2a: `read50x2x4/serial` (100 MiB)** | P50 / P90 / P99 | 3.83–4.31 / 3.91–4.37 / 3.91–4.37 s | 4.02–5.03 / 4.09–5.21 / 4.09–5.21 s | 7.20–7.37 / 11.74–12.41 / 11.74–12.41 s | 3.89–14.06 / 3.94–16.32 / 3.94–16.32 s |
| | Max / Mean / MiB/s | 4.15–5.18 / 3.87–4.40 s / **22.6–25.7 MiB/s** | 4.33–5.37 / 4.06–4.96 s / **20.1–24.5 MiB/s** | 15.91–16.05 / 8.91–9.13 s / **10.9–11.2 MiB/s** | 4.23–18.31 / 3.92–13.48 s / **7.4–25.4 MiB/s** |
| | CPU s/GiB / Peak RSS | 3.34–3.52 / 188–189 MiB | **2.43–2.57** / 201–204 MiB | 3.40–3.52 / 156–157 MiB | **2.53–2.76** / 209–210 MiB |
| **W2b: `read50x2x4/concurrent` (100 MiB)** | P50 / P90 / P99 | 3.37–3.50 / 3.41–3.74 / 3.41–3.74 s | 3.36–3.38 / 3.43–3.44 / 3.43–3.44 s | 328.8–359.1 / 347.0–373.6 / 347.0–373.6 ms | 3.34 / 3.41–3.44 / 3.41–3.44 s |
| | Max / Mean / MiB/s | 3.45–3.82 / 3.38–3.48 s / **28.6–29.4 MiB/s** | 3.43–3.44 / 3.38 s / **29.4 MiB/s** | 369.4–490.7 / 333.5–373.8 ms / **263.8–294.1 MiB/s** | 3.44 / 3.34–3.38 s / **29.4–29.8 MiB/s** |
| | CPU s/GiB / Peak RSS | 2.99–3.29 / 289–290 MiB | **2.55–2.65** / 308–377 MiB | 2.85–3.52 / 254–256 MiB | **2.69** / 385–400 MiB |
| **W3: `random8k` (8 KiB reads)** | P50 / P90 / P99 | 31.3–32.1 / 31.9–39.5 / 40.5–52.5 ms | 32.2 / 33.2–39.8 / 36.1–42.6 ms | 55.4–57.1 / 63.6–66.5 / 71.9–83.8 ms | 31.3–39.9 / 31.9–40.4 / 34.5–43.9 ms |
| | Max / Mean / MiB/s | 44.0–83.1 / 31.8–33.7 ms / 0.2 MiB/s | 44.3–63.4 / 32.4–35.5 ms / 0.2 MiB/s | 135.2–197.8 / 55.6–57.8 ms / 0.1 MiB/s | 38.9–50.6 / 31.4–40.1 ms / 0.2 MiB/s |
| | CPU ms/op / Peak RSS | **0.15–0.17 ms** / 290–291 MiB | **0.34 ms** / 319–389 MiB | 0.23–0.25 ms / 255–257 MiB | **0.36–0.37 ms** / 396–411 MiB |
| **W6a: `list-objects-100`** | P50 / P90 / P99 | 191.0–219.2 / 217.0–231.1 / 217.0–231.1 ms | **102.8–114.2** / 154.1–171.4 / 154.1–171.4 ms | 95.9–96.5 / 110.1–115.8 / 110.1–115.8 ms | **78.0–109.9** / 139.1–190.3 / 139.1–190.3 ms |
| | Max / Mean / CPU ms/op | 222.7–235.2 / 198.6–209.8 ms / 0.72–0.83 ms | 185.1–188.8 / 107.2–116.2 ms / **0.73–0.80 ms** | 112.6–118.0 / 94.8–98.0 ms / 1.23–4.82 ms | 192.0–197.2 / 109.9–113.9 ms / **0.84–0.89 ms** |
| **W6b: `list-buckets-100`** | P50 / P90 / P99 | 935.9–962.4 / 987.3–987.8 / 987.3–987.8 ms | 934.0–942.2 / 970.1–981.0 / 970.1–981.0 ms | 923.8–948.5 / 961.3–968.5 / 961.3–968.5 ms | 924.9–937.8 / 957.0–957.8 / 957.0–957.8 ms |
| | Max / Mean / CPU ms/op | 992.1–996.9 / 950.5–965.8 ms / 3.88–4.02 ms | 972.1–995.8 / 937.4–945.2 ms / **2.15–2.18 ms** | 962.6–990.6 / 913.0–931.6 ms / 4.47–5.58 ms | 958.0–964.1 / 929.4–937.8 ms / **2.42–5.47 ms** |

#### 2. Workloads 4 & 5 (Sustained 48-Thread Parallel BidiRead & BidiWrite for 5 Minutes) + Emitted Cloud Monitoring VM Metrics
*48 parallel worker goroutines running continuously for 5 minutes (`-sustained-threads=48 -sustained-dur=5m`) on `c4-standard-192` (`192 vCPUs`, `720 GiB RAM`). Cloud Monitoring metrics queried directly from `monitoring.googleapis.com/v3` for `instance_id=3697964452583574513`.*

| Workload | Backend | P50 | P90 | P99 | Max | Mean | Aggregate Throughput | Process CPU (`s/GiB` / `ms/op` / `%`) | Process Peak RSS | Cloud Monitoring `instance/cpu/utilization` (avg / max) | Cloud Monitoring `memory/percent_used` (`state=used` avg / max) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **W4: BidiRead 16 MiB chunks (48 threads, 5m)** | **Native Go gRPC (`go-grpc-mrd`)** | **535.7 ms** | **637.3 ms** | 804.9 ms | **908.0 ms** | **544.4 ms** | **1405.9 MiB/s** (`1474.2 MB/s`) | **2.43 s/GiB** / 39.50 ms / **1.7%** | **1945 MiB** | **2.33%** / **2.61%** | **0.98%** / **0.98%** |
| *(Per-200 MiB object rollup)* | *Native Go gRPC (`bidiread-200m-48t`)* | *6.76 s* | *7.75 s* | *10.10 s* | *10.20 s* | *6.81 s* | *1405.9 MiB/s* | *2.43 s/GiB / 480.00 ms / 1.7%* | *1945 MiB* | *2.33% / 2.61%* | *0.98% / 0.98%* |
| **W4: BidiRead 16 MiB chunks (48 threads, 5m)** | **Go→UDS→Rust gRPC (`go-uds-grpc`)** | **536.8 ms** | 667.3 ms | **765.6 ms** | 999.4 ms | 553.6 ms | **1383.0 MiB/s** (`1450.2 MB/s`, **98.4% of native**) | **3.20 s/GiB** / 52.03 ms / **2.2%** | **2755 MiB** | **3.00%** / **3.20%** | **1.53%** / **1.63%** |
| *(Per-200 MiB object rollup)* | *Go→UDS→Rust gRPC (`bidiread-200m-48t`)* | *6.76 s* | *8.35 s* | *9.26 s* | *10.28 s* | *6.92 s* | *1383.0 MiB/s* | *3.20 s/GiB / 632.25 ms / 2.2%* | *2755 MiB* | *3.00% / 3.20%* | *1.53% / 1.63%* |
| **W5: Write 200 MiB (48 threads, 5m)** | **Native Go gRPC (`go-grpc-mrd`)** | **1.53 s** | **1.91 s** | **2.28 s** | 19.10 s | **1.54 s** | **6195.6 MiB/s** (`6496.6 MB/s`) | **1.45 s/GiB** / 282.83 ms / **4.6%** | **2579 MiB** | **5.63%** / **6.01%** | **1.47%** / **1.55%** |
| **W5: Write 200 MiB (48 threads, 5m)** | **Go→UDS→Rust (`go-uds-grpc` → JSON fallback)** | **6.06 s** | **7.00 s** | **7.62 s** | **13.08 s** | **6.19 s** | **1534.8 MiB/s** (`1609.4 MB/s`) | **1.55 s/GiB** / 303.64 ms / **1.2%** | **16928 MiB** | **1.99%** / **3.48%** | **3.06%** / **3.09%** |

*Why W5 UDS drops from `6195.6 MiB/s` to `1534.8 MiB/s` and raises Peak RSS to `16.9 GiB` on a standard regional bucket:* On a regional bucket (`gs://cpriti-thickthin-poc-use1`), Go's native `storage.Writer` uses gRPC `BidiWriteObject` streaming (`6.2 GiB/s`, `2.58 GiB` RSS). By contrast, `google-cloud-storage` 1.21.0 only supports `open_appendable_object` on **Zonal** buckets (returning `UNIMPLEMENTED` on regional buckets), so the Rust sidecar automatically falls back to `storage.write_object()` over JSON/HTTP1, which buffers the 200 MiB payload per concurrent writer (`48 × 200 MiB = 9.6 GiB` in-flight buffers → `16.9 GiB` Peak RSS, reflected in Cloud Monitoring `memory/percent_used` rising from `1.47%` to `3.06%`). On a **Zonal** bucket where both sides use gRPC `BidiWriteObject` (`open_appendable_object`), UDS write latency and memory are at full parity with native Go (`423–437 ms` vs `404–456 ms` at 128 MiB; `1528–1531 MiB` vs `1537–1544 MiB` Peak RSS at 512 MiB).

#### 3. Sequential W1R3 & Shared-Memory (`memfd`) Data Plane on `c4-standard-192` (with Tuned HTTP/2 Windows & 8 MiB UDS Buffers)

**Sequential `whole` & `32k` (128 MiB, `us-central1-b` → `us-east1`):**
| Style | Op | Native `go-json-h1` | Go→UDS→Rust JSON (`go-uds-json`) | Native `go-grpc-cp` | Go→UDS→Rust gRPC (`go-uds-grpc`) |
|---|---|---|---|---|---|
| `whole` (128 MiB) | Write P50 / P90 / P99 | 4.40 / 4.62 / 4.62 s (`29.1 MiB/s`) | **4.23 / 4.52 / 4.52 s (`30.3 MiB/s`)** | 1.56 / 1.91 / 1.91 s (`82.2 MiB/s`) | 4.56 / 4.77 / 4.77 s (`28.1 MiB/s`, JSON write fallback) |
| `whole` (128 MiB) | Read P50 / P90 / P99 | 1.25 / 1.56 / 1.57 s (`102.2 MiB/s`) | **1.35 / 1.62 / 1.75 s (`94.8 MiB/s`)** | 4.51 / 4.95 / 5.17 s (`28.4 MiB/s`) | **4.45 / 4.58 / 5.19 s (`28.8 MiB/s`)** |
| `whole` (128 MiB) | CPU s/GiB / Peak MiB | **1.67 s/GiB** / 476 MiB | **2.17 s/GiB** / 740 MiB | **2.30 s/GiB** / 558 MiB | **2.29 s/GiB** / 649 MiB |
| `32k` (128 MiB) | Write P50 / Read P50 | 3.90 s / 1.28 s (`100.1 MiB/s`) | **3.99 s / 1.28 s (`99.7 MiB/s`)** | 1.79 s / 4.50 s (`28.4 MiB/s`) | 4.15 s / **4.38 s (`29.3 MiB/s`)** |
| `32k` (128 MiB) | CPU s/GiB / Peak MiB | **1.63 s/GiB** / 350 MiB | **2.20 s/GiB** / 993 MiB | **2.27 s/GiB** / 443 MiB | **2.28 s/GiB** / 861 MiB |

**Shared-Memory (`memfd` + `SCM_RIGHTS` + `mmap`) vs Tuned gRPC Chunks (128 MiB Read on `c4-standard-192`):**
| Data Plane Mode | JSON Transport P50 / Throughput | JSON CPU s/GiB (Go + Sidecar) | gRPC Transport P50 / Throughput | gRPC CPU s/GiB (Go + Sidecar) |
|---|---|---|---|---|
| **Tuned gRPC chunks over UDS** (copy into caller `p`) | 1.573 s (`81.4 MiB/s`) | **2.30 s/GiB** (Go `0.61` + sidecar `1.70`) | 4.630 s (`27.6 MiB/s`) | **2.39 s/GiB** (Go `0.63` + sidecar `1.77`) |
| **`ReadObjectShm` (`memfd`)** (copy into caller `p`) | 1.572 s (`81.4 MiB/s`) | **1.27 s/GiB** (Go `0.11` + sidecar `1.16`) | 4.539 s (`28.2 MiB/s`) | **1.47 s/GiB** (Go `0.12` + sidecar `1.36`) |
| **`ReadObjectShm` (`memfd`)** (zero-copy loaned slice) | 1.689 s (`75.8 MiB/s`) | **1.14 s/GiB** (Go **`0.00`** + sidecar `1.14`) | 4.486 s (`28.5 MiB/s`) | **1.33 s/GiB** (Go **`0.02`** + sidecar `1.31`) |

---

### B. Same-Region (`asia-southeast1`) & Zonal Rapid (`us-central1-b`) Reference Summary (`c3-standard-8`)

**JSON path, same-region (`asia-southeast1`), one op at a time (p50, two rounds; CPU = Go + sidecar, s/GiB write+read)**

| size | op | Go SDK JSON/HTTP1 | Go SDK JSON (HTTP2, default) | Go→UDS→Rust JSON | CPU h1 / h2 / UDS |
|---|---|---|---|---|---|
| 1 MiB | write / read | 88–92 / 37–41 ms | 87–94 / 36–37 ms | 83–93 / 39 ms | 3.2 / 4.1 / 4.2–4.4 |
| 16 MiB | write / read | 266–305 / 103–117 ms | 286–312 / 98–108 ms | 288–348 / 109–116 ms | 1.5 / 2.4 / 2.6 |
| 128 MiB | write / read | 1.50–1.60 s / 625–646 ms | 1.44–1.57 s / 524–608 ms | 1.54–1.70 s / 556–636 ms | 1.4 / 2.3 / 2.6 |
| 512 MiB | write / read | 5.6–5.8 s / 2.46–2.49 s | 5.8–6.5 s / 1.89–2.09 s | 5.8 s / 1.81–2.17 s | 1.4 / 2.3 / 2.6 |

**Rapid SLIs (Open / Read50x2x4 / Random8K), gRPC both sides (Same-Zone Zonal vs Same-Region Regional)**

| SLI | zonal native | zonal UDS | same-region native | same-region UDS |
|---|---|---|---|---|
| Open p50 | 30–39 ms | 20–22 ms | 31–40 ms | 28–31.5 ms |
| Read50x2x4 serial / concurrent p50 | 530–555 / 316–359 ms | 587–684 / 323–332 ms | 710–831 / 348–443 ms | 564–803 / 368–532 ms |
| Random8K p50 / p99 | 1.4–1.7 / 2.6–4.7 ms | 1.1–1.5 / 2.3–3.1 ms | 3.2–4.5 / 9.3–12.8 ms | 4.2–5.0 / 7.2–10.5 ms |
| Random8K CPU ms/op | 0.09 | 0.22–0.23 | 0.09 | 0.23 |
| Peak RSS (pair) | 184–221 MiB | 238–293 MiB | 156–194 MiB | 289–331 MiB |

**Zonal appendable writes + reads (Go `Append=true` vs Rust appendable, p50)**: 128 MiB write 404–456 vs 423–437 ms, read 469–492 vs 493–533 ms; 512 MiB write 1.26–1.33 vs 1.37–1.43 s, read 1.79–1.90 vs 1.86–1.94 s; CPU 2.2–2.3 vs 2.9–3.0 s/GiB; peak RSS 1537–1544 vs 1528–1531 MiB.

**Summary for Go**:
- **Read & Control-Plane Parity:** On `c4-standard-192` (200 Gbps `TIER_1`), 48-thread sustained BidiRead (`Workload 4`) reaches **1383.0 MiB/s on UDS vs 1405.9 MiB/s native (98.4% parity)** with P50 `536.8 ms` vs `535.7 ms` and P99 `765.6 ms` vs `804.9 ms`, while `list-objects-100` (`102.8–114.2 ms` vs `191.0–219.2 ms`) and `list-buckets-100` (`934.0–942.2 ms` vs `935.9–962.4 ms`) match or beat native Go.
- **Tuned IPC & Shared Memory:** Tuning HTTP/2 flow-control windows (16 MiB/64 MiB), 4 MiB frames, and 8 MiB UDS socket buffers brings 128 MiB gRPC read CPU to **2.28–2.29 s/GiB** (matching native Go gRPC `2.27–2.30 s/GiB`), while `ReadObjectShm` (`memfd`) drops total CPU to **1.14–1.33 s/GiB** (Go process CPU **0.00–0.02 s/GiB**).
- **Key Gaps / Blockers:** (1) **Rust crate lacks DirectPath** on GCE/GKE; (2) **Rust crate lacks non-zonal gRPC `BidiWriteObject` / streaming `write_object`** (`Workload 5` on regional buckets falls back to buffered JSON `write_object`, dropping 48-thread write throughput from `6195.6 MiB/s` to `1534.8 MiB/s` and raising Peak RSS from `2.58 GiB` to `16.9 GiB`); (3) **caller-process hooks (`WithHTTPClient`/`WithGRPCConn`/in-process OTel) cannot cross the socket**; and (4) `noexec`/distroless/EDR environments and two-runtime debugging require explicit operational handling.
