# Contributing to Walspool

Thank you for your interest in contributing to **Walspool**! We welcome contributions that align with our core focus: building a resilient, zero-dependency, sub-microsecond write-ahead log buffer and real-time observability hub for microservices and edge architectures.

---

## 1. Architectural Philosophy & Coding Standards

Walspool is built on the **Black-Box Architecture** doctrine (Parnas, Meyer DbC, Cockburn Ports & Adapters, Ousterhout Deep Modules):

- **Zero External Dependencies**: The core repository relies strictly on the Go standard library (`os`, `sync`, `bufio`, `hash/crc32`, `net/http`, `log/slog`). External third-party dependencies are not accepted in the core engine.
- **Contract First**: Any new feature must be specified via interfaces in [`ports.go`](ports.go) before implementation.
- **Defensive Boundaries**: Validate all inputs at the boundary. Never leak implementation details across public types.
- **Deterministic Concurrency**: All code must pass the Go race detector with zero warnings (`go test -race ./...`). Deadlocks, lock contention on hot paths, and unbounded goroutine spawning are strictly rejected.
- **Sub-Microsecond Hot Path**: Ingestion paths must minimize or eliminate heap allocations (`sync.Pool`, defensive slicing).

---

## 2. Developer Certificate of Origin (DCO)

To maintain clarity around intellectual property and ensure that all contributions are legally safe, Walspool enforces the **Developer Certificate of Origin (DCO), Version 1.1**.

### How to Sign Your Commits
By signing off on your commit using Git's `-s` flag:

```bash
git commit -s -m "feat(storage): implement synchronous fsync option"
```

You certify the following statement:

```text
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it; and

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

---

## 3. Licensing & Intellectual Property

Walspool is distributed under the **[Functional Source License, Version 1.1, MIT Future License (FSL-1.1-MIT)](LICENSE)**:

1. **Contributor Grant**: By submitting a pull request, you agree that your contributions will be licensed under the terms of the FSL-1.1-MIT, and will automatically convert to the permissive MIT License on the second anniversary of each release.
2. **Commercial Rights**: All rights not expressly granted under the FSL-1.1-MIT (including the exclusive right to operate managed commercial cloud services or commercial enterprise editions) remain with **Yohann Hommet**.

---

## 4. Trademark Policy

The **Walspool** name, wordmark, 3D ribbon logo, and associated branding assets located in `assets/` are the exclusive property of **Yohann Hommet**.

- You **may**:
  - Refer to "Walspool" truthfully and accurately when describing compatibility (e.g., *"Connector for Walspool"*).
  - Use the logo to link back to the official Walspool repository.
- You **may not**:
  - Use the name "Walspool" or its logo as part of a commercial product, company name, or managed cloud service name without express written consent.
  - Distribute modified versions of Walspool under the name "Walspool" without a clear disclaimer stating it is an unofficial fork.

---

## 5. Development Workflow

### Prerequisites
- Go 1.22+ installed
- Docker (optional, for sidecar testing)

### Running Tests
Every PR must pass all unit, integration, and race detector tests:

```bash
# Run all tests with race detector
go test -v -race ./...

# Run benchmarks
go test -bench=. -benchmem ./...
```

### Pull Request Checklist
- [ ] Code follows idiomatic Go style (`gofmt`, `go vet`).
- [ ] Interfaces in [`ports.go`](ports.go) are updated if contracts changed.
- [ ] Unit tests are included and verify boundary behaviors.
- [ ] Zero race conditions (`go test -race ./...` passes).
- [ ] Commits are signed with DCO (`git commit -s`).
- [ ] Documentation (README.md, API tables) is updated if public routes or flags changed.
