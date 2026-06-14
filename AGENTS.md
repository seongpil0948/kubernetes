# Kubernetes

## Communication Preferences

- Dry, concise, low-key humor. No flattery, no forced memes. Skip preambles and postambles.
- Comments explain "why", not "what".
- Error messages: actionable and specific. No vague "something went wrong" output.

## Constraints

- **Generated files are read-only.** Never hand-edit `zz_generated.*` or `generated.pb.go`. Run `make update`.
- **go.mod/go.work are generated.** Use `hack/pin-dependency.sh` + `hack/update-vendor.sh`. Never `go mod tidy`.
- **Staging is source of truth** for `k8s.io/*` (`staging/src/k8s.io/`). Never import `k8s.io/kubernetes` from staging.
- **Boilerplate required.** Every `.go` file needs the license header from `hack/boilerplate/boilerplate.go.txt`.

## Contributor Guidelines

- Keep changes focused and reviewable
- Add or update relevant tests
- Do not put `@mentions` or `fixes #...` keywords in commit messages
- Do not add `Co-authored-by:`  in commit messages

## Layout

- `cmd/` holds thin binary entry points only. Real logic lives elsewhere: kubectl is `staging/src/k8s.io/kubectl/`, apiserver machinery is `staging/src/k8s.io/apiserver/`, kubelet/scheduler/controllers are under `pkg/`.
- External API types: `staging/src/k8s.io/api/`. Internal versions: `pkg/apis/`.
- [dissection/](dissection/) has end-to-end code traces (API request flow, scheduling, kubelet pod lifecycle, etc.) with exact file/function references — read these before exploring a subsystem from scratch.

## Commands

Run `make help` for all available targets. Common workflows:

```
make test WHAT=./pkg/kubelet GOFLAGS=-v     # Unit tests (one package)
make test WHAT=./pkg/x KUBE_TEST_ARGS='-run ^TestName$$'   # Single test
make test-integration WHAT=./test/integration/scheduler
make verify                                 # All verification checks (slow)
make update                                 # ALL generators and formatters (slow)
```

Prefer targeted runs over the slow full targets:

- `make verify WHAT="gofmt typecheck"` or run a single `hack/verify-*.sh` script
- `make quick-verify` — fast subset for iteration
- Run the matching `hack/update-*.sh` instead of `make update` (e.g. `hack/update-codegen.sh` after editing API types)
- `hack/verify-golangci-lint.sh -a [packages]` — lint only code changed vs origin/master

Integration tests need etcd on PATH: `hack/install-etcd.sh`, then `export PATH=$PWD/third_party/etcd:$PATH`.

## API changes

1. Edit external types (`staging/src/k8s.io/api/<group>/<version>/types.go`) and internal types (`pkg/apis/<group>/types.go`)
2. Run `hack/update-codegen.sh` (deepcopy, conversion, defaults, protobuf, validation)
3. Refresh round-trip fixtures: `UPDATE_COMPATIBILITY_FIXTURE_DATA=true go test k8s.io/api -run //HEAD`

Feature gates: declared in `pkg/features/kube_features.go` (plus per-component `staging/src/k8s.io/{apiserver,controller-manager,apiextensions-apiserver}/pkg/features/kube_features.go`); run `hack/update-featuregates.sh` after adding one.

## Style

- Packages: lowercase, single word, match directory.
