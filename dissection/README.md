# Kubernetes Source Code Dissection

A collection of end-to-end analyses of the Kubernetes open source code, organized by major scenario.
Each document traces code flow based on actual file paths, function names, and line numbers.

## How To Use This Folder

1. Start from the scenario closest to your symptom (request path, scheduling, kubelet, network, or auth).
2. Read the `Big Picture` section first, then walk the numbered steps in order.
3. At every interface boundary, use the interface tracing recipe below before continuing.
4. Run the scenario's verification commands to confirm the flow on a live cluster.

## Suggested Learning Path

If you are new to Kubernetes internals, this order minimizes context switching:

1. Read Scenario 1 first to learn API request and persistence foundations.
2. Read Scenario 6 next to understand authn/authz/admission gates on every request.
3. Read Scenario 2 and Scenario 3 to understand scheduler + controller reconciliation.
4. Read Scenario 4 for node execution details (runtime, probes, termination).
5. Finish with Scenario 5 to connect readiness state to traffic routing behavior.

## Interface Tracing Recipe

Use this checklist whenever code jumps through interfaces or function indirection:

1. Find the interface definition and method signature.
2. Search compile-time assertions (`var _ Interface = &Struct{}`) first.
3. If assertions are missing, follow constructor return types and field assignments.
4. Locate runtime selection points (registry maps, mode switches, profile lookups, queue handlers).
5. Confirm the concrete method that executes at runtime.

### Worked Example: `framework.Framework` -> `*frameworkImpl`

This is what "follow the wiring" looks like on real Kubernetes code:

1. Start at the interface: `type Framework interface` in [../pkg/scheduler/framework/interface.go](../pkg/scheduler/framework/interface.go#L200).
2. Find the highest-signal implementation proof: `var _ framework.Framework = &frameworkImpl{}` in [../pkg/scheduler/framework/runtime/framework.go](../pkg/scheduler/framework/runtime/framework.go#L321).
3. Follow the factory that hides the concrete type: `NewFramework(...) (framework.Framework, error)` in [../pkg/scheduler/framework/runtime/framework.go](../pkg/scheduler/framework/runtime/framework.go#L328).
4. Follow the DI wiring that stores the interface value: scheduler profile construction writes the returned value into `profile.Map` in [../pkg/scheduler/scheduler.go](../pkg/scheduler/scheduler.go#L362).
5. Confirm the runtime selection point: `frameworkForPod` picks `Profiles[pod.Spec.SchedulerName]` in [../pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L533).

The first grep tells you who *can* implement the interface. The wiring tells you which concrete value this scenario *actually uses*.

## Source Accuracy Convention

To keep docs beginner-friendly without inventing APIs:

1. Prefer exact function/type names that exist in source.
2. If a step is conceptual (not a literal symbol), label it as conceptual text, not as code.
3. When no compile-time assertion exists, prove interface resolution via constructor signature + assignment wiring.
4. If Kubernetes renames internals, update snippets to match current code before extending the flow.

## Scenarios

| # | Scenario | Document | Key Components |
|---|---------|------|-------------|
| 1 | API request flow | [01-api-request-flow.md](01-api-request-flow.md) | kubectl → client-go → kube-apiserver → etcd |
| 2 | Pod scheduling | [02-pod-scheduling.md](02-pod-scheduling.md) | kube-scheduler → Filter → Score → Bind |
| 3 | Deployment rolling update | [03-deployment-rolling-update.md](03-deployment-rolling-update.md) | DeploymentController → ReplicaSetController → Pod |
| 4 | kubelet pod lifecycle | [04-kubelet-pod-lifecycle.md](04-kubelet-pod-lifecycle.md) | kubelet → CRI → Volume → Probe → Graceful shutdown |
| 5 | Service network routing | [05-service-network-routing.md](05-service-network-routing.md) | EndpointSlice → kube-proxy → iptables |
| 6 | Authentication/authorization flow | [06-auth-flow.md](06-auth-flow.md) | Authenticator → RBAC → Admission Webhook |
| 7 | Leases: node heartbeats and leader election | [07-leases-heartbeat-leader-election.md](07-leases-heartbeat-leader-election.md) | kubelet node lease → kube-node-lease; LeaderElector → LeaseLock → kube-system |
| 8 | Node heartbeat vs Node status | [08-node-heartbeat-node-status.md](08-node-heartbeat-node-status.md) | kubelet lease loop + node status loop → node lifecycle controller health decisions |

## Codebase Layout Summary

```
kubernetes/
├── cmd/                         # Binary entry points for each component
│   ├── kube-apiserver/          # API server
│   ├── kube-scheduler/          # Scheduler
│   ├── kube-controller-manager/ # Controller manager
│   ├── kube-proxy/              # Proxy
│   ├── kubelet/                 # Node agent
│   └── kubectl/                 # CLI client
├── pkg/                         # Internal packages
│   ├── scheduler/               # Scheduler core logic
│   ├── controller/              # Controllers (deployment, rs, endpoint, etc.)
│   ├── kubelet/                 # kubelet core logic
│   ├── proxy/                   # kube-proxy core logic
│   ├── kubeapiserver/           # API server configuration
│   └── auth/                    # Authentication/authorization
├── staging/src/k8s.io/          # Independent libraries (future separate repos)
│   ├── client-go/               # Go client library
│   ├── apiserver/               # Generic API server framework
│   └── api/                     # API type definitions
└── plugin/pkg/auth/authorizer/  # RBAC authorizer
```

## Common Patterns

### Informer + WorkQueue Pattern
The basic pattern used by every controller:
```
API Server
    │ Watch (Long Polling)
    ▼
Informer (local cache)
    │ Events (Add/Update/Delete)
    ▼
WorkQueue (Rate-limited)
    │ key (namespace/name)
    ▼
syncXxx() function
    │ Compare current state vs desired state
    ▼
Reconcile state via API calls
```

### Reconciliation Loop
```go
// The core controller pattern
for {
    desired := getDesiredState()  // from spec
    actual  := getActualState()   // current cluster state
    if desired != actual {
        reconcile(desired, actual)  // close the gap
    }
}
```

## Core Concepts Glossary

Cross-cutting terms that recur across every scenario. Each scenario's own **Related Concepts** section goes deeper.

| Term | One-line meaning | Seen in |
|------|------------------|---------|
| **Declarative API / desired state** | Objects record intent in `spec`; controllers reconcile `status` toward it | All |
| **Reconciliation (level-triggered)** | Re-read current state and close the gap to `spec`; missed events are harmless | 02, 03, 04, 05 |
| **Informer / lister / workqueue** | Cached watch + rate-limited queue feeding a `syncXxx()` handler | 02, 03, 05 |
| **`resourceVersion`** | Optimistic-concurrency token; stale writes get HTTP 409 | 01, 03 |
| **Owner references** | Parent→child links enabling cascading delete and adoption | 03, 04 |
| **GVR vs. GVK** | Resource (URL path) vs. Kind (object schema), bridged by the RESTMapper | 01, 06 |
| **Interface → concrete type** | Go implicit interfaces resolved via `var _ Iface = &Struct{}` | All |
| **CRI / CNI** | gRPC contract to the container runtime / plugin contract for Pod networking | 04, 05 |
| **conntrack / DNAT / SNAT** | Connection tracking and address rewriting behind Service VIPs | 05 |
| **AuthN / AuthZ / Admission** | Who are you / are you allowed / is the object acceptable | 01, 06 |

## Live Validation Workflow

When you want to verify a trace end-to-end:

1. Reproduce a minimal scenario (`kubectl apply`, create Pod, create Service, etc.).
2. Observe events (`kubectl get events -A --sort-by=.lastTimestamp`).
3. Inspect component logs (`kube-apiserver`, `kube-scheduler`, `kube-controller-manager`, `kubelet`, `kube-proxy`).
4. Correlate timestamps from logs with step numbers in the matching dissection doc.
5. If a hop is unclear, re-run the interface tracing recipe at that boundary.
