# Scenario 2: Pod Scheduling Flow

Traces the full flow from the creation of a Pending Pod through the scheduler detecting it, selecting a node, and binding it.

## Big Picture

Scheduling is a two-phase decision process. The scheduling cycle computes a best node using plugin-driven filtering and scoring, and the binding cycle commits that decision back to the API server. The scheduler keeps these phases separate so expensive selection logic can stay deterministic while API write/backoff handling remains isolated.

## Interface Resolution Guide (This Scenario)

Scheduler code is heavily interface-based (`framework.Framework`, plugin extension points). Use this order:
1. Start from the extension-point interface.
2. Jump to compile-time assertions (`var _ framework.X = &Y{}`).
3. Follow framework/plugin constructors that return interfaces.
4. Verify runtime dispatch at profile selection and plugin execution sites.

### Worked Example: `framework.Framework` -> `*frameworkImpl`

This scenario crosses this interface on nearly every scheduling call:

1. Interface: `type Framework interface` in [../pkg/scheduler/framework/interface.go](../pkg/scheduler/framework/interface.go#L200).
2. Compile-time proof: `var _ framework.Framework = &frameworkImpl{}` in [../pkg/scheduler/framework/runtime/framework.go](../pkg/scheduler/framework/runtime/framework.go#L321).
3. Factory that hides the type: `NewFramework(...) (framework.Framework, error)` in [../pkg/scheduler/framework/runtime/framework.go](../pkg/scheduler/framework/runtime/framework.go#L328).
4. DI wiring: profile construction stores the returned interface value in `profile.Map` during scheduler startup in [../pkg/scheduler/scheduler.go](../pkg/scheduler/scheduler.go#L362).
5. Runtime selection: `frameworkForPod` resolves `Profiles[pod.Spec.SchedulerName]` in [../pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L533), so each later `fwk.RunFilterPlugins(...)` call executes on a `*frameworkImpl`.

## Reading Guide (Beginner)

- **Trigger:** a Pending Pod appears in the scheduler queue from informer events.
- **Two different outputs:** scheduling cycle chooses a node; binding cycle writes that choice to the API server.
- **Why retries happen:** cache assumptions and API writes are decoupled, so failures requeue Pods for another pass.
- **Success criterion for this scenario:** `pod.spec.nodeName` is set and `PodScheduled=True`.
- **Most common confusion:** scheduling success does not start containers; kubelet execution is a separate scenario.

## Overall Flow Diagram

```
Pending Pod stored in etcd
        │ (Watch event)
        ▼
[1] pkg/scheduler/backend/queue/scheduling_queue.go
    PriorityQueue.Add() — add to activeQ
        │
        ▼
[2] pkg/scheduler/schedule_one.go:ScheduleOne()
    Pop Pod from activeQ → scheduleOnePod()
        │
        ├─ [3] schedulingCycle()
        │       │
        │       ├─ [3a] RunPreFilterPlugins()
        │       │         Pod-level pre-validation
        │       │
        │       ├─ [3b] findNodesThatPassFilters()
        │       │         Run Filter plugins in parallel
        │       │         → list of feasible nodes
        │       │
        │       ├─ [3c] prioritizeNodes()
        │       │         PreScore → Score → NormalizeScore
        │       │         → per-node scores
        │       │
        │       ├─ [3d] selectHost()
        │       │         Select highest-scoring node
        │       │
        │       └─ [3e] assumeAndReserve()
        │                 Reserve in Cache + Reserve plugins
        │
        └─ [4] runBindingCycle() (async goroutine)
                │
                ├─ RunPermitPlugins() — permit check
                ├─ RunPreBindPlugins() — pre-bind processing
                ├─ bind() — set nodeName on API server
                └─ RunPostBindPlugins()
```

---

## Step-by-Step Detailed Analysis

### Scheduler Struct

**File:** [pkg/scheduler/scheduler.go](../pkg/scheduler/scheduler.go#L68)

```go
// Lines 68-125: Scheduler struct
type Scheduler struct {
    Cache           internalcache.Cache              // in-memory node/Pod cache
    NextPod         func(...) (*framework.QueuedPodInfo, error)  // Pop from activeQ
    SchedulingQueue internalqueue.SchedulingQueue    // 3-tier priority queue
    SchedulePod     func(...)                        // Filter + Score logic
    Profiles        profile.Map                      // scheduler profiles (plugin configuration)
    APIDispatcher   *apidispatcher.APIDispatcher     // asynchronous API calls
}
```

> ⚠️ **Where does the concrete `Framework` come from?** `Profiles` is `profile.Map`, i.e. `map[string]framework.Framework` ([profile.go:46](../pkg/scheduler/profile/profile.go#L46)) — a map of *interface* values, so the struct behind it is not obvious. Because Go uses implicit interface satisfaction, trace it like this:
>
> 1. **Interface:** `type Framework interface` — [framework/interface.go:200](../pkg/scheduler/framework/interface.go#L200)
> 2. **Concrete struct:** `frameworkImpl` — [framework/runtime/framework.go:58](../pkg/scheduler/framework/runtime/framework.go#L58)
> 3. **Compile-time proof:** `var _ framework.Framework = &frameworkImpl{}` — [framework.go:321](../pkg/scheduler/framework/runtime/framework.go#L321) (the highest-signal line: it names both interface and struct)
> 4. **Factory (hides the type):** `NewFramework(...) (framework.Framework, error)` — [framework.go:328](../pkg/scheduler/framework/runtime/framework.go#L328)
> 5. **DI wiring:** `profile.NewMap` stores one `frameworkImpl` per profile — [scheduler.go:362](../pkg/scheduler/scheduler.go#L362)
> 6. **Runtime selection:** `Profiles[pod.Spec.SchedulerName]` picks the concrete value per Pod — [schedule_one.go:533](../pkg/scheduler/schedule_one.go#L533) (in `frameworkForPod`)
>
> So every `fwk framework.Framework` parameter you see below is really a `*frameworkImpl`.

---

### [1] Scheduling Queue

**File:** [pkg/scheduler/backend/queue/scheduling_queue.go](../pkg/scheduler/backend/queue/scheduling_queue.go#L170)

```go
// Lines 170-215: PriorityQueue struct
type PriorityQueue struct {
    activeQ             activeQueuer        // pods to schedule (heap)
    backoffQ            backoffQueuer       // Pods in backoff (exponential backoff)
    unschedulablePods   *unschedulablePods  // unschedulable Pods (max 5 minutes)
    podMaxInUnschedulablePodsDuration time.Duration
    clock               clock.WithTicker
    lock                sync.RWMutex
}
```

**Queue state transitions:**
```
New Pod → activeQ
                │ scheduling failure
                ├─ (unschedulable plugin) → unschedulablePods
                │                              │ 5 min elapsed or cluster event
                └─ (other)       → backoffQ   │
                                    │ backoff expired└──→ activeQ
                                    └──────────────→ activeQ
```

**QueuedPodInfo structure:**
```go
type QueuedPodInfo struct {
    PodInfo                *PodInfo
    Pod                    *v1.Pod
    Timestamp              time.Time
    Attempts               int                  // number of attempts
    UnschedulablePlugins   sets.Set[string]     // plugins that failed
    BackoffExpiration      time.Time
    InitialAttemptTimestamp *time.Time
}
```

**Key functions:**

| Function | Line | Role |
|------|------|------|
| `Add(ctx, pod)` | 728 | Add Pod to activeQ (runs PreEnqueue plugins) |
| `Pop(logger)` | 945 | Pop next Pod from activeQ |
| `Activate()` | 744 | Move unschedulable/backoff → activeQ |
| `MoveAllToActiveOrBackoffQueue()` | 129 | Requeue on cluster events |
| `AddUnschedulableIfNotPresent()` | - | Requeue failed Pods |

---

### [Framework] How Plugins Are Wired into `frameworkImpl`

Every `fwk.RunXxxPlugins(...)` call you see in the scheduling cycle dispatches to a concrete plugin slice inside `frameworkImpl`. Understanding the wiring from plugin registration to those slices is essential before reading the Filter/Score sections.

**`frameworkImpl` holds one slice per extension point (runtime/framework.go:58):**

```go
type frameworkImpl struct {
    preFilterPlugins  []framework.PreFilterPlugin
    filterPlugins     []framework.FilterPlugin
    postFilterPlugins []framework.PostFilterPlugin
    preScorePlugins   []framework.PreScorePlugin
    scorePlugins      []framework.ScorePlugin
    reservePlugins    []framework.ReservePlugin
    preBindPlugins    []framework.PreBindPlugin
    bindPlugins       []framework.BindPlugin
    postBindPlugins   []framework.PostBindPlugin
    permitPlugins     []framework.PermitPlugin
    preEnqueuePlugins []framework.PreEnqueuePlugin
    queueSortPlugins  []framework.QueueSortPlugin
    // ... plus placement/score variant slices

    pluginsMap        map[string]fwk.Plugin  // all plugins by name
}
```

**`getExtensionPoints()` maps config → slice pointer (runtime/framework.go):**

```go
func (f *frameworkImpl) getExtensionPoints(plugins *config.Plugins) []extensionPoint {
    return []extensionPoint{
        {&plugins.PreFilter, &f.preFilterPlugins},
        {&plugins.Filter,    &f.filterPlugins},
        {&plugins.Score,     &f.scorePlugins},
        {&plugins.Bind,      &f.bindPlugins},
        // ... one entry per extension point
    }
}
```

During `NewFramework(...)`, a single loop calls `getExtensionPoints`, then for each configured plugin name, does a type-assertion against the target interface and appends the concrete value to the slice:

```go
// Simplified from NewFramework() (framework.go:328)
for _, ep := range f.getExtensionPoints(plugins) {
    for _, pg := range *ep.plugins {
        p := f.pluginsMap[pg.Name]  // concrete Plugin already constructed
        if iface, ok := p.(FilterPlugin); ok {  // interface assertion
            *ep.slicePtr = append(*ep.slicePtr, iface)
        }
    }
}
```

A plugin that implements multiple interfaces (e.g. `NodeAffinity` implements both `PreFilterPlugin` and `FilterPlugin`) ends up in **multiple** slices.

**Compile-time interface proofs for built-in plugins:**

| Plugin | Interfaces | Assertion location |
|--------|-----------|-------------------|
| `NodeAffinity` | PreFilter, Filter, Score | [nodeaffinity.go](../pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go) |
| `TaintToleration` | Filter, PreScore, Score | [taint_toleration.go](../pkg/scheduler/framework/plugins/tainttoleration/taint_toleration.go) |
| `DefaultBinder` | Bind | [binder.go](../pkg/scheduler/framework/plugins/defaultbinder/default_binder.go) |
| `DefaultPreemption` | PostFilter | [default_preemption.go](../pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go) |

**Runtime dispatch:**

```go
// RunFilterPlugins iterates the slice; any Unschedulable status short-circuits
func (f *frameworkImpl) RunFilterPlugins(ctx, state, pod, nodeInfo) {
    for _, pl := range f.filterPlugins {
        status := pl.Filter(ctx, state, pod, nodeInfo)
        if !status.IsSuccess() {
            return status  // first failure stops the chain for this node
        }
    }
}
```

Score plugins use the same pattern but collect all scores into `NodePluginScores` (one entry per node per plugin) before NormalizeScore and weight multiplication.

---

### [2] Scheduling Main Loop

**File:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L67)

```go
// Lines 67-96: ScheduleOne()
func (sched *Scheduler) ScheduleOne(ctx context.Context) {
    podInfo := sched.NextPod(logger)  // activeQ.Pop()
    sched.scheduleOnePod(ctx, podInfo)
}

// Lines 99-148: scheduleOnePod()
func (sched *Scheduler) scheduleOnePod(ctx context.Context, podInfo *framework.QueuedPodInfo) {

    // Line 127: initialize cycle state (data sharing between plugins)
    state := framework.NewCycleState()

    // Line 140: scheduling cycle (synchronous)
    scheduleResult, assumedPodInfo, status := sched.schedulingCycle(ctx, state, ...)

    // Line 147: binding cycle (async goroutine)
    go sched.runBindingCycle(ctx, state, ...)
}
```

---

### [3] Scheduling Cycle

**File:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L175)

```go
// Lines 175-252: schedulingCycle()
func schedulingCycle(ctx, state, fwk, podInfo, ...) {

    // 1. Refresh NodeInfo snapshot (line 183)
    sched.Cache.UpdateSnapshot(logger, sched.nodeInfoSnapshot)

    // 2. Filter + Score (line 193)
    scheduleResult, err := sched.SchedulePod(ctx, fwk, state, assumedPodInfo.Pod)

    // 3. PostFilter on failure (preemption, line 200)
    if err != nil {
        fwk.RunPostFilterPlugins(ctx, state, pod, diagnosis.NodeToStatus)
    }

    // 4. Assume + Reserve (line 230)
    assumeAndReserve(ctx, state, fwk, podInfo, ...)
}
```

#### [3a] PreFilter Plugins

**File:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L628)

```go
// Lines 628-718: findNodesThatFitPod()
preFilterResult, preFilterStatus, preFilterStatusMap :=
    fwk.RunPreFilterPlugins(ctx, state, pod)
```

**Example PreFilter plugins:**

| Plugin | File | Role |
|---------|------|------|
| NodePorts | [pkg/scheduler/framework/plugins/nodeports/node_ports.go](../pkg/scheduler/framework/plugins/nodeports/node_ports.go#L73) | Cache the Pod's port requirements |
| NodeAffinity | [pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go](../pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go#L148) | Parse node affinity rules |
| VolumeBinding | pkg/scheduler/framework/plugins/volumebinding/ | Pre-compute PVC binding feasibility |

#### [3b] Filter Plugins (Parallel Execution)

**File:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L777)

```go
// Lines 777-860: findNodesThatPassFilters()
func findNodesThatPassFilters(ctx, fwk, state, pod, diagnosis, nodes) {

    // Parallel processing (lines 801-846)
    fwk.Parallelizer().Until(ctx, len(nodes), func(i int) {
        nodeInfo := nodes[i]
        status := fwk.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
        if status.IsSuccess() {
            feasibleNodes = append(feasibleNodes, nodeInfo)
        }
    }, metrics.Filter)

    // Early exit once the configured count is reached
}
```

**percentageOfNodesToScore:** adjusts the fraction of nodes evaluated based on cluster size
- 100 nodes or fewer: 100%
- 5000 nodes: about 10%
- Minimum: 5%

**Key Filter plugins:**

| Plugin | Role |
|---------|------|
| NodeUnschedulable | Check `spec.unschedulable` |
| NodeName | Check the `spec.nodeName` assignment |
| NodePorts | Check for port conflicts |
| NodeAffinity | Check node affinity/anti-affinity |
| TaintToleration | Match taints/tolerations |
| NodeResourcesFit | Check CPU/Memory/GPU resources |
| VolumeBinding | PVC volume attachability |
| InterPodAffinity | Inter-Pod affinity/anti-affinity |
| PodTopologySpread | Topology spread constraints |

#### [3c] Score Plugins

**File:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L943)

```go
// Lines 943-1000+: prioritizeNodes()
func prioritizeNodes(ctx, extenders, fwk, state, pod, nodes) ([]framework.NodePluginScores, error) {

    // PreScore: prepare shared state (line 966)
    fwk.RunPreScorePlugins(ctx, state, pod, nodes)

    // Score: compute scores for each node (line 972)
    nodesScores, err := fwk.RunScorePlugins(ctx, state, pod, nodes)

    // Add scores from external Extenders
    for _, extender := range extenders {
        extender.Prioritize(pod, nodes)
    }
}
```

**Score computation process:**
```
PreScore (build shared cache)
    ↓
Score (0-100 points per plugin)
    ↓
NormalizeScore (normalization: relative comparison within a plugin)
    ↓
Apply weights: plugin score × weight
    ↓
Sum: final score per node
    ↓
Select highest-scoring node (random on tie)
```

**Key Score plugins:**

| Plugin | Role | Weight (default) |
|---------|------|------------|
| NodeAffinity | Preferred node affinity score | 2 |
| NodeResourcesFit | Resource fit (LeastAllocated/MostAllocated) | 1 |
| InterPodAffinity | Inter-Pod affinity score | 2 |
| PodTopologySpread | Even topology spreading | 2 |
| ImageLocality | Prefer nodes that already have the image | 1 |
| TaintToleration | Toleration preference | 1 |

#### [3d] PostFilter (Preemption)

Runs when scheduling fails (all nodes rejected by Filter):

```go
// Lines 293-308: inside schedulingAlgorithm()
status := fwk.RunPostFilterPlugins(ctx, state, pod, diagnosis.NodeToStatus)
```

**DefaultPreemption plugin:**
1. Find nodes with lower-priority Pods
2. Simulate whether scheduling succeeds if those Pods are removed
3. If feasible, set `nominatedNodeName`
4. Perform the actual preemption in the next cycle

#### [3e] Assume & Reserve

**File:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L313)

```go
// Lines 313-359: assumeAndReserve()

// 1. Assume: reserve the Pod-node assignment in the cache (line 326)
sched.Cache.AssumePod(assumedPod)

// 2. Reserve: reserve resources (line 337)
fwk.RunReservePluginsReserve(ctx, state, assumedPod, scheduleResult.SuggestedHost)
// └─ VolumeBinding: reserve PVC binding
// └─ NodeResources: reserve resources in memory
```

**Lines 1108-1143: assume()**
```go
// Set the node name on the Pod (cache only, before the actual API update)
assumedPodInfo.Pod.Spec.NodeName = host  // line 1113
sched.Cache.AssumePod(assumedPodInfo.Pod) // lines 1132-1135
```

---

### [4] Binding Cycle

**File:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L397)

```go
// Lines 397-502: bindingCycle() — runs in a separate goroutine

// 1. PreBindPreFlight: update nominatedNodeName (line 411)
fwk.RunPreBindPreFlights(ctx, state, assumedPod, scheduleResult.SuggestedHost)

// 2. Permit: wait for final approval (line 431)
fwk.WaitOnPermit(ctx, assumedPod)

// 3. Done: remove in-flight entry from the queue (line 453)
sched.SchedulingQueue.Done(assumedPod.UID)

// 4. PreBind: pre-bind processing (line 464)
fwk.RunPreBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)
// └─ VolumeBinding.PreBind: bind PVCs to actual PVs

// 5. Bind: set nodeName on the API server (line 476)
sched.bind(ctx, fwk, assumedPod, scheduleResult.SuggestedHost, state)

// 6. PostBind (line 493)
fwk.RunPostBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)
```

**Lines 1154-1177: bind()**
```go
func (sched *Scheduler) bind(ctx, fwk, assumed, targetNode, state) error {
    // Try Extender binding
    for _, extender := range fwk.HasFilterPlugins() {
        if extender.IsInterested(assumed) {
            return extender.Bind(binding)  // external scheduler performs the binding
        }
    }
    // Use the default DefaultBinder
    return fwk.RunBindPlugins(ctx, state, assumed, targetNode)
}
```

**DefaultBinder:**
```go
// Update the Pod on the API server:
// PATCH /api/v1/namespaces/{ns}/pods/{name}/binding
// body: {"target": {"name": "node-1"}}
```

---

### Complete List of Extension Points

**File:** [pkg/scheduler/framework/interface.go](../pkg/scheduler/framework/interface.go#L180)

```
PreEnqueue  → check before entering the queue
Sort        → determine sort order within the queue
PreFilter   → prepare Pod-level state before the cycle starts
Filter      → per-node feasibility check
PostFilter  → when all nodes fail (preemption)
PreScore    → prepare shared state before Score
Score       → compute per-node scores
NormalizeScore → normalize scores
Reserve     → reserve resources
Permit      → approve/wait/deny binding
PreBind     → pre-bind processing
Bind        → actual binding
PostBind    → post-bind cleanup
Unreserve   → roll back Reserve (on failure)
```

Each extension point is its own interface (`FilterPlugin`, `ScorePlugin`, `PreBindPlugin`, …), and every in-tree plugin satisfies one *implicitly* — there is no `implements` keyword to grep for. To jump from a plugin interface to its concrete struct, search for the compile-time assertion the plugins declare:

```bash
# All concrete plugins that implement a given extension point
rg "var _ framework.PlacementFeasiblePlugin = &" pkg/scheduler/framework/plugins
```

For example, `GangScheduling` is the struct behind `PlacementFeasiblePlugin` — [gangscheduling.go:58](../pkg/scheduler/framework/plugins/gangscheduling/gangscheduling.go#L58). The same trick (`var _ <Interface> = &<Struct>{}`) locates the implementer for any extension point.

---

### Failure Handling and Requeueing

**File:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L1200)

```go
// Lines 1200-1250+: handleSchedulingFailure()
func handleSchedulingFailure(ctx, podFwk, podInfo, status, ...) {

    // 1. Update Pod condition (PodScheduled=False)
    // 2. Record an event
    // 3. Requeue
    sched.SchedulingQueue.AddUnschedulableIfNotPresent(podInfo, ...)
}
```

**Event-driven requeueing:** reactivates unschedulable Pods when the cluster state changes:
- Node added/updated
- Another Pod deleted (frees resources)
- PVC bound
- Other resource changes

---

## Performance Optimizations

| Optimization | Location | Effect |
|--------|------|------|
| Parallel Filter | `Parallelizer().Until()` | Throughput scales with node count |
| percentageOfNodesToScore | `numFeasibleNodesToScore` | Limits evaluated nodes in large clusters |
| OpportunisticBatching | Lines 596-616 | Reuses cached results for Pods with identical specs |
| Backoff & Unschedulable Pool | PriorityQueue | Prevents immediate retry of failed Pods |
| Snapshot-based cache | `Cache.UpdateSnapshot()` | Fast lock-free node lookups |

---

## Key File Path Summary

| Step | File | Key Function | Line |
|------|------|----------|------|
| Queue | [pkg/scheduler/backend/queue/scheduling_queue.go](../pkg/scheduler/backend/queue/scheduling_queue.go) | `PriorityQueue.Add/Pop` | 728, 945 |
| Main loop | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `ScheduleOne` | 67 |
| Scheduling cycle | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `schedulingCycle` | 175 |
| Filter | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `findNodesThatPassFilters` | 777 |
| Score | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `prioritizeNodes` | 943 |
| Assume | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `assumeAndReserve` | 313 |
| Binding | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `bindingCycle` | 397 |
| Framework | [pkg/scheduler/framework/runtime/framework.go](../pkg/scheduler/framework/runtime/framework.go) | `Framework` implementation | - |
| Plugins | [pkg/scheduler/framework/plugins/](../pkg/scheduler/framework/plugins/) | Individual plugin implementations | - |

---

## Related Concepts

- **Why split scheduling and binding cycles?** The scheduling cycle is synchronous and CPU-bound (a pure placement decision); the binding cycle is asynchronous and I/O-bound (PVC binding, the API `Bind` write). Separating them keeps node selection fast and serialized while slow API calls run in the background.
- **Predicates/priorities → the Scheduling Framework.** Older schedulers hard-coded "predicates" (hard filters) and "priorities" (soft scores). Today every decision is a plugin implementing an extension-point interface (`PreFilter`, `Filter`, `Score`, `Reserve`, `Permit`, `Bind`, …) selected per profile — which is why this trace keeps crossing interfaces.
- **Filter vs. Score.** Filter answers "*can* this Pod run here?" (yes/no, narrows candidates); Score answers "*how good* is this node?" (0–100, ranks survivors). The highest total score wins.
- **Assume + optimistic cache.** Once a node is chosen, the scheduler writes the assignment into its in-memory cache ("assume") so the *next* Pod already sees those resources consumed — before the API bind completes. A failed bind triggers `Unreserve`/forget to roll it back.
- **Preemption & PriorityClass.** If nothing fits, `PostFilter` (DefaultPreemption) may evict lower-priority Pods to make room; `nominatedNodeName` marks the intended node while victims drain.
- **`PodScheduled` condition.** The outcome surfaces as a Pod condition — `True` on success, `False` with a reason on failure — which is exactly what `kubectl describe pod` prints under Events.
- **`percentageOfNodesToScore`.** In large clusters the scheduler stops after finding "enough" feasible nodes instead of scoring every node, trading perfect placement for latency.

> ⚠️ **The scheduler does not start containers.** It only writes `pod.spec.nodeName` (the Bind). The kubelet on that node (Scenario 4) is what actually pulls images and runs containers.

---

## Related Scenarios

- [Scenario 1: API Request Flow](01-api-request-flow.md) — how a Pod is stored in etcd
- [Scenario 4: kubelet Pod Lifecycle](04-kubelet-pod-lifecycle.md) — how the kubelet runs a bound Pod
