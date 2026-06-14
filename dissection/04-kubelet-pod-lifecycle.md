# Scenario 4: kubelet Pod Lifecycle

Traces the full flow from the moment the scheduler sets nodeName on a Pod through the kubelet running and terminating it.

## Big Picture

The kubelet is a per-node reconciliation engine. It turns desired Pod specs into runtime reality by coordinating three layers: pod worker state machine, CRI runtime operations (sandbox/container lifecycle), and node-local side systems (volumes, probes, status updates). Termination follows the same pattern in reverse, with explicit cleanup checkpoints.

## Interface Resolution Guide (This Scenario)

Kubelet code combines interfaces with constructor wiring. Use this order:
1. Find the interface field on `Kubelet` or runtime manager.
2. Check for compile-time assertions.
3. If absent, follow constructor signatures and assignment lines.
4. Confirm the concrete implementation used at the call site (`SyncPod`, `RunPodSandbox`, `StartContainer`, etc.).

### Worked Example: `kubecontainer.Runtime` -> `*kubeGenericRuntimeManager`

This is the runtime boundary that controls most of the Pod lifecycle:

1. Interface: `type Runtime interface` in [../pkg/kubelet/container/runtime.go](../pkg/kubelet/container/runtime.go#L74).
2. Concrete struct: `kubeGenericRuntimeManager` in [../pkg/kubelet/kuberuntime/kuberuntime_manager.go](../pkg/kubelet/kuberuntime/kuberuntime_manager.go#L114).
3. Factory that hides the type: `kuberuntime.NewKubeGenericRuntimeManager(...)` is called in [../pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L784) and returns a `kubecontainer.Runtime` interface value.
4. Assignment wiring: kubelet stores that value in `klet.containerRuntime` in [../pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L829).
5. Runtime call site: the main sync path invokes `kl.containerRuntime.SyncPod(...)` in [../pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L2251), which therefore runs the concrete `kubeGenericRuntimeManager` implementation.

There is no compile-time assertion breadcrumb here. Constructor return type plus field assignment is the proof.

## Reading Guide (Beginner)

- **Trigger:** kubelet receives Pod updates (API watch, static pod source, PLEG resync, or probe-related resync).
- **Core loop model:** each sync computes desired-vs-actual delta, then applies only required runtime actions.
- **Probe worker detail:** probe workers are created in `AddPod()` via `newWorker(...)` + `go w.run(ctx)`; there is no `AddWorker` API in this path.
- **Termination model:** graceful stop is an explicit state transition (`TerminatingPod` -> `TerminatedPod`) with separate cleanup phase.
- **Success criterion for this scenario:** runtime/container state, volumes, and reported Pod status converge to current Pod intent.

## Overall Flow

```
Scheduler: sets Pod.Spec.NodeName = "node-1"
        │ (Watch event)
        ▼
[1] pkg/kubelet/kubelet.go:Run()
    Initialization: volumeManager, statusManager, pleg.Start()
        │
        ▼
[2] syncLoop() → syncLoopIteration()
    configCh event → HandlePodAdditions()
        │
        ▼
[3] pkg/kubelet/pod_workers.go:UpdatePod()
    Per-Pod goroutine → podWorkerLoop()
        │
        ├─ State: SyncPod
        │       ↓
[4] pkg/kubelet/kuberuntime/kuberuntime_manager.go:SyncPod()
        │
        ├─ [4a] computePodActions() — compute required changes
        ├─ [4b] createPodSandbox() — networking setup (CNI)
        ├─ [4c] startContainer() — start container via CRI call
        │       ├─ EnsureImageExists() — image pull
        │       ├─ CreateContainer() — CRI gRPC
        │       ├─ StartContainer() — CRI gRPC
        │       └─ Lifecycle.PostStart hook
        │
        ├─ [4d] volumeManager (parallel): Attach → Mount
        │
        └─ [4e] probeManager: start liveness / readiness / startup probes
        │
        ▼
[5] Termination flow:
    DeletionTimestamp detected → TerminatingPod
        │
        ├─ SyncTerminatingPod()
        │   ├─ probeManager.StopLivenessAndStartup()
        │   ├─ killPod() (SIGTERM → grace period → SIGKILL)
        │   └─ probeManager.RemovePod()
        │
        └─ SyncTerminatedPod()
            ├─ volumeManager.WaitForUnmount()
            ├─ clean up volume directories
            ├─ remove cgroups
            └─ statusManager.TerminatePod()
```

---

## Step-by-Step Analysis

### [1] kubelet Initialization

**File:** [cmd/kubelet/kubelet.go](../cmd/kubelet/kubelet.go#L35)

```go
// Lines 35-39: main()
func main() {
    command := app.NewKubeletCommand()
    code := cli.Run(command)
    os.Exit(code)
}
```

**File:** [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L1858)

```go
// Lines 1858-1976: Run()
func (kl *Kubelet) Run(ctx context.Context, updates <-chan kubetypes.PodUpdate) {

    // Line 1899: initialize internal modules
    kl.initializeModules(ctx)

    // Line 1915: start the volume manager (separate goroutine)
    go kl.volumeManager.Run(ctx, sourcesReady)

    // Line 1956: start the status manager (sends status to the API server)
    kl.statusManager.Start(ctx)

    // Line 1964: start PLEG (Pod Lifecycle Event Generator)
    kl.pleg.Start()

    // Line 1975: enter the main sync loop (blocking)
    kl.syncLoop(ctx, updates, kl)
}
```

**Key Kubelet members (lines 1193-1400+):**

| Member | Role |
|------|------|
| `podManager` | Tracks Pod metadata |
| `podWorkers` | Manages per-Pod goroutines |
| `volumeManager` | Volume Attach/Mount/Unmount |
| `probeManager` | liveness/readiness/startup probes |
| `statusManager` | Sends Pod status to the API server |
| `containerRuntime` | CRI interface (containerd/cri-o) |
| `pleg` | Detects runtime state changes |

> ⚠️ **`containerRuntime` is an interface — the real type is `kubeGenericRuntimeManager`.** The field is typed `containerRuntime kubecontainer.Runtime` ([kubelet.go:1404](../pkg/kubelet/kubelet.go#L1404)); the interface lives at [container/runtime.go:74](../pkg/kubelet/container/runtime.go#L74). The concrete struct is `kubeGenericRuntimeManager` ([kuberuntime_manager.go:114](../pkg/kubelet/kuberuntime/kuberuntime_manager.go#L114)). In this path, there is no compile-time `var _ kubecontainer.Runtime = &kubeGenericRuntimeManager{}` assertion to grep for. Instead, prove the binding through wiring: `NewKubeGenericRuntimeManager(...)` returns `kubecontainer.Runtime` ([kubelet.go:784](../pkg/kubelet/kubelet.go#L784)), and that value is assigned to `klet.containerRuntime` ([kubelet.go:829](../pkg/kubelet/kubelet.go#L829)). When the assertion is absent, constructor signature + assignment is the reliable trace.

---

### [PLEG] Pod Lifecycle Event Generator — How the kubelet Detects Runtime State Changes

**File:** [pkg/kubelet/pleg/generic.go](../pkg/kubelet/pleg/generic.go#L53)

PLEG bridges the gap between kube-apiserver-based Pod spec changes and the actual state of containers in the runtime. Instead of reading container state on every sync, the kubelet has a single PLEG goroutine that periodically lists runtime state, diffs it against the previous snapshot, and emits typed events that the main `syncLoop` consumes.

> ⚠️ **`kl.pleg` is typed as `PodLifecycleEventGenerator` (interface).** The field is declared at [kubelet.go:1416](../pkg/kubelet/kubelet.go#L1416). The concrete struct in this path is `GenericPLEG` ([generic.go:53](../pkg/kubelet/pleg/generic.go#L53)), constructed and assigned in the kubelet constructor. `GenericPLEG` is called "generic" because it works with any CRI runtime, using only periodic list rather than native runtime event streams.

**`GenericPLEG` struct (lines 53-90):**

```go
type GenericPLEG struct {
    runtime      kubecontainer.Runtime  // CRI bridge — same containerRuntime as kubelet
    eventChannel chan *PodLifecycleEvent // events consumed by syncLoopIteration via plegCh
    podRecords   podRecords             // map[podUID]podRecord{old, current}
    cache        kubecontainer.Cache    // pod status cache shared with kubelet
    relistDuration *RelistDuration      // configurable relist interval
    relistRequests chan relistRequest    // on-demand relist trigger
}
```

**`Relist()` — the core polling function (line 289):**

```go
func (g *GenericPLEG) Relist(ctx context.Context) {
    g.relistLock.Lock()
    defer g.relistLock.Unlock()

    // 1. Ask the CRI runtime for all pods + containers (line 303)
    podList, err := g.runtime.GetPods(ctx, true)
    // └─ gRPC ListPodSandbox() + ListContainers() on containerd/cri-o

    g.podRecords.setCurrent(pods)

    // 2. For each pod that had any state in old or new snapshot,
    //    compute events and update the cache (line 313)
    for pid := range g.podRecords {
        g.reconcilePodRecord(ctx, pid)
    }

    // 3. Advance the cache timestamp so kubelet's WaitForCacheSync can unblock
    g.cache.UpdateTime(timestamp)
}
```

**`reconcilePodRecord()` — diff old vs new state (line 330):**

```go
func (g *GenericPLEG) reconcilePodRecord(ctx, pid) {
    oldPod := g.podRecords.getOld(pid)
    pod    := g.podRecords.getCurrent(pid)

    // Walk every container in old ∪ new and compute transition events
    for _, container := range getContainersFromPods(oldPod, pod) {
        containerEvents := computeEvents(logger, oldPod, pod, &container.ID)
        events = append(events, containerEvents...)
    }

    // If there are events (or a reinspect was queued), update the status cache
    status, updated, err := g.updateCache(ctx, pod, pid)
    // └─ calls runtime.GetPodStatus() via CRI gRPC
    // └─ stores result in g.cache for kubelet to read

    // Emit events onto the channel that syncLoopIteration reads (plegCh)
    for _, event := range events {
        g.eventChannel <- event
    }
    g.podRecords.update(pid)  // promote current → old for next cycle
}
```

**PLEG event types:**

| Event | Meaning |
|-------|---------|
| `ContainerStarted` | Container transitioned to running |
| `ContainerDied` | Container exited (exit code attached as `Data`) |
| `ContainerRemoved` | Container no longer in runtime list |
| `PodSync` | Reinspect requested without a specific container change |

`ContainerChanged` events are **filtered out** before emission — they are too noisy and no kubelet component consumes them.

**How `syncLoopIteration` consumes PLEG events:**

```go
// From syncLoopIteration (kubelet.go:2703)
case e := <-plegCh:
    if isSyncPodWorthy(e) {
        // Look up the Pod in podManager and trigger a SyncPod pass
        kl.HandlePodSyncs([]*v1.Pod{pod})
    }
```

`isSyncPodWorthy` returns true for `ContainerDied` and `ContainerStarted` but not for `ContainerRemoved` alone — removals are handled by the housekeeping path.

**Relist frequency:**

The relist period (default **1 second**) is configurable via `--relist-period` on the kubelet. If a relist takes longer than one period, the next scheduled relist is skipped to avoid queue buildup. PLEG health is surfaced via `kubelet_pleg_relist_duration_seconds` and the `PLEG is not healthy` log line that triggers a node `NotReady` condition.

---

### [2] SyncLoop — Main Event Loop

**File:** [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L2703)

```go
// Lines 2703-2823: syncLoopIteration() — handles 5 channels

// 1. configCh: receives Pod changes from the API server or file/HTTP
case u, open := <-configCh:
    switch u.Op {
    case kubetypes.ADD:
        kl.HandlePodAdditions(u.Pods)
    case kubetypes.UPDATE:
        kl.HandlePodUpdates(u.Pods)
    case kubetypes.REMOVE:
        kl.HandlePodRemoves(u.Pods)
    case kubetypes.RECONCILE:
        kl.HandlePodReconcile(u.Pods)
    }

// 2. plegCh: detects container runtime state changes
case e := <-plegCh:
    if isSyncPodWorthy(e) {
        kl.HandlePodSyncs([]*v1.Pod{pod})
    }

// 3. syncCh: periodic resync every 1 second
case <-syncCh:
    podsToSync := kl.getPodsToSync()
    kl.HandlePodSyncs(podsToSync)

// 4. Probe updates
case update := <-kl.livenessManager.Updates():
    if update.Result == proberesults.Failure {
        kl.HandleProbeSync(pod)  // liveness failure → restart Pod
    }
case update := <-kl.readinessManager.Updates():
    kl.statusManager.SetContainerReadiness(...)  // only updates ready state

// 5. housekeepingCh: cleanup every 2 seconds
case <-housekeepingCh:
    kl.HandlePodCleanups(ctx)
```

---

### [3] Pod Worker — State Machine

**File:** [pkg/kubelet/pod_workers.go](../pkg/kubelet/pod_workers.go#L108)

**Pod states (lines 108-119):**
```go
type PodWorkerState int
const (
    SyncPod        PodWorkerState = iota  // running
    TerminatingPod                         // stopping containers
    TerminatedPod                          // fully terminated, cleaning up resources
)
```

**UpdatePod() (lines 751-900+):**
```go
func (p *podWorkers) UpdatePod(ctx context.Context, options UpdatePodOptions) {
    // Create a goroutine when the Pod first appears (lines 782-816)
    if !podUpdates, exists := p.podUpdates[uid]; !exists {
        podUpdates = make(chan struct{}, 1)
        p.podUpdates[uid] = podUpdates
        go p.podWorkerLoop(ctx, uid, podUpdates)  // per-Pod goroutine
    }

    // Detect transition to termination (lines 862-890)
    if options.RunningPod != nil || d.DeletionTimestamp != nil ||
       isTerminalPhase(d.Phase) || options.UpdateType == SyncPodKill {
        // transition state to TerminatingPod
    }

    // wake the worker
    select {
    case podUpdates <- struct{}{}:
    default:  // ignore if already queued
    }
}
```

**podWorkerLoop() (lines 1231-1363):**
```go
// Runs in a per-Pod goroutine
for range podUpdates {
    status, shouldSync, shouldTerminate := p.startPodSync(...)

    switch {
    case status == TerminatedPod:
        p.podSyncer.SyncTerminatedPod(ctx, pod, status)  // resource cleanup
    case status == TerminatingPod:
        p.podSyncer.SyncTerminatingPod(ctx, pod, status, ...)  // stop containers
    default:
        p.podSyncer.SyncPod(ctx, updateType, pod, mirrorPod, status)  // run
    }
}
```

---

### [4] SyncPod — Running Containers

**File:** [pkg/kubelet/kuberuntime/kuberuntime_manager.go](../pkg/kubelet/kuberuntime/kuberuntime_manager.go#L1450)

```go
// Lines 1450-1800+: SyncPod()
func (m *kubeGenericRuntimeManager) SyncPod(ctx, pod, podStatus, auth, backOff) PodSyncResult {

    // 1. Compute changes (line 1453)
    podContainerChanges := m.computePodActions(ctx, pod, podStatus)
    // Returns: containers to recreate, whether init-container state requires re-running init flow,
    // and whether the Pod sandbox must be recreated (KillPod=true).

    // 2. If the sandbox must change, kill the entire existing Pod (lines 1468-1480)
    if podContainerChanges.KillPod {
        m.killPodWithSyncResult(ctx, pod, podStatus, nil)
    }

    // 3. Kill unwanted containers (lines 1487-1496)
    for _, c := range podContainerChanges.ContainersToKill {
        m.killContainer(ctx, pod, c.ID, ...)
    }

    // 4. Create the Pod sandbox (lines 1545-1638)
    podSandboxID, msg, err := m.createPodSandbox(ctx, pod, podContainerChanges.Attempt)
    // └─ RunPodSandbox() CRI gRPC call
    // └─ CNI plugin invocation → allocates Pod IP, creates network interface

    // 5. Run init containers sequentially (lines 1682-1700)
    for _, container := range pod.Spec.InitContainers {
        m.startContainer(ctx, podSandboxID, podSandboxConfig, &container, pod, ...)
        // move on to the next init container after completion
    }

    // 6. Run regular containers (lines 1745-1800)
    for _, container := range pod.Spec.Containers {
        m.startContainer(ctx, podSandboxID, podSandboxConfig, &container, pod, ...)
    }
}
```

#### [4b] Pod Sandbox Creation

```go
// createPodSandbox() — main responsibilities:
// 1. Look up the RuntimeClass
// 2. CRI: RunPodSandbox() → creates the pause container (infra container)
// 3. CNI: Pod IP allocation + veth pair + iptables rules
// Result: podSandboxID (all subsequent containers share this sandbox)
```

#### [4c] startContainer() — Starting a Container

**File:** [pkg/kubelet/kuberuntime/kuberuntime_container.go](../pkg/kubelet/kuberuntime/kuberuntime_container.go#L199)

```go
// Lines 199-339: startContainer()
func (m *kubeGenericRuntimeManager) startContainer(ctx, podSandboxID, podSandboxConfig,
    spec *v1.Container, pod *v1.Pod, ...) (string, error) {

    // Step 1: Check and pull the image (lines 203-219)
    imageRef, msg, err := m.imagePuller.EnsureImageExists(ctx, pod, spec, ...)
    // └─ PullImage() CRI gRPC or cache hit

    // Step 2: Generate the container config (lines 221-288)
    containerConfig, cleanupAction, err := m.generateContainerConfig(ctx, spec, pod, ...)
    // Includes: environment variables (ConfigMap/Secret), volume mounts, security context, resource limits

    // Step 3: PreCreateContainer hook (line 269)
    m.internalLifecycle.PreCreateContainer(pod, spec, containerConfig)

    // Step 4: CRI CreateContainer (line 276)
    containerID, err := m.runtimeService.CreateContainer(ctx, podSandboxID, containerConfig, podSandboxConfig)
    // └─ gRPC → containerd/cri-o → OCI runtime (runc) → creates namespaces/cgroups

    // Step 5: PreStartContainer hook (line 282)
    m.internalLifecycle.PreStartContainer(pod, spec, containerID)

    // Step 6: CRI StartContainer (line 291)
    err = m.runtimeService.StartContainer(ctx, containerID)
    // └─ actually starts the process (runs the entrypoint)

    // Step 7: PostStart lifecycle hook (lines 318-336)
    if container.Lifecycle != nil && container.Lifecycle.PostStart != nil {
        handlerErr := m.runner.Run(ctx, containerID, pod, spec, spec.Lifecycle.PostStart)
        if handlerErr != nil {
            m.killContainer(...)  // PostStart failure → kill the container
        }
    }
}
```

---

### [4d] Volume Management

**File:** [pkg/kubelet/volumemanager/volume_manager.go](../pkg/kubelet/volumemanager/volume_manager.go#L298)

```go
// Lines 298-317: Run()
func (vm *volumeManager) Run(ctx, sourcesReady) {

    // Watch CSI driver info (line 304)
    go vm.volumePluginMgr.Run(ctx)

    // Track desired volume state (line 307)
    go vm.desiredStateOfWorldPopulator.Run(ctx, sourcesReady)
    // └─ reads volumes from the Pod spec to build the DesiredStateOfWorld

    // Reconcile with actual state (line 311)
    go vm.reconciler.Run(ctx)
    // └─ executes in order: Attach → WaitForAttach → Mount
    // └─ on deletion: Unmount → Detach
}
```

**Waiting for volumes within SyncPod:**
```go
// Called from kubelet.go before SyncPod
kl.volumeManager.WaitForAttachAndMount(ctx, pod)
// Blocks until all volumes are mounted (with a timeout)
```

---

### [4e] Probe Management

**File:** [pkg/kubelet/prober/prober_manager.go](../pkg/kubelet/prober/prober_manager.go#L185)

```go
// Lines 185-230: AddPod()
func (m *manager) AddPod(ctx context.Context, pod *v1.Pod) {
    // Regular containers + restartable init containers get probe workers.
    for _, c := range append(pod.Spec.Containers, getRestartableInitContainers(pod)...) {
        if c.StartupProbe != nil {
            w := newWorker(m, startup, pod, c)
            m.workers[key] = w
            go w.run(ctx)
        }
        if c.ReadinessProbe != nil {
            w := newWorker(m, readiness, pod, c)
            m.workers[key] = w
            go w.run(ctx)
        }
        if c.LivenessProbe != nil {
            w := newWorker(m, liveness, pod, c)
            m.workers[key] = w
            go w.run(ctx)
        }
    }
}
```

> ⚠️ **There is no `AddWorker` method in this path.** Worker creation is inline inside `AddPod`: `newWorker(...)` then `go w.run(ctx)` in [prober_manager.go:200](../pkg/kubelet/prober/prober_manager.go#L200), [prober_manager.go:214](../pkg/kubelet/prober/prober_manager.go#L214), and [prober_manager.go:228](../pkg/kubelet/prober/prober_manager.go#L228).

**File:** [pkg/kubelet/prober/worker.go](../pkg/kubelet/prober/worker.go)

**Probe worker behavior:**
```
1. Wait initialDelaySeconds
2. Run the probe periodically (HTTP/TCP/gRPC/Exec)
3. Store the result in resultsManager
4. Result detected in syncLoopIteration:
   - startup failure → restart container (restartPolicy applies)
   - liveness failure → restart container
    - readiness change → status update (affects traffic routing)
5. `UpdatePodStatus()` can trigger an immediate readiness probe run (`manualTriggerCh`) to reduce stale readiness windows
```

**Behavior by probe type:**

| Probe | On Failure | On Success |
|-------|------------|------------|
| `startupProbe` | Restart container (liveness/readiness disabled) | Enable liveness/readiness |
| `livenessProbe` | Restart container | Ignored |
| `readinessProbe` | Ready=False (removed from Service endpoints) | Ready=True |

---

### [5] Graceful Termination Flow

#### SyncTerminatingPod

**File:** [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L2297)

```go
// Lines 2297-2416: SyncTerminatingPod()
func (kl *Kubelet) SyncTerminatingPod(ctx, pod, podStatus, gracePeriod, podStatusFn) error {

    // 1. Record the final status (line 2319)
    kl.statusManager.SetPodStatus(ctx, pod, apiPodStatus)

    // 2. Stop liveness/startup probes (line 2331)
    kl.probeManager.StopLivenessAndStartup(pod)
    // readiness keeps running (for observation)

    // 3. Kill the Pod (line 2334)
    kl.killPod(ctx, pod, podStatus, gracePeriod)
    // ↓
    // kuberuntime_manager.go:KillPodByID()
    //   └─ send SIGTERM to each container
    //   └─ wait gracePeriod seconds (default 30s)
    //   └─ SIGKILL remaining containers
    //   └─ StopPodSandbox() CRI gRPC

    // 4. Remove all probes (line 2344)
    kl.probeManager.RemovePod(pod)

    // 5. Verify all containers are stopped (lines 2365-2410)
    // Return an error if any container is still running (retried)
}
```

**Graceful termination timeline:**
```
DeletionTimestamp set
    ├─ run PreStop hook (if present)
    ├─ send SIGTERM
    ├─ wait terminationGracePeriodSeconds (default 30s)
    └─ send SIGKILL (if still running)
```

**Shortening forced termination:**
```
actual grace period = min(
    pod.Spec.TerminationGracePeriodSeconds,
    gracePeriodSeconds from the --force-delete flag  ← kubectl delete --grace-period=0
)
```

#### SyncTerminatedPod — Resource Cleanup

**File:** [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L2466)

```go
// Lines 2466-2533: SyncTerminatedPod()
func (kl *Kubelet) SyncTerminatedPod(ctx, pod, podStatus) error {

    // 1. Mark the final status (line 2480)
    kl.statusManager.SetPodStatus(ctx, pod, apiPodStatus)

    // 2. Wait for volume unmount (line 2486)
    kl.volumeManager.WaitForUnmount(ctx, pod)

    // 3. Delete volume data directories (lines 2493-2502)
    kl.removeOrphanedPodVolumeDirs(pod.UID)

    // 4. Unregister Secret/ConfigMap (lines 2505-2510)
    kl.secretManager.UnregisterPod(pod)
    kl.configMapManager.UnregisterPod(pod)

    // 5. Remove cgroups (lines 2517-2524)
    pcm.Destroy(...)  // delete CPU/memory cgroups

    // 6. Release the user namespace (line 2526, if enabled)
    kl.usernsManager.Release(pod.UID)

    // 7. Mark final termination (line 2529)
    kl.statusManager.TerminatePod(logger, pod)
}
```

---

### CRI (Container Runtime Interface) Communication

The gRPC interface between the kubelet and the container runtime (containerd, CRI-O):

```
kubelet
    │ gRPC (unix socket)
    ▼
CRI runtime (containerd / CRI-O)
    │
    ├─ RuntimeService:
    │   RunPodSandbox()       → create pause container
    │   CreateContainer()     → create OCI spec
    │   StartContainer()      → run runc
    │   StopContainer()       → SIGTERM/SIGKILL
    │   RemoveContainer()     → remove container
    │   StopPodSandbox()      → network cleanup
    │
    └─ ImageService:
        PullImage()            → download image
        ListImages()           → list local images
        RemoveImage()          → delete image
```

> ⚠️ **Behind `RuntimeService`/`ImageService`.** These are the CRI interfaces (`internalapi.RuntimeService`, held as `runtimeService internalapi.RuntimeService` at [kubelet.go:1410](../pkg/kubelet/kubelet.go#L1410)). The concrete client that actually speaks gRPC is `remoteRuntimeService` ([cri-client/pkg/remote_runtime.go:48](../staging/src/k8s.io/cri-client/pkg/remote_runtime.go#L48)), built by `NewRemoteRuntimeService(...)` ([remote_runtime.go:232](../staging/src/k8s.io/cri-client/pkg/remote_runtime.go#L232)). So `RunPodSandbox()` and friends are interface method calls that ultimately hit the gRPC stub inside `remoteRuntimeService` — swappable with a fake in tests.

---

## Pod State Machine Summary

```
                       [scheduler binding]
                              │
                              ▼
Pending ──────────────────────────────────────────────────────→ Running
                              │
                [SyncPod: start sandbox + containers]
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
  [all completed]    [liveness failure]  [DeletionTimestamp]
         │               [OOMKilled]          │
         ▼                    │               ▼
     Succeeded        [restartPolicy]   Terminating
                             │                │ [SIGTERM→SIGKILL]
                    ┌────────┴────────┐        │
                    ▼                ▼         ▼
                 Always         OnFailure   Terminated
                 restart          restart  [resource cleanup]
                                  │              │
                               Never            ▼
                               stop        Pod deleted
```

---

## Key File Path Summary

| Step | File | Key Function | Line |
|------|------|----------|------|
| Entry point | [cmd/kubelet/kubelet.go](../cmd/kubelet/kubelet.go) | `main` | 35 |
| Initialization | [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go) | `Run` | 1858 |
| Main loop | [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go) | `syncLoopIteration` | 2703 |
| Pod Worker | [pkg/kubelet/pod_workers.go](../pkg/kubelet/pod_workers.go) | `UpdatePod`, `podWorkerLoop` | 751, 1231 |
| SyncPod | [pkg/kubelet/kuberuntime/kuberuntime_manager.go](../pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `SyncPod` | 1450 |
| Container start | [pkg/kubelet/kuberuntime/kuberuntime_container.go](../pkg/kubelet/kuberuntime/kuberuntime_container.go) | `startContainer` | 199 |
| Volume management | [pkg/kubelet/volumemanager/volume_manager.go](../pkg/kubelet/volumemanager/volume_manager.go) | `Run` | 298 |
| Probe management | [pkg/kubelet/prober/prober_manager.go](../pkg/kubelet/prober/prober_manager.go) | `AddPod` | 185 |
| Graceful termination | [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go) | `SyncTerminatingPod` | 2297 |
| Resource cleanup | [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go) | `SyncTerminatedPod` | 2466 |

---

## Related Concepts

- **CRI / OCI / runc.** The kubelet speaks the **CRI** gRPC API to a runtime (containerd, CRI-O); that runtime then drives an **OCI** runtime (runc) to create the actual Linux namespaces and cgroups. The kubelet never invokes runc directly — it only knows CRI.
- **Pod sandbox & the pause container.** A Pod's "sandbox" is a tiny **pause** container that owns the network namespace; the app containers *join* that namespace, which is why every container in a Pod shares one IP and can reach each other over `localhost`.
- **CNI.** Once the sandbox exists, a **CNI** plugin wires the Pod into the network (veth pair, IP allocation, routes). CNI runs at sandbox creation, not per app container.
- **PLEG (Pod Lifecycle Event Generator).** Rather than trusting every runtime event, the kubelet periodically *relists* container states and emits sync events on any drift — the safety net behind `plegCh` that catches missed transitions.
- **Static pods & mirror pods.** Pods sourced from a file/HTTP endpoint (not the API) are **static**; the kubelet publishes a read-only **mirror pod** to the API so they're visible. This is how kubeadm runs the control plane itself.
- **cgroups & QoS classes.** Requests/limits become cgroup settings; a Pod's QoS class (Guaranteed / Burstable / BestEffort), derived from those values, decides OOM-kill priority and eviction order under pressure.
- **Probe semantics.** Startup probes gate the others; a **liveness** failure *restarts* the container; a **readiness** failure only removes the Pod from Service endpoints (Scenario 5) without restarting. That asymmetry is the most common probe gotcha.

> ⚠️ **`SyncPod` is idempotent and convergent.** It is called repeatedly, and each call computes the delta between desired and actual container state (`computePodActions`). One call does not mean "start once" — it means "make reality match the spec right now."

---

## Related Scenarios

- [Scenario 2: Pod Scheduling](02-pod-scheduling.md) — the scheduling flow before the kubelet receives the Pod
- [Scenario 5: Service Network Routing](05-service-network-routing.md) — the relationship between readinessProbe and EndpointSlice
