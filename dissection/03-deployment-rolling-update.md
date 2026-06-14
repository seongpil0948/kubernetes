# Scenario 3: Deployment Rolling Update

Traces the flow in which, after `kubectl apply`, the controller creates/modifies ReplicaSets and replaces Pods when a Deployment changes.

## Big Picture

Rolling update is a reconciliation loop over two ReplicaSet groups: the new-template RS and old-template RSs. The Deployment controller continuously adjusts both sides to satisfy availability constraints (`maxSurge`, `maxUnavailable`), while the ReplicaSet controller performs the actual Pod create/delete operations.

## Interface and Indirection Guide (This Scenario)

This path uses less classic interface polymorphism and more controller indirection. Use this order:
1. Identify function-field indirection (`syncHandler`) and find its assignment.
2. Follow typed clients/listers to concrete API calls (`AppsV1().ReplicaSets().Create/Update`).
3. Confirm queue-driven re-entry points (`processNextWorkItem` -> sync function).
4. Treat controller ownership (`OwnerReferences`, adoption) as runtime dispatch logic for which object gets reconciled.

### Worked Example: `controller.PodControlInterface` -> `controller.RealPodControl`

The Deployment controller itself scales ReplicaSets, but the ReplicaSet controller uses this interface to do the Pod-level work:

1. Interface: `type PodControlInterface interface` in [../pkg/controller/controller_utils.go](../pkg/controller/controller_utils.go#L470).
2. Compile-time proof: `var _ PodControlInterface = &RealPodControl{}` in [../pkg/controller/controller_utils.go](../pkg/controller/controller_utils.go#L488).
3. Factory-style wiring: `NewReplicaSetController(...)` passes a concrete `controller.RealPodControl{...}` into `NewBaseController(...)` in [../pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go#L155) and [../pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go#L191).
4. Assignment wiring: `NewBaseController(...)` stores that value in `rsc.podControl` in [../pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go#L205).
5. Runtime call sites: `manageReplicas()` executes `rsc.podControl.CreatePods(...)` and `DeletePod(...)` in [../pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go#L678) and [../pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go#L726).

This is a good example of why wiring matters: many types could satisfy the interface, but this rollout path actually uses `RealPodControl`, which turns desired replica changes into real Pod create/delete API calls.

## Reading Guide (Beginner)

- **Trigger:** Deployment spec/template change or ReplicaSet/Pod change event requeues the Deployment key.
- **What the controller directly changes:** only ReplicaSet replica counts and Deployment status.
- **Where Pods are really created/deleted:** ReplicaSet controller performs Pod-level actions.
- **Success criterion for this scenario:** new ReplicaSet reaches desired replicas while old ReplicaSets scale down within rollout limits.
- **Most common confusion:** one sync does not finish a rollout; it converges over many queue iterations.

## Overall Flow Diagram

```
kubectl apply -f deployment-v2.yaml
        │
[1] Deployment update stored in API server
        │ (Watch event)
        ▼
[2] pkg/controller/deployment/deployment_controller.go
    updateDeployment() → add key to WorkQueue
        │
        ▼
[3] syncDeployment() — strategy branch
        │
        ├─ d.Spec.Strategy.Type == RollingUpdate
        │          ↓
[4] rolling.go:rolloutRolling()
        │
        ├─ [4a] getAllReplicaSetsAndSyncRevision()
        │        create new RS if absent (sync.go:getNewReplicaSet)
        │
        ├─ [4b] reconcileNewReplicaSet()
        │        scale up new RS
        │
        ├─ [4c] reconcileOldReplicaSets()
        │        scale down old RSs
        │
        └─ [4d] syncRolloutStatus()
                 update Deployment status
        │
        ▼ (ReplicaSet.Spec.Replicas changed)
[5] pkg/controller/replicaset/replica_set.go
    syncReplicaSet() → manageReplicas()
        │
        ├─ diff < 0: create Pods via slowStartBatch()
        └─ diff > 0: delete Pods in parallel
```

---

## Step-by-Step Detailed Analysis

### [2] DeploymentController Initialization and Event Handling

**File:** [pkg/controller/deployment/deployment_controller.go](../pkg/controller/deployment/deployment_controller.go#L104)

```go
// Lines 104-168: NewDeploymentController()
func NewDeploymentController(ctx, dInformer, rsInformer, podInformer, client) (*DeploymentController, error) {
    dc := &DeploymentController{
        client:        client,
        dLister:       dInformer.Lister(),
        rsLister:      rsInformer.Lister(),
        podIndexer:    podInformer.Informer().GetIndexer(),
        queue:         workqueue.NewTypedRateLimitingQueue(...),
    }

    // Register informer event handlers (lines 131-167)
    dInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc:    dc.addDeployment,    // Line 201: add to queue
        UpdateFunc: dc.updateDeployment, // Line 207: add to queue
        DeleteFunc: dc.deleteDeployment,
    })
}
```

**Processing loop:**
```go
// Lines 481-484: worker()
func (dc *DeploymentController) worker(ctx context.Context) {
    for dc.processNextWorkItem(ctx) {}
}

// Lines 486-497: processNextWorkItem()
key, _ := dc.queue.Get()
dc.syncHandler(ctx, key.(string))  // calls syncDeployment()
```

> ⚠️ **`syncHandler` is a function field, not a hard-coded call.** It is declared as `syncHandler func(ctx context.Context, dKey string) error` ([deployment_controller.go:76](../pkg/controller/deployment/deployment_controller.go#L76)) and wired in the constructor with `dc.syncHandler = dc.syncDeployment` ([deployment_controller.go:152](../pkg/controller/deployment/deployment_controller.go#L152)). This is the function-pointer form of dependency injection: to find what actually runs, grep for where the field is **assigned** (here, `syncDeployment` at line 574), not where it is called. Controllers use this indirection so tests can swap in a fake sync function.

---

### [3] syncDeployment() — Strategy Branch

**File:** [pkg/controller/deployment/deployment_controller.go](../pkg/controller/deployment/deployment_controller.go#L574)

```go
// Lines 574-660: syncDeployment()
func (dc *DeploymentController) syncDeployment(ctx context.Context, key string) error {

    // Line 588: look up the Deployment
    deployment, err := dc.dLister.Deployments(namespace).Get(name)

    // Line 612: list RSs and adopt orphan RSs
    rsList, err := dc.getReplicaSetsForDeployment(ctx, d)

    // Line 617: being deleted
    if d.DeletionTimestamp != nil {
        return dc.syncStatusOnly(ctx, d, rsList)
    }

    // Line 628: paused state
    if d.Spec.Paused {
        return dc.sync(ctx, d, rsList)
    }

    // Line 635: rollback requested
    if getRollbackTo(d) != nil {
        return dc.rollback(ctx, d, rsList)
    }

    // Line 639: simple scaling event
    scalingEvent, err := dc.isScalingEvent(ctx, d, rsList)
    if scalingEvent {
        return dc.sync(ctx, d, rsList)
    }

    // Lines 647-658: strategy branch
    switch d.Spec.Strategy.Type {
    case apps.RecreateDeploymentStrategyType:
        return dc.rolloutRecreate(ctx, d, rsList, podMap)
    case apps.RollingUpdateDeploymentStrategyType:
        return dc.rolloutRolling(ctx, d, rsList)  // ← rolling update
    }
}
```

---

### Concept Primer: `maxSurge` and `maxUnavailable`

Before reading the rolling-update code, you need the two knobs that drive every
calculation in it. Both live under `spec.strategy.rollingUpdate` and can be an
absolute number or a percentage of `spec.replicas`:

- **`maxSurge`** — how many **extra** Pods, *above* the desired count, may exist
  during a rollout. It controls how aggressively the **new** ReplicaSet is
  allowed to scale **up**. With `replicas: 10, maxSurge: 2`, the controller may
  run up to 12 Pods at once.
- **`maxUnavailable`** — how many Pods, *below* the desired count, are allowed to
  be **unavailable** during a rollout. It controls how aggressively the **old**
  ReplicaSet is allowed to scale **down**. With `replicas: 10, maxUnavailable: 2`,
  at least 8 Pods must stay Ready at all times.

Together they define a window the total Pod count must stay inside for the whole
rollout:

```
desired - maxUnavailable  ≤  Ready Pods  ≤  desired + maxSurge
        (8)                                       (12)
```

The entire rolling update is just the controller nudging the new RS up and the
old RS down, one step at a time, **without ever leaving that window** — which is
why no single sync finishes the rollout (see "Most common confusion" above). The
formulas in [4b]/[4c] (`NewRSNewReplicas`, `maxScaledDown`) are simply this
window expressed in code.

> ⚠️ `maxSurge` and `maxUnavailable` cannot **both** be 0 — that would leave no
> room to either add a new Pod or remove an old one, so the rollout could never
> make progress. The API validation rejects that combination.

---

### [4] rolloutRolling() — Rolling Update Core

**File:** [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go#L31)

```go
// Lines 31-66: rolloutRolling()
func (dc *DeploymentController) rolloutRolling(ctx, d, rsList) error {

    // Line 32: look up or create the new RS
    newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, true)
    allRSs := append(oldRSs, newRS)

    // Line 39: scale up new RS
    scaledUp, err := dc.reconcileNewReplicaSet(ctx, allRSs, newRS, d)

    // Line 49: scale down old RSs
    scaledDown, err := dc.reconcileOldReplicaSets(ctx, allRSs,
        controller.FilterActiveReplicaSets(oldRSs), newRS, d)

    // Line 58: cleanup on completion (applies RevisionHistoryLimit)
    if deploymentutil.DeploymentComplete(d, &d.Status) {
        dc.cleanupDeployment(ctx, oldRSs, d)
    }

    // Line 65: update status
    return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
}
```

---

### [4a] Creating the New ReplicaSet

**File:** [pkg/controller/deployment/sync.go](../pkg/controller/deployment/sync.go#L146)

```go
// Lines 146-300: getNewReplicaSet()
func (dc *DeploymentController) getNewReplicaSet(ctx, d, rsList, ...) (*apps.ReplicaSet, error) {

    // Line 148: find existing RS based on PodTemplateHash
    existingNewRS := deploymentutil.FindNewReplicaSet(d, rsList)
    if existingNewRS != nil {
        // Line 160: update annotations/revision of the existing RS
        return updatedRS, nil
    }

    // Lines 196-232: create new RS
    newRS := apps.ReplicaSet{
        ObjectMeta: metav1.ObjectMeta{
            // Line 207: name = deployment name + pod template hash
            Name:            generateReplicaSetName(d.Name, podTemplateSpecHash),
            OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, controllerKind)},
        },
        Spec: apps.ReplicaSetSpec{
            Replicas: new(int32),  // initial value computed below
            Template: newRSTemplate,
        },
    }

    // Line 220: compute initial replica count (accounting for maxSurge)
    newReplicasCount, err := deploymentutil.NewRSNewReplicas(d, allRSs, &newRS)

    // Line 232: create the RS in the API server
    createdRS, err := dc.client.AppsV1().ReplicaSets(d.Namespace).Create(ctx, &newRS, ...)
}
```

**Hash collision handling (lines 233-268):**
```go
case errors.IsAlreadyExists(err):
    // Same name but owned by a different Deployment, or template differs
    // d.Status.CollisionCount++ then retry
```

**Initial replicas calculation (NewRSNewReplicas):**

**File:** [pkg/controller/deployment/util/deployment_util.go](../pkg/controller/deployment/util/deployment_util.go#L817)

```go
// Lines 817-842: NewRSNewReplicas()
// Formula:
maxTotalPods := desired + maxSurge
scaleUpCount := maxTotalPods - currentTotal
// new RS replicas = current value + min(scaleUpCount, desired - newRS current value)
```

---

### [4b] Scaling Up the New RS

**File:** [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go#L68)

```go
// Lines 68-84: reconcileNewReplicaSet()
func (dc *DeploymentController) reconcileNewReplicaSet(ctx, allRSs, newRS, d) (bool, error) {

    // already at desired count
    if *(newRS.Spec.Replicas) == *(d.Spec.Replicas) {
        return false, nil
    }

    // over desired (due to surge)
    if *(newRS.Spec.Replicas) > *(d.Spec.Replicas) {
        scaled, _, err := dc.scaleReplicaSet(ctx, newRS, *(d.Spec.Replicas), d, false)
        return scaled, err
    }

    // compute scale-up count and apply
    newReplicasCount, err := deploymentutil.NewRSNewReplicas(d, allRSs, newRS)
    scaled, _, err := dc.scaleReplicaSet(ctx, newRS, newReplicasCount, d, false)
    return scaled, err
}
```

---

### [4c] Scaling Down the Old RSs

**File:** [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go#L86)

```go
// Lines 86-152: reconcileOldReplicaSets()
func (dc *DeploymentController) reconcileOldReplicaSets(ctx, allRSs, oldRSs, newRS, d) (bool, error) {

    oldPodsCount := deploymentutil.GetReplicaCountForReplicaSets(oldRSs)
    if oldPodsCount == 0 { return false, nil }

    // Line 95: compute maxUnavailable
    maxUnavailable := deploymentutil.MaxUnavailable(*d)

    // compute how many Pods can be safely scaled down
    minAvailable := *(d.Spec.Replicas) - maxUnavailable
    newRSUnavailablePodCount := *(newRS.Spec.Replicas) - newRS.Status.AvailableReplicas
    maxScaledDown := allPodsCount - minAvailable - newRSUnavailablePodCount

    if maxScaledDown <= 0 { return false, nil }

    // Line 136: remove unhealthy Pods first
    oldRSs, cleanupCount, _ := dc.cleanupUnhealthyReplicas(ctx, oldRSs, d, maxScaledDown)

    // Line 144: safely scale down the rest
    scaledDownCount, _ := dc.scaleDownOldReplicaSetsForRollingUpdate(ctx, allRSs, oldRSs, d)
}
```

**Scale down safety formula:**
```
minAvailable  = desiredReplicas - maxUnavailable
maxScaledDown = totalPods - minAvailable - newRS_unavailable

At least minAvailable Pods always remain Ready
```

**Scale down example (replicas=10, maxSurge=2, maxUnavailable=2):**
```
t=0s: oldRS=10, newRS=0   → newRS scale up: 2 (maxSurge)
t=1s: oldRS=10, newRS=2   → oldRS scale down: 2 (minAvail=8, total=12)
t=2s: oldRS=8,  newRS=2   → newRS scale up: 2 more
t=3s: oldRS=8,  newRS=4   → oldRS scale down: 2 more
...
t=Ns: oldRS=0,  newRS=10  → complete
```

**Unhealthy-first removal (lines 155-189):**
```go
// Line 157: sort by creation time (oldest first)
sort.Sort(controller.ReplicaSetsByCreationTimestamp(oldRSs))

for _, targetRS := range oldRSs {
    // unhealthy Pod count = Spec.Replicas - Status.AvailableReplicas
    scaledDownCount := min(maxCleanupCount-totalScaledDown,
        *(targetRS.Spec.Replicas) - targetRS.Status.AvailableReplicas)
    dc.scaleReplicaSet(ctx, targetRS, *(targetRS.Spec.Replicas)-scaledDownCount, ...)
}
```

---

### [5] ReplicaSetController — Pod Creation/Deletion

**File:** [pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go#L649)

```go
// Lines 649-750: manageReplicas()
func (rsc *ReplicaSetController) manageReplicas(ctx, activePods, rs) error {

    diff := len(activePods) - int(*(rs.Spec.Replicas))

    if diff < 0 {  // not enough Pods → create
        diff *= -1
        if diff > rsc.burstReplicas { diff = rsc.burstReplicas }  // max 500

        rsc.expectations.ExpectCreations(logger, rsKey, diff)  // register expectation

        // Line 677: slow-start batch creation
        successfulCreations, err := slowStartBatch(diff, controller.SlowStartInitialBatchSize,
            func() error {
                return rsc.podControl.CreatePods(ctx, rs.Namespace, &rs.Spec.Template, rs, ...)
            })

    } else if diff > 0 {  // too many Pods → delete
        if diff > rsc.burstReplicas { diff = rsc.burstReplicas }

        // Line 710: determine deletion order (not-ready < ready, unscheduled < scheduled)
        podsToDelete := getPodsToDelete(activePods, relatedPods, diff)

        rsc.expectations.ExpectDeletions(logger, rsKey, getPodKeys(podsToDelete))

        // Line 723: parallel deletion
        var wg sync.WaitGroup
        wg.Add(diff)
        for _, pod := range podsToDelete {
            go func(targetPod *v1.Pod) {
                defer wg.Done()
                rsc.podControl.DeletePod(ctx, rs.Namespace, targetPod.Name, rs)
            }(pod)
        }
        wg.Wait()
    }
}
```

**Slow-start batch pattern (lines 887-911):**
```
Batch sizes: 1 → 2 → 4 → 8 → 16 ...
Why: if a batch errors, only that batch size fails
      → caps the total number of failed Pods at O(failure size)
```

**Pod deletion priority (getPodsToDelete):**
```
1. Not-ready first (ready=false)
2. Unscheduled first (nodeName="")
3. Pending first (phase=Pending)
4. Pods on nodes with more related Pods first (ensures diversity)
```

---

### [4d] Rollout Status Update

**File:** [pkg/controller/deployment/progress.go](../pkg/controller/deployment/progress.go#L36)

```go
// Lines 36-118: syncRolloutStatus()
func (dc *DeploymentController) syncRolloutStatus(ctx, allRSs, newRS, d) error {
    newStatus := calculateStatus(allRSs, newRS, d)

    switch {
    // Line 53: complete
    case DeploymentComplete(d, &newStatus):
        condition := NewDeploymentCondition(DeploymentProgressing,
            True, NewRSAvailableReason,
            "ReplicaSet has successfully progressed.")

    // Line 63: in progress
    case DeploymentProgressing(d, &newStatus):
        condition := NewDeploymentCondition(DeploymentProgressing,
            True, ReplicaSetUpdatedReason, "...")

    // Line 86: timed out
    case DeploymentTimedOut(ctx, d, &newStatus):
        condition := NewDeploymentCondition(DeploymentProgressing,
            False, TimedOutReason, "ReplicaSet has timed out.")
    }

    // update status in the API server
    dc.client.AppsV1().Deployments(ns).UpdateStatus(ctx, newDeployment, ...)
}
```

**Completion condition (lines 741-746):**
```go
func DeploymentComplete(deployment, newStatus) bool {
    return newStatus.UpdatedReplicas == *(deployment.Spec.Replicas) &&  // all updated
           newStatus.Replicas == *(deployment.Spec.Replicas) &&          // total count matches
           newStatus.AvailableReplicas == *(deployment.Spec.Replicas) && // all available
           newStatus.ObservedGeneration >= deployment.Generation          // generation in sync
}
```

**Progressing condition (lines 752-763):**
```go
func DeploymentProgressing(deployment, newStatus) bool {
    return newStatus.UpdatedReplicas > oldStatus.UpdatedReplicas ||  // new Pods increasing
           oldReplicas > newReplicas ||                               // old Pods decreasing
           newStatus.ReadyReplicas > deployment.Status.ReadyReplicas || // ready increasing
           newStatus.AvailableReplicas > deployment.Status.AvailableReplicas
}
```

---

### Cleanup — RevisionHistoryLimit

**File:** [pkg/controller/deployment/sync.go](../pkg/controller/deployment/sync.go#L441)

```go
// Lines 441-476: cleanupDeployment()
// Delete old RSs exceeding RevisionHistoryLimit (default 10)
// Condition: only RSs with Pod count = 0 can be deleted
sort.Sort(deploymentutil.ReplicaSetsByRevision(cleanableRSes))
for i := int32(0); i < diff; i++ {
    rs := cleanableRSes[i]
    if rs.Status.Replicas != 0 { continue }  // skip if it has Pods
    dc.client.AppsV1().ReplicaSets(rs.Namespace).Delete(ctx, rs.Name, ...)
}
```

---

## The Expectations Pattern

Prevents duplicate processing after the controller issues an API request, until confirmed by actual Watch events:

```
1. ExpectCreations(key, 5) ← registered after requesting creation of 5 Pods
2. Pod Watch event received → CreationObserved(key) called (count--)
3. No re-sync until SatisfiedExpectations(key) == true
```

---

## Deployment Status Conditions

| Type | Reason | Status | Description |
|------|--------|--------|------|
| `Progressing` | `NewReplicaSetReason` | True | New RS created |
| `Progressing` | `ReplicaSetUpdatedReason` | True | RS being updated |
| `Progressing` | `NewRSAvailableReason` | True | Rollout complete |
| `Progressing` | `TimedOutReason` | False | progressDeadlineSeconds exceeded |
| `Available` | `MinimumReplicasAvailable` | True | Minimum replicas running |
| `Available` | `MinimumReplicasUnavailable` | False | Below minimum replicas |

---

## Key File Path Summary

| Step | File | Key Function | Line |
|------|------|----------|------|
| Controller init | [pkg/controller/deployment/deployment_controller.go](../pkg/controller/deployment/deployment_controller.go) | `NewDeploymentController` | 104 |
| Sync entry | [pkg/controller/deployment/deployment_controller.go](../pkg/controller/deployment/deployment_controller.go) | `syncDeployment` | 574 |
| Rolling update | [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go) | `rolloutRolling` | 31 |
| RS creation | [pkg/controller/deployment/sync.go](../pkg/controller/deployment/sync.go) | `getNewReplicaSet` | 146 |
| Scale up | [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go) | `reconcileNewReplicaSet` | 68 |
| Scale down | [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go) | `reconcileOldReplicaSets` | 86 |
| RS sync | [pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go) | `syncReplicaSet` | 755 |
| Pod reconciliation | [pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go) | `manageReplicas` | 649 |
| Status update | [pkg/controller/deployment/progress.go](../pkg/controller/deployment/progress.go) | `syncRolloutStatus` | 36 |
| Utilities | [pkg/controller/deployment/util/deployment_util.go](../pkg/controller/deployment/util/deployment_util.go) | `NewRSNewReplicas`, `DeploymentComplete` | 817, 741 |

---

## Related Concepts

- **Controller pattern & level-triggered reconciliation.** A controller ignores the event payload and instead re-reads current state, then drives it toward `spec`. Missing an event is harmless because the next sync recomputes everything from scratch — "level-triggered, not edge-triggered."
- **Owner references & cascading deletion.** A Deployment owns its ReplicaSets, which own their Pods, via `metadata.ownerReferences`. Deleting the Deployment lets the garbage collector cascade down; *adoption* lets a controller claim matching orphans that lack an owner.
- **`pod-template-hash`.** Each ReplicaSet is named and labeled with a hash of its Pod template, so a changed template deterministically maps to a *new* ReplicaSet and old/new Pods never get mixed up.
- **`maxSurge` / `maxUnavailable`.** The two rollout knobs: how many *extra* Pods may exist above the desired count, and how many may be *missing* below it. Together they bound the safety envelope the reconcile arithmetic enforces each sync.
- **`generation` vs. `observedGeneration`.** `metadata.generation` increments on every spec change; the controller copies it into `status.observedGeneration` once it has processed that change — how tooling knows the controller has "seen" your latest edit.
- **The Expectations pattern.** A short-lived in-memory cache of "I expect N creates / M deletes." It stops the controller from re-issuing the same create/delete while its earlier writes are still in flight through the informer (which would otherwise double-create).
- **Revision history & rollback.** Each ReplicaSet is a frozen template revision; `kubectl rollout undo` simply scales an old ReplicaSet back up. `revisionHistoryLimit` caps how many are retained.

> ⚠️ **A Deployment never touches Pods directly.** It only manages ReplicaSets; ReplicaSets manage Pods. Debugging a stuck rollout almost always means inspecting the ReplicaSets in between.

---

## Related Scenarios

- [Scenario 1: API Request Flow](01-api-request-flow.md) — the flow in which the Deployment object is stored
- [Scenario 4: kubelet Pod Lifecycle](04-kubelet-pod-lifecycle.md) — the flow in which Pods actually run
