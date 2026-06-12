# 시나리오 3: Deployment 롤링 업데이트

`kubectl apply` 후 Deployment가 변경될 때 컨트롤러가 ReplicaSet을 생성/수정하고 Pod를 교체하는 흐름을 추적합니다.

## 전체 흐름도

```
kubectl apply -f deployment-v2.yaml
        │
[1] API 서버에 Deployment 업데이트 저장
        │ (Watch 이벤트)
        ▼
[2] pkg/controller/deployment/deployment_controller.go
    updateDeployment() → WorkQueue에 key 추가
        │
        ▼
[3] syncDeployment() — 전략 분기
        │
        ├─ d.Spec.Strategy.Type == RollingUpdate
        │          ↓
[4] rolling.go:rolloutRolling()
        │
        ├─ [4a] getAllReplicaSetsAndSyncRevision()
        │        새 RS 없으면 생성 (sync.go:getNewReplicaSet)
        │
        ├─ [4b] reconcileNewReplicaSet()
        │        새 RS scale up
        │
        ├─ [4c] reconcileOldReplicaSets()
        │        기존 RS scale down
        │
        └─ [4d] syncRolloutStatus()
                 Deployment 상태 업데이트
        │
        ▼ (ReplicaSet.Spec.Replicas 변경)
[5] pkg/controller/replicaset/replica_set.go
    syncReplicaSet() → manageReplicas()
        │
        ├─ diff < 0: slowStartBatch()로 Pod 생성
        └─ diff > 0: 병렬 Pod 삭제
```

---

## 단계별 상세 분석

### [2] DeploymentController 초기화 및 이벤트 처리

**파일:** [pkg/controller/deployment/deployment_controller.go](../pkg/controller/deployment/deployment_controller.go#L104)

```go
// 라인 104-168: NewDeploymentController()
func NewDeploymentController(ctx, dInformer, rsInformer, podInformer, client) (*DeploymentController, error) {
    dc := &DeploymentController{
        client:        client,
        dLister:       dInformer.Lister(),
        rsLister:      rsInformer.Lister(),
        podIndexer:    podInformer.Informer().GetIndexer(),
        queue:         workqueue.NewTypedRateLimitingQueue(...),
    }

    // Informer 이벤트 핸들러 등록 (라인 131-167)
    dInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc:    dc.addDeployment,    // 라인 201: 큐에 추가
        UpdateFunc: dc.updateDeployment, // 라인 207: 큐에 추가
        DeleteFunc: dc.deleteDeployment,
    })
}
```

**처리 루프:**
```go
// 라인 481-484: worker()
func (dc *DeploymentController) worker(ctx context.Context) {
    for dc.processNextWorkItem(ctx) {}
}

// 라인 486-497: processNextWorkItem()
key, _ := dc.queue.Get()
dc.syncHandler(ctx, key.(string))  // syncDeployment() 호출
```

---

### [3] syncDeployment() — 전략 분기

**파일:** [pkg/controller/deployment/deployment_controller.go](../pkg/controller/deployment/deployment_controller.go#L574)

```go
// 라인 574-660: syncDeployment()
func (dc *DeploymentController) syncDeployment(ctx context.Context, key string) error {

    // 라인 588: Deployment 조회
    deployment, err := dc.dLister.Deployments(namespace).Get(name)

    // 라인 612: RS 목록 조회 및 orphan RS 주장
    rsList, err := dc.getReplicaSetsForDeployment(ctx, d)

    // 라인 617: 삭제 중인 경우
    if d.DeletionTimestamp != nil {
        return dc.syncStatusOnly(ctx, d, rsList)
    }

    // 라인 628: 일시정지 상태
    if d.Spec.Paused {
        return dc.sync(ctx, d, rsList)
    }

    // 라인 635: 롤백 요청
    if getRollbackTo(d) != nil {
        return dc.rollback(ctx, d, rsList)
    }

    // 라인 639: 단순 스케일링 이벤트
    scalingEvent, err := dc.isScalingEvent(ctx, d, rsList)
    if scalingEvent {
        return dc.sync(ctx, d, rsList)
    }

    // 라인 647-658: 전략 분기
    switch d.Spec.Strategy.Type {
    case apps.RecreateDeploymentStrategyType:
        return dc.rolloutRecreate(ctx, d, rsList, podMap)
    case apps.RollingUpdateDeploymentStrategyType:
        return dc.rolloutRolling(ctx, d, rsList)  // ← 롤링 업데이트
    }
}
```

---

### [4] rolloutRolling() — 롤링 업데이트 핵심

**파일:** [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go#L31)

```go
// 라인 31-66: rolloutRolling()
func (dc *DeploymentController) rolloutRolling(ctx, d, rsList) error {

    // 라인 32: 새 RS 조회 또는 생성
    newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, true)
    allRSs := append(oldRSs, newRS)

    // 라인 39: 새 RS scale up
    scaledUp, err := dc.reconcileNewReplicaSet(ctx, allRSs, newRS, d)

    // 라인 49: 기존 RS scale down
    scaledDown, err := dc.reconcileOldReplicaSets(ctx, allRSs,
        controller.FilterActiveReplicaSets(oldRSs), newRS, d)

    // 라인 58: 완료 시 cleanup (RevisionHistoryLimit 적용)
    if deploymentutil.DeploymentComplete(d, &d.Status) {
        dc.cleanupDeployment(ctx, oldRSs, d)
    }

    // 라인 65: 상태 업데이트
    return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
}
```

---

### [4a] 새 ReplicaSet 생성

**파일:** [pkg/controller/deployment/sync.go](../pkg/controller/deployment/sync.go#L146)

```go
// 라인 146-300: getNewReplicaSet()
func (dc *DeploymentController) getNewReplicaSet(ctx, d, rsList, ...) (*apps.ReplicaSet, error) {

    // 라인 148: PodTemplateHash 기반으로 기존 RS 탐색
    existingNewRS := deploymentutil.FindNewReplicaSet(d, rsList)
    if existingNewRS != nil {
        // 라인 160: 기존 RS 어노테이션/revision 업데이트
        return updatedRS, nil
    }

    // 라인 196-232: 새 RS 생성
    newRS := apps.ReplicaSet{
        ObjectMeta: metav1.ObjectMeta{
            // 라인 207: 이름 = deployment명 + pod template hash
            Name:            generateReplicaSetName(d.Name, podTemplateSpecHash),
            OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, controllerKind)},
        },
        Spec: apps.ReplicaSetSpec{
            Replicas: new(int32),  // 초기값 계산
            Template: newRSTemplate,
        },
    }

    // 라인 220: 초기 replicas 수 계산 (maxSurge 고려)
    newReplicasCount, err := deploymentutil.NewRSNewReplicas(d, allRSs, &newRS)

    // 라인 232: API 서버에 RS 생성
    createdRS, err := dc.client.AppsV1().ReplicaSets(d.Namespace).Create(ctx, &newRS, ...)
}
```

**Hash 충돌 처리 (라인 233-268):**
```go
case errors.IsAlreadyExists(err):
    // 같은 이름이지만 다른 Deployment 소유이거나 template이 다른 경우
    // d.Status.CollisionCount++ 후 재시도
```

**초기 Replicas 계산 (NewRSNewReplicas):**

**파일:** [pkg/controller/deployment/util/deployment_util.go](../pkg/controller/deployment/util/deployment_util.go#L817)

```go
// 라인 817-842: NewRSNewReplicas()
// 공식:
maxTotalPods := desired + maxSurge
scaleUpCount := maxTotalPods - currentTotal
// 새 RS replicas = 기존값 + min(scaleUpCount, desired-newRS현재값)
```

---

### [4b] 새 RS Scale Up

**파일:** [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go#L68)

```go
// 라인 68-84: reconcileNewReplicaSet()
func (dc *DeploymentController) reconcileNewReplicaSet(ctx, allRSs, newRS, d) (bool, error) {

    // 이미 desired 수 달성
    if *(newRS.Spec.Replicas) == *(d.Spec.Replicas) {
        return false, nil
    }

    // 초과 (surge 때문에)
    if *(newRS.Spec.Replicas) > *(d.Spec.Replicas) {
        scaled, _, err := dc.scaleReplicaSet(ctx, newRS, *(d.Spec.Replicas), d, false)
        return scaled, err
    }

    // scale up 수 계산 후 적용
    newReplicasCount, err := deploymentutil.NewRSNewReplicas(d, allRSs, newRS)
    scaled, _, err := dc.scaleReplicaSet(ctx, newRS, newReplicasCount, d, false)
    return scaled, err
}
```

---

### [4c] 기존 RS Scale Down

**파일:** [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go#L86)

```go
// 라인 86-152: reconcileOldReplicaSets()
func (dc *DeploymentController) reconcileOldReplicaSets(ctx, allRSs, oldRSs, newRS, d) (bool, error) {

    oldPodsCount := deploymentutil.GetReplicaCountForReplicaSets(oldRSs)
    if oldPodsCount == 0 { return false, nil }

    // 라인 95: maxUnavailable 계산
    maxUnavailable := deploymentutil.MaxUnavailable(*d)

    // 안전하게 내릴 수 있는 Pod 수 계산
    minAvailable := *(d.Spec.Replicas) - maxUnavailable
    newRSUnavailablePodCount := *(newRS.Spec.Replicas) - newRS.Status.AvailableReplicas
    maxScaledDown := allPodsCount - minAvailable - newRSUnavailablePodCount

    if maxScaledDown <= 0 { return false, nil }

    // 라인 136: unhealthy Pod 먼저 제거
    oldRSs, cleanupCount, _ := dc.cleanupUnhealthyReplicas(ctx, oldRSs, d, maxScaledDown)

    // 라인 144: 나머지 안전하게 scale down
    scaledDownCount, _ := dc.scaleDownOldReplicaSetsForRollingUpdate(ctx, allRSs, oldRSs, d)
}
```

**Scale Down 안전성 공식:**
```
minAvailable  = desiredReplicas - maxUnavailable
maxScaledDown = totalPods - minAvailable - newRS_unavailable

항상 minAvailable 이상의 Pod가 Ready 상태를 유지
```

**Scale Down 예시 (replicas=10, maxSurge=2, maxUnavailable=2):**
```
t=0s: oldRS=10, newRS=0   → newRS scale up: 2 (maxSurge)
t=1s: oldRS=10, newRS=2   → oldRS scale down: 2 (minAvail=8, total=12)
t=2s: oldRS=8,  newRS=2   → newRS scale up: 2 더
t=3s: oldRS=8,  newRS=4   → oldRS scale down: 2 더
...
t=Ns: oldRS=0,  newRS=10  → 완료
```

**unhealthy 우선 제거 (라인 155-189):**
```go
// 라인 157: 생성 시간순 정렬 (오래된 것부터)
sort.Sort(controller.ReplicaSetsByCreationTimestamp(oldRSs))

for _, targetRS := range oldRSs {
    // unhealthy Pod 수 = Spec.Replicas - Status.AvailableReplicas
    scaledDownCount := min(maxCleanupCount-totalScaledDown,
        *(targetRS.Spec.Replicas) - targetRS.Status.AvailableReplicas)
    dc.scaleReplicaSet(ctx, targetRS, *(targetRS.Spec.Replicas)-scaledDownCount, ...)
}
```

---

### [5] ReplicaSetController — Pod 생성/삭제

**파일:** [pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go#L649)

```go
// 라인 649-750: manageReplicas()
func (rsc *ReplicaSetController) manageReplicas(ctx, activePods, rs) error {

    diff := len(activePods) - int(*(rs.Spec.Replicas))

    if diff < 0 {  // Pod 부족 → 생성
        diff *= -1
        if diff > rsc.burstReplicas { diff = rsc.burstReplicas }  // 최대 500개

        rsc.expectations.ExpectCreations(logger, rsKey, diff)  // expectation 등록

        // 라인 677: Slow-start 배치 생성
        successfulCreations, err := slowStartBatch(diff, controller.SlowStartInitialBatchSize,
            func() error {
                return rsc.podControl.CreatePods(ctx, rs.Namespace, &rs.Spec.Template, rs, ...)
            })

    } else if diff > 0 {  // Pod 초과 → 삭제
        if diff > rsc.burstReplicas { diff = rsc.burstReplicas }

        // 라인 710: 삭제 순서 결정 (not-ready < ready, unscheduled < scheduled)
        podsToDelete := getPodsToDelete(activePods, relatedPods, diff)

        rsc.expectations.ExpectDeletions(logger, rsKey, getPodKeys(podsToDelete))

        // 라인 723: 병렬 삭제
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

**Slow-Start Batch 패턴 (라인 887-911):**
```
배치 크기: 1 → 2 → 4 → 8 → 16 ...
이유: 한 배치에서 에러 발생 시 그 크기만큼만 실패
      → 전체 실패 Pod 수를 O(실패크기)로 제한
```

**Pod 삭제 우선순위 (getPodsToDelete):**
```
1. Not-ready 먼저 (ready=false)
2. Unscheduled 먼저 (nodeName="")
3. Pending 먼저 (phase=Pending)
4. 같은 노드에 관련 Pod 많은 것 먼저 (다양성 보장)
```

---

### [4d] 롤아웃 상태 업데이트

**파일:** [pkg/controller/deployment/progress.go](../pkg/controller/deployment/progress.go#L36)

```go
// 라인 36-118: syncRolloutStatus()
func (dc *DeploymentController) syncRolloutStatus(ctx, allRSs, newRS, d) error {
    newStatus := calculateStatus(allRSs, newRS, d)

    switch {
    // 라인 53: 완료
    case DeploymentComplete(d, &newStatus):
        condition := NewDeploymentCondition(DeploymentProgressing,
            True, NewRSAvailableReason,
            "ReplicaSet has successfully progressed.")

    // 라인 63: 진행 중
    case DeploymentProgressing(d, &newStatus):
        condition := NewDeploymentCondition(DeploymentProgressing,
            True, ReplicaSetUpdatedReason, "...")

    // 라인 86: 타임아웃
    case DeploymentTimedOut(ctx, d, &newStatus):
        condition := NewDeploymentCondition(DeploymentProgressing,
            False, TimedOutReason, "ReplicaSet has timed out.")
    }

    // API 서버에 status 업데이트
    dc.client.AppsV1().Deployments(ns).UpdateStatus(ctx, newDeployment, ...)
}
```

**완료 조건 (라인 741-746):**
```go
func DeploymentComplete(deployment, newStatus) bool {
    return newStatus.UpdatedReplicas == *(deployment.Spec.Replicas) &&  // 모두 업데이트됨
           newStatus.Replicas == *(deployment.Spec.Replicas) &&          // 총 수 일치
           newStatus.AvailableReplicas == *(deployment.Spec.Replicas) && // 모두 available
           newStatus.ObservedGeneration >= deployment.Generation          // generation 동기화됨
}
```

**진행 조건 (라인 752-763):**
```go
func DeploymentProgressing(deployment, newStatus) bool {
    return newStatus.UpdatedReplicas > oldStatus.UpdatedReplicas ||  // 새 Pod 증가
           oldReplicas > newReplicas ||                               // 기존 Pod 감소
           newStatus.ReadyReplicas > deployment.Status.ReadyReplicas || // ready 증가
           newStatus.AvailableReplicas > deployment.Status.AvailableReplicas
}
```

---

### Cleanup — RevisionHistoryLimit

**파일:** [pkg/controller/deployment/sync.go](../pkg/controller/deployment/sync.go#L441)

```go
// 라인 441-476: cleanupDeployment()
// RevisionHistoryLimit(기본 10) 초과한 오래된 RS 삭제
// 조건: Pod 수 = 0인 RS만 삭제 가능
sort.Sort(deploymentutil.ReplicaSetsByRevision(cleanableRSes))
for i := int32(0); i < diff; i++ {
    rs := cleanableRSes[i]
    if rs.Status.Replicas != 0 { continue }  // Pod 있으면 건너뜀
    dc.client.AppsV1().ReplicaSets(rs.Namespace).Delete(ctx, rs.Name, ...)
}
```

---

## Expectations 패턴

컨트롤러가 API 요청 후 실제 Watch 이벤트로 확인받을 때까지 중복 처리 방지:

```
1. ExpectCreations(key, 5) ← 5개 Pod 생성 요청 후 등록
2. Pod Watch 이벤트 수신 → CreationObserved(key) 호출 (count--)
3. SatisfiedExpectations(key) == true 가 될 때까지 재sync 하지 않음
```

---

## Deployment Status Conditions

| Type | Reason | Status | 설명 |
|------|--------|--------|------|
| `Progressing` | `NewReplicaSetReason` | True | 새 RS 생성됨 |
| `Progressing` | `ReplicaSetUpdatedReason` | True | RS 업데이트 중 |
| `Progressing` | `NewRSAvailableReason` | True | 롤아웃 완료 |
| `Progressing` | `TimedOutReason` | False | progressDeadlineSeconds 초과 |
| `Available` | `MinimumReplicasAvailable` | True | 최소 replica 운영 중 |
| `Available` | `MinimumReplicasUnavailable` | False | 최소 replica 부족 |

---

## 핵심 파일 경로 요약

| 단계 | 파일 | 핵심 함수 | 라인 |
|------|------|----------|------|
| 컨트롤러 초기화 | [pkg/controller/deployment/deployment_controller.go](../pkg/controller/deployment/deployment_controller.go) | `NewDeploymentController` | 104 |
| Sync 진입 | [pkg/controller/deployment/deployment_controller.go](../pkg/controller/deployment/deployment_controller.go) | `syncDeployment` | 574 |
| 롤링 업데이트 | [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go) | `rolloutRolling` | 31 |
| RS 생성 | [pkg/controller/deployment/sync.go](../pkg/controller/deployment/sync.go) | `getNewReplicaSet` | 146 |
| Scale Up | [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go) | `reconcileNewReplicaSet` | 68 |
| Scale Down | [pkg/controller/deployment/rolling.go](../pkg/controller/deployment/rolling.go) | `reconcileOldReplicaSets` | 86 |
| RS Sync | [pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go) | `syncReplicaSet` | 755 |
| Pod 조율 | [pkg/controller/replicaset/replica_set.go](../pkg/controller/replicaset/replica_set.go) | `manageReplicas` | 649 |
| 상태 업데이트 | [pkg/controller/deployment/progress.go](../pkg/controller/deployment/progress.go) | `syncRolloutStatus` | 36 |
| 유틸 | [pkg/controller/deployment/util/deployment_util.go](../pkg/controller/deployment/util/deployment_util.go) | `NewRSNewReplicas`, `DeploymentComplete` | 817, 741 |

---

## 관련 시나리오

- [시나리오 1: API 요청 흐름](01-api-request-flow.md) — Deployment 객체가 저장되는 흐름
- [시나리오 4: kubelet Pod 라이프사이클](04-kubelet-pod-lifecycle.md) — Pod가 실제 실행되는 흐름
