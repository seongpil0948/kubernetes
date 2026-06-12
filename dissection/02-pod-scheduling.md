# 시나리오 2: Pod 스케줄링 흐름

Pending Pod가 생성된 후 스케줄러가 감지하고 노드를 선택하여 바인딩하기까지의 전체 흐름을 추적합니다.

## 전체 흐름도

```
etcd에 Pending Pod 저장
        │ (Watch 이벤트)
        ▼
[1] pkg/scheduler/backend/queue/scheduling_queue.go
    PriorityQueue.Add() — activeQ에 추가
        │
        ▼
[2] pkg/scheduler/schedule_one.go:ScheduleOne()
    activeQ에서 Pod 추출 → scheduleOnePod()
        │
        ├─ [3] schedulingCycle()
        │       │
        │       ├─ [3a] RunPreFilterPlugins()
        │       │         Pod 레벨 사전 검증
        │       │
        │       ├─ [3b] findNodesThatPassFilters()
        │       │         병렬 Filter 플러그인 실행
        │       │         → 가능한 노드 목록
        │       │
        │       ├─ [3c] prioritizeNodes()
        │       │         PreScore → Score → NormalizeScore
        │       │         → 노드별 점수
        │       │
        │       ├─ [3d] selectHost()
        │       │         최고점 노드 선택
        │       │
        │       └─ [3e] assumeAndReserve()
        │                 Cache에 예약 + Reserve 플러그인
        │
        └─ [4] runBindingCycle() (비동기 goroutine)
                │
                ├─ RunPermitPlugins() — 허가 확인
                ├─ RunPreBindPlugins() — 바인딩 전 처리
                ├─ bind() — API 서버에 nodeName 설정
                └─ RunPostBindPlugins()
```

---

## 단계별 상세 분석

### 스케줄러 구조체

**파일:** [pkg/scheduler/scheduler.go](../pkg/scheduler/scheduler.go#L68)

```go
// 라인 68-125: Scheduler struct
type Scheduler struct {
    Cache           internalcache.Cache              // 노드/Pod 인메모리 캐시
    NextPod         func(...) (*framework.QueuedPodInfo, error)  // activeQ에서 Pop
    SchedulingQueue internalqueue.SchedulingQueue    // 3-tier 우선순위 큐
    SchedulePod     func(...)                        // Filter + Score 로직
    Profiles        profile.Map                      // 스케줄러 프로필 (플러그인 설정)
    APIDispatcher   *apidispatcher.APIDispatcher     // 비동기 API 호출
}
```

---

### [1] 스케줄링 큐

**파일:** [pkg/scheduler/backend/queue/scheduling_queue.go](../pkg/scheduler/backend/queue/scheduling_queue.go#L170)

```go
// 라인 170-215: PriorityQueue struct
type PriorityQueue struct {
    activeQ             activeQueuer        // 스케줄링 대상 (힙)
    backoffQ            backoffQueuer       // 백오프 중인 Pod (지수 백오프)
    unschedulablePods   *unschedulablePods  // 스케줄 불가 Pod (최대 5분)
    podMaxInUnschedulablePodsDuration time.Duration
    clock               clock.WithTicker
    lock                sync.RWMutex
}
```

**큐 상태 전이:**
```
신규 Pod → activeQ
                │ 스케줄 실패
                ├─ (unschedulable plugin) → unschedulablePods
                │                              │ 5분 경과 or 클러스터 이벤트
                └─ (기타)        → backoffQ   │
                                    │ 백오프 만료└──→ activeQ
                                    └──────────────→ activeQ
```

**QueuedPodInfo 구조:**
```go
type QueuedPodInfo struct {
    PodInfo                *PodInfo
    Pod                    *v1.Pod
    Timestamp              time.Time
    Attempts               int                  // 시도 횟수
    UnschedulablePlugins   sets.Set[string]     // 실패한 플러그인
    BackoffExpiration      time.Time
    InitialAttemptTimestamp *time.Time
}
```

**주요 함수:**

| 함수 | 라인 | 역할 |
|------|------|------|
| `Add(ctx, pod)` | 728 | activeQ에 Pod 추가 (PreEnqueue 플러그인 실행) |
| `Pop(logger)` | 945 | activeQ에서 다음 Pod 추출 |
| `Activate()` | 744 | unschedulable/backoff → activeQ 이동 |
| `MoveAllToActiveOrBackoffQueue()` | 129 | 클러스터 이벤트 시 재큐잉 |
| `AddUnschedulableIfNotPresent()` | - | 실패한 Pod 재큐잉 |

---

### [2] 스케줄링 메인 루프

**파일:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L67)

```go
// 라인 67-96: ScheduleOne()
func (sched *Scheduler) ScheduleOne(ctx context.Context) {
    podInfo := sched.NextPod(logger)  // activeQ.Pop()
    sched.scheduleOnePod(ctx, podInfo)
}

// 라인 99-148: scheduleOnePod()
func (sched *Scheduler) scheduleOnePod(ctx context.Context, podInfo *framework.QueuedPodInfo) {

    // 라인 127: 사이클 상태 초기화 (플러그인 간 데이터 공유)
    state := framework.NewCycleState()

    // 라인 140: 스케줄링 사이클 (동기)
    scheduleResult, assumedPodInfo, status := sched.schedulingCycle(ctx, state, ...)

    // 라인 147: 바인딩 사이클 (비동기 goroutine)
    go sched.runBindingCycle(ctx, state, ...)
}
```

---

### [3] 스케줄링 사이클

**파일:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L175)

```go
// 라인 175-252: schedulingCycle()
func schedulingCycle(ctx, state, fwk, podInfo, ...) {

    // 1. NodeInfo 스냅샷 갱신 (라인 183)
    sched.Cache.UpdateSnapshot(logger, sched.nodeInfoSnapshot)

    // 2. Filter + Score (라인 193)
    scheduleResult, err := sched.SchedulePod(ctx, fwk, state, assumedPodInfo.Pod)

    // 3. 실패 시 PostFilter (프리엠션, 라인 200)
    if err != nil {
        fwk.RunPostFilterPlugins(ctx, state, pod, diagnosis.NodeToStatus)
    }

    // 4. Assume + Reserve (라인 230)
    assumeAndReserve(ctx, state, fwk, podInfo, ...)
}
```

#### [3a] PreFilter 플러그인

**파일:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L628)

```go
// 라인 628-718: findNodesThatFitPod()
preFilterResult, preFilterStatus, preFilterStatusMap :=
    fwk.RunPreFilterPlugins(ctx, state, pod)
```

**PreFilter 플러그인 예시:**

| 플러그인 | 파일 | 역할 |
|---------|------|------|
| NodePorts | [pkg/scheduler/framework/plugins/nodeports/node_ports.go](../pkg/scheduler/framework/plugins/nodeports/node_ports.go#L73) | Pod의 포트 요구사항 캐싱 |
| NodeAffinity | [pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go](../pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go#L148) | 노드 친화성 규칙 파싱 |
| VolumeBinding | pkg/scheduler/framework/plugins/volumebinding/ | PVC 바인딩 가능성 사전 계산 |

#### [3b] Filter 플러그인 (병렬 실행)

**파일:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L777)

```go
// 라인 777-860: findNodesThatPassFilters()
func findNodesThatPassFilters(ctx, fwk, state, pod, diagnosis, nodes) {

    // 병렬 처리 (라인 801-846)
    fwk.Parallelizer().Until(ctx, len(nodes), func(i int) {
        nodeInfo := nodes[i]
        status := fwk.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
        if status.IsSuccess() {
            feasibleNodes = append(feasibleNodes, nodeInfo)
        }
    }, metrics.Filter)

    // 설정된 수 달성 시 조기 종료
}
```

**percentageOfNodesToScore:** 클러스터 크기에 따라 평가할 노드 비율 조정
- 노드 100개 이하: 100%
- 노드 5000개: 약 10%
- 최소: 5%

**주요 Filter 플러그인:**

| 플러그인 | 역할 |
|---------|------|
| NodeUnschedulable | `spec.unschedulable` 확인 |
| NodeName | `spec.nodeName` 지정 확인 |
| NodePorts | 포트 충돌 확인 |
| NodeAffinity | node affinity/anti-affinity 검사 |
| TaintToleration | taint/toleration 매칭 |
| NodeResourcesFit | CPU/Memory/GPU 자원 확인 |
| VolumeBinding | PVC 볼륨 연결 가능성 |
| InterPodAffinity | Pod 간 친화성/반친화성 |
| PodTopologySpread | 토폴로지 분산 제약 |

#### [3c] Score 플러그인

**파일:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L943)

```go
// 라인 943-1000+: prioritizeNodes()
func prioritizeNodes(ctx, extenders, fwk, state, pod, nodes) ([]framework.NodePluginScores, error) {

    // PreScore: 공유 상태 준비 (라인 966)
    fwk.RunPreScorePlugins(ctx, state, pod, nodes)

    // Score: 각 노드 점수 계산 (라인 972)
    nodesScores, err := fwk.RunScorePlugins(ctx, state, pod, nodes)

    // 외부 Extender 점수 추가
    for _, extender := range extenders {
        extender.Prioritize(pod, nodes)
    }
}
```

**점수 계산 과정:**
```
PreScore (공유 캐시 구축)
    ↓
Score (플러그인별 0~100점)
    ↓
NormalizeScore (정규화: 플러그인 내 상대 비교)
    ↓
가중치 적용: 플러그인점수 × weight
    ↓
합산: 노드별 최종 점수
    ↓
최고점 노드 선택 (동점 시 랜덤)
```

**주요 Score 플러그인:**

| 플러그인 | 역할 | 가중치(기본) |
|---------|------|------------|
| NodeAffinity | 선호 노드 친화성 점수 | 2 |
| NodeResourcesFit | 자원 적합도 (LeastAllocated/MostAllocated) | 1 |
| InterPodAffinity | Pod 간 친화성 점수 | 2 |
| PodTopologySpread | 토폴로지 균등 분산 | 2 |
| ImageLocality | 이미지 이미 있는 노드 선호 | 1 |
| TaintToleration | toleration 선호도 | 1 |

#### [3d] PostFilter (프리엠션)

스케줄링 실패(Filter에서 모든 노드 탈락) 시 실행:

```go
// 라인 293-308: schedulingAlgorithm() 내부
status := fwk.RunPostFilterPlugins(ctx, state, pod, diagnosis.NodeToStatus)
```

**DefaultPreemption 플러그인:**
1. 우선순위 낮은 Pod가 있는 노드 탐색
2. 해당 Pod 제거 시 스케줄 가능한지 시뮬레이션
3. 가능하면 `nominatedNodeName` 설정
4. 다음 사이클에서 실제 프리엠션 수행

#### [3e] Assume & Reserve

**파일:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L313)

```go
// 라인 313-359: assumeAndReserve()

// 1. Assume: 캐시에 Pod-노드 할당 예약 (라인 326)
sched.Cache.AssumePod(assumedPod)

// 2. Reserve: 리소스 예약 (라인 337)
fwk.RunReservePluginsReserve(ctx, state, assumedPod, scheduleResult.SuggestedHost)
// └─ VolumeBinding: PVC 바인딩 예약
// └─ NodeResources: 메모리상 자원 예약
```

**라인 1108-1143: assume()**
```go
// Pod에 노드 이름 설정 (실제 API 갱신 전 캐시만)
assumedPodInfo.Pod.Spec.NodeName = host  // 라인 1113
sched.Cache.AssumePod(assumedPodInfo.Pod) // 라인 1132-1135
```

---

### [4] 바인딩 사이클

**파일:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L397)

```go
// 라인 397-502: bindingCycle() — 별도 goroutine에서 실행

// 1. PreBindPreFlight: nominatedNodeName 업데이트 (라인 411)
fwk.RunPreBindPreFlights(ctx, state, assumedPod, scheduleResult.SuggestedHost)

// 2. Permit: 최종 허가 대기 (라인 431)
fwk.WaitOnPermit(ctx, assumedPod)

// 3. Done: 큐에서 in-flight 제거 (라인 453)
sched.SchedulingQueue.Done(assumedPod.UID)

// 4. PreBind: 바인딩 전 처리 (라인 464)
fwk.RunPreBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)
// └─ VolumeBinding.PreBind: PVC를 실제 PV에 바인딩

// 5. Bind: API 서버에 nodeName 설정 (라인 476)
sched.bind(ctx, fwk, assumedPod, scheduleResult.SuggestedHost, state)

// 6. PostBind (라인 493)
fwk.RunPostBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)
```

**라인 1154-1177: bind()**
```go
func (sched *Scheduler) bind(ctx, fwk, assumed, targetNode, state) error {
    // Extender 바인딩 시도
    for _, extender := range fwk.HasFilterPlugins() {
        if extender.IsInterested(assumed) {
            return extender.Bind(binding)  // 외부 스케줄러가 바인딩
        }
    }
    // 기본 DefaultBinder 사용
    return fwk.RunBindPlugins(ctx, state, assumed, targetNode)
}
```

**DefaultBinder:**
```go
// API 서버에 Pod 업데이트:
// PATCH /api/v1/namespaces/{ns}/pods/{name}/binding
// body: {"target": {"name": "node-1"}}
```

---

### 확장점(Extension Points) 전체 목록

**파일:** [pkg/scheduler/framework/interface.go](../pkg/scheduler/framework/interface.go#L180)

```
PreEnqueue  → 큐 진입 전 검사
Sort        → 큐 내 정렬 순서 결정
PreFilter   → 사이클 시작 전 Pod 레벨 상태 준비
Filter      → 노드별 적합성 검사
PostFilter  → 모든 노드 실패 시 (프리엠션)
PreScore    → Score 전 공유 상태 준비
Score       → 노드별 점수 계산
NormalizeScore → 점수 정규화
Reserve     → 리소스 예약
Permit      → 바인딩 허가/대기/거부
PreBind     → 바인딩 전 처리
Bind        → 실제 바인딩
PostBind    → 바인딩 후 정리
Unreserve   → Reserve 롤백 (실패 시)
```

---

### 실패 처리 및 재큐잉

**파일:** [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go#L1200)

```go
// 라인 1200-1250+: handleSchedulingFailure()
func handleSchedulingFailure(ctx, podFwk, podInfo, status, ...) {

    // 1. Pod 조건 업데이트 (PodScheduled=False)
    // 2. 이벤트 기록
    // 3. 재큐잉
    sched.SchedulingQueue.AddUnschedulableIfNotPresent(podInfo, ...)
}
```

**이벤트 기반 재큐잉:** 클러스터 상태 변화 시 unschedulable Pod 재활성화:
- Node 추가/업데이트
- 다른 Pod 삭제 (자원 확보)
- PVC 바운드
- 기타 리소스 변화

---

## 성능 최적화 요소

| 최적화 | 위치 | 효과 |
|--------|------|------|
| 병렬 Filter | `Parallelizer().Until()` | 노드 수에 비례한 처리 속도 |
| percentageOfNodesToScore | `numFeasibleNodesToScore` | 대규모 클러스터에서 평가 노드 제한 |
| OpportunisticBatching | 라인 596-616 | 동일 spec Pod의 결과 캐시 재사용 |
| Backoff & Unschedulable Pool | PriorityQueue | 실패 Pod 즉시 재시도 방지 |
| 스냅샷 기반 캐시 | `Cache.UpdateSnapshot()` | 읽기 잠금 없는 빠른 노드 조회 |

---

## 핵심 파일 경로 요약

| 단계 | 파일 | 핵심 함수 | 라인 |
|------|------|----------|------|
| 큐 | [pkg/scheduler/backend/queue/scheduling_queue.go](../pkg/scheduler/backend/queue/scheduling_queue.go) | `PriorityQueue.Add/Pop` | 728, 945 |
| 메인 루프 | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `ScheduleOne` | 67 |
| 스케줄링 사이클 | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `schedulingCycle` | 175 |
| Filter | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `findNodesThatPassFilters` | 777 |
| Score | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `prioritizeNodes` | 943 |
| Assume | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `assumeAndReserve` | 313 |
| 바인딩 | [pkg/scheduler/schedule_one.go](../pkg/scheduler/schedule_one.go) | `bindingCycle` | 397 |
| 프레임워크 | [pkg/scheduler/framework/runtime/framework.go](../pkg/scheduler/framework/runtime/framework.go) | `Framework` 구현 | - |
| 플러그인 | [pkg/scheduler/framework/plugins/](../pkg/scheduler/framework/plugins/) | 각 플러그인 구현 | - |

---

## 관련 시나리오

- [시나리오 1: API 요청 흐름](01-api-request-flow.md) — Pod가 etcd에 저장되는 흐름
- [시나리오 4: kubelet Pod 라이프사이클](04-kubelet-pod-lifecycle.md) — 바인딩된 Pod를 kubelet이 실행하는 흐름
