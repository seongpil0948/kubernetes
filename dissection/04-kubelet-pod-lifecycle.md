# 시나리오 4: kubelet Pod 라이프사이클

스케줄러가 Pod에 nodeName을 설정한 이후 kubelet이 Pod를 실행하고 종료하기까지의 전체 흐름을 추적합니다.

## 전체 흐름도

```
스케줄러: Pod.Spec.NodeName = "node-1" 설정
        │ (Watch 이벤트)
        ▼
[1] pkg/kubelet/kubelet.go:Run()
    초기화: volumeManager, statusManager, pleg.Start()
        │
        ▼
[2] syncLoop() → syncLoopIteration()
    configCh 이벤트 → HandlePodAdditions()
        │
        ▼
[3] pkg/kubelet/pod_workers.go:UpdatePod()
    Pod별 goroutine → podWorkerLoop()
        │
        ├─ 상태: SyncPod
        │       ↓
[4] pkg/kubelet/kuberuntime/kuberuntime_manager.go:SyncPod()
        │
        ├─ [4a] computePodActions() — 필요한 변경사항 계산
        ├─ [4b] createPodSandbox() — 네트워킹 설정 (CNI)
        ├─ [4c] startContainer() — CRI 호출하여 컨테이너 시작
        │       ├─ EnsureImageExists() — 이미지 Pull
        │       ├─ CreateContainer() — CRI gRPC
        │       ├─ StartContainer() — CRI gRPC
        │       └─ Lifecycle.PostStart 훅
        │
        ├─ [4d] volumeManager (병렬): Attach → Mount
        │
        └─ [4e] probeManager: liveness / readiness / startup probe 시작
        │
        ▼
[5] 종료 흐름:
    DeletionTimestamp 감지 → TerminatingPod
        │
        ├─ SyncTerminatingPod()
        │   ├─ probeManager.StopLivenessAndStartup()
        │   ├─ killPod() (SIGTERM → grace period → SIGKILL)
        │   └─ probeManager.RemovePod()
        │
        └─ SyncTerminatedPod()
            ├─ volumeManager.WaitForUnmount()
            ├─ 볼륨 디렉토리 정리
            ├─ Cgroup 제거
            └─ statusManager.TerminatePod()
```

---

## 단계별 상세 분석

### [1] kubelet 초기화

**파일:** [cmd/kubelet/kubelet.go](../cmd/kubelet/kubelet.go#L35)

```go
// 라인 35-39: main()
func main() {
    command := app.NewKubeletCommand()
    code := cli.Run(command)
    os.Exit(code)
}
```

**파일:** [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L1858)

```go
// 라인 1858-1976: Run()
func (kl *Kubelet) Run(ctx context.Context, updates <-chan kubetypes.PodUpdate) {

    // 라인 1899: 내부 모듈 초기화
    kl.initializeModules(ctx)

    // 라인 1915: 볼륨 관리자 시작 (별도 goroutine)
    go kl.volumeManager.Run(ctx, sourcesReady)

    // 라인 1956: 상태 관리자 시작 (API 서버로 상태 전송)
    kl.statusManager.Start(ctx)

    // 라인 1964: PLEG (Pod Lifecycle Event Generator) 시작
    kl.pleg.Start()

    // 라인 1975: 메인 동기화 루프 진입 (블로킹)
    kl.syncLoop(ctx, updates, kl)
}
```

**Kubelet 핵심 멤버 (라인 1193-1400+):**

| 멤버 | 역할 |
|------|------|
| `podManager` | Pod 메타데이터 추적 |
| `podWorkers` | Pod별 goroutine 관리 |
| `volumeManager` | 볼륨 Attach/Mount/Unmount |
| `probeManager` | liveness/readiness/startup probe |
| `statusManager` | API 서버에 Pod 상태 전송 |
| `containerRuntime` | CRI 인터페이스 (containerd/cri-o) |
| `pleg` | 런타임 상태 변화 감지 |

---

### [2] SyncLoop — 메인 이벤트 루프

**파일:** [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L2703)

```go
// 라인 2703-2823: syncLoopIteration() — 5가지 채널 처리

// 1. configCh: API 서버 또는 파일/HTTP에서 Pod 변경 수신
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

// 2. plegCh: 컨테이너 런타임 상태 변화 감지
case e := <-plegCh:
    if isSyncPodWorthy(e) {
        kl.HandlePodSyncs([]*v1.Pod{pod})
    }

// 3. syncCh: 1초 주기 재동기화
case <-syncCh:
    podsToSync := kl.getPodsToSync()
    kl.HandlePodSyncs(podsToSync)

// 4. Probe 업데이트
case update := <-kl.livenessManager.Updates():
    if update.Result == proberesults.Failure {
        kl.HandleProbeSync(pod)  // liveness 실패 → Pod 재시작
    }
case update := <-kl.readinessManager.Updates():
    kl.statusManager.SetContainerReadiness(...)  // ready 상태만 갱신

// 5. housekeepingCh: 2초 주기 정리
case <-housekeepingCh:
    kl.HandlePodCleanups(ctx)
```

---

### [3] Pod Worker — 상태 머신

**파일:** [pkg/kubelet/pod_workers.go](../pkg/kubelet/pod_workers.go#L108)

**Pod 상태 (라인 108-119):**
```go
type PodWorkerState int
const (
    SyncPod        PodWorkerState = iota  // 실행 중
    TerminatingPod                         // 컨테이너 정지 중
    TerminatedPod                          // 완전 종료, 리소스 정리 중
)
```

**UpdatePod() (라인 751-900+):**
```go
func (p *podWorkers) UpdatePod(ctx context.Context, options UpdatePodOptions) {
    // Pod 처음 등장 시 goroutine 생성 (라인 782-816)
    if !podUpdates, exists := p.podUpdates[uid]; !exists {
        podUpdates = make(chan struct{}, 1)
        p.podUpdates[uid] = podUpdates
        go p.podWorkerLoop(ctx, uid, podUpdates)  // Pod별 goroutine
    }

    // 종료 전환 감지 (라인 862-890)
    if options.RunningPod != nil || d.DeletionTimestamp != nil ||
       isTerminalPhase(d.Phase) || options.UpdateType == SyncPodKill {
        // 상태를 TerminatingPod로 전환
    }

    // worker 깨우기
    select {
    case podUpdates <- struct{}{}:
    default:  // 이미 큐에 있으면 무시
    }
}
```

**podWorkerLoop() (라인 1231-1363):**
```go
// Pod별 goroutine에서 실행
for range podUpdates {
    status, shouldSync, shouldTerminate := p.startPodSync(...)

    switch {
    case status == TerminatedPod:
        p.podSyncer.SyncTerminatedPod(ctx, pod, status)  // 리소스 정리
    case status == TerminatingPod:
        p.podSyncer.SyncTerminatingPod(ctx, pod, status, ...)  // 컨테이너 정지
    default:
        p.podSyncer.SyncPod(ctx, updateType, pod, mirrorPod, status)  // 실행
    }
}
```

---

### [4] SyncPod — 컨테이너 실행

**파일:** [pkg/kubelet/kuberuntime/kuberuntime_manager.go](../pkg/kubelet/kuberuntime/kuberuntime_manager.go#L1450)

```go
// 라인 1450-1800+: SyncPod()
func (m *kubeGenericRuntimeManager) SyncPod(ctx, pod, podStatus, auth, backOff) PodSyncResult {

    // 1. 변경사항 계산 (라인 1453)
    podContainerChanges := m.computePodActions(ctx, pod, podStatus)
    // 반환: 재생성할 컨테이너, init 컨테이너 변경 여부, 샌드박스 변경 여부

    // 2. 샌드박스 변경이 필요하면 기존 Pod 전체 종료 (라인 1468-1480)
    if podContainerChanges.KillPod {
        m.killPodWithSyncResult(ctx, pod, podStatus, nil)
    }

    // 3. 원치 않는 컨테이너 종료 (라인 1487-1496)
    for _, c := range podContainerChanges.ContainersToKill {
        m.killContainer(ctx, pod, c.ID, ...)
    }

    // 4. Pod 샌드박스 생성 (라인 1545-1638)
    podSandboxID, msg, err := m.createPodSandbox(ctx, pod, podContainerChanges.Attempt)
    // └─ RunPodSandbox() CRI gRPC 호출
    // └─ CNI 플러그인 호출 → Pod IP 할당, 네트워크 인터페이스 생성

    // 5. Init 컨테이너 순차 실행 (라인 1682-1700)
    for _, container := range pod.Spec.InitContainers {
        m.startContainer(ctx, podSandboxID, podSandboxConfig, &container, pod, ...)
        // 완료 후 다음 init 컨테이너로
    }

    // 6. 일반 컨테이너 실행 (라인 1745-1800)
    for _, container := range pod.Spec.Containers {
        m.startContainer(ctx, podSandboxID, podSandboxConfig, &container, pod, ...)
    }
}
```

#### [4b] Pod 샌드박스 생성

```go
// createPodSandbox() — 주요 역할:
// 1. RuntimeClass 조회
// 2. CRI: RunPodSandbox() → pause 컨테이너 (infra container) 생성
// 3. CNI: Pod IP 할당 + veth pair + iptables 규칙
// 결과: podSandboxID (이후 모든 컨테이너가 이 샌드박스 공유)
```

#### [4c] startContainer() — 컨테이너 시작

**파일:** [pkg/kubelet/kuberuntime/kuberuntime_container.go](../pkg/kubelet/kuberuntime/kuberuntime_container.go#L199)

```go
// 라인 199-339: startContainer()
func (m *kubeGenericRuntimeManager) startContainer(ctx, podSandboxID, podSandboxConfig,
    spec *v1.Container, pod *v1.Pod, ...) (string, error) {

    // Step 1: 이미지 확인 및 Pull (라인 203-219)
    imageRef, msg, err := m.imagePuller.EnsureImageExists(ctx, pod, spec, ...)
    // └─ PullImage() CRI gRPC 또는 캐시 히트

    // Step 2: 컨테이너 설정 생성 (라인 221-288)
    containerConfig, cleanupAction, err := m.generateContainerConfig(ctx, spec, pod, ...)
    // 포함: 환경변수(ConfigMap/Secret), 볼륨 마운트, 보안 컨텍스트, 리소스 제한

    // Step 3: PreCreateContainer 훅 (라인 269)
    m.internalLifecycle.PreCreateContainer(pod, spec, containerConfig)

    // Step 4: CRI CreateContainer (라인 276)
    containerID, err := m.runtimeService.CreateContainer(ctx, podSandboxID, containerConfig, podSandboxConfig)
    // └─ gRPC → containerd/cri-o → OCI 런타임(runc) → 네임스페이스/cgroup 생성

    // Step 5: PreStartContainer 훅 (라인 282)
    m.internalLifecycle.PreStartContainer(pod, spec, containerID)

    // Step 6: CRI StartContainer (라인 291)
    err = m.runtimeService.StartContainer(ctx, containerID)
    // └─ 프로세스 실제 시작 (entrypoint 실행)

    // Step 7: PostStart 생명주기 훅 (라인 318-336)
    if container.Lifecycle != nil && container.Lifecycle.PostStart != nil {
        handlerErr := m.runner.Run(ctx, containerID, pod, spec, spec.Lifecycle.PostStart)
        if handlerErr != nil {
            m.killContainer(...)  // PostStart 실패 → 컨테이너 종료
        }
    }
}
```

---

### [4d] 볼륨 관리

**파일:** [pkg/kubelet/volumemanager/volume_manager.go](../pkg/kubelet/volumemanager/volume_manager.go#L298)

```go
// 라인 298-317: Run()
func (vm *volumeManager) Run(ctx, sourcesReady) {

    // CSI 드라이버 정보 감시 (라인 304)
    go vm.volumePluginMgr.Run(ctx)

    // 원하는 볼륨 상태 추적 (라인 307)
    go vm.desiredStateOfWorldPopulator.Run(ctx, sourcesReady)
    // └─ Pod spec에서 volumes를 읽어 DesiredStateOfWorld 구성

    // 실제 상태와 조화 (라인 311)
    go vm.reconciler.Run(ctx)
    // └─ Attach → WaitForAttach → Mount 순으로 실행
    // └─ 삭제 시: Unmount → Detach
}
```

**SyncPod 내 볼륨 대기:**
```go
// kubelet.go에서 SyncPod 전 호출
kl.volumeManager.WaitForAttachAndMount(ctx, pod)
// 모든 볼륨이 마운트될 때까지 블로킹 (타임아웃 있음)
```

---

### [4e] Probe 관리

**파일:** [pkg/kubelet/prober/prober_manager.go](../pkg/kubelet/prober/prober_manager.go#L185)

```go
// 라인 185-230: AddPod()
func (m *manager) AddPod(ctx, pod) {
    for _, c := range pod.Spec.Containers {
        if c.StartupProbe != nil {
            m.AddWorker(ctx, pod, &c, startup)  // 별도 goroutine
        }
        if c.ReadinessProbe != nil {
            m.AddWorker(ctx, pod, &c, readiness)
        }
        if c.LivenessProbe != nil {
            m.AddWorker(ctx, pod, &c, liveness)
        }
    }
}
```

**파일:** [pkg/kubelet/prober/worker.go](../pkg/kubelet/prober/worker.go)

**Probe Worker 동작:**
```
1. initialDelaySeconds 대기
2. 주기적으로 probe 실행 (HTTP/TCP/gRPC/Exec)
3. 결과를 resultsManager에 저장
4. syncLoopIteration에서 결과 감지:
   - startup 실패 → 컨테이너 재시작 (restartPolicy 적용)
   - liveness 실패 → 컨테이너 재시작
   - readiness 변화 → status 업데이트 (트래픽 라우팅 영향)
```

**Probe 종류별 동작:**

| Probe | 실패 시 동작 | 성공 시 동작 |
|-------|------------|------------|
| `startupProbe` | 컨테이너 재시작 (liveness/readiness 비활성화) | liveness/readiness 활성화 |
| `livenessProbe` | 컨테이너 재시작 | 무시 |
| `readinessProbe` | Ready=False (Service 엔드포인트에서 제거) | Ready=True |

---

### [5] Graceful 종료 흐름

#### SyncTerminatingPod

**파일:** [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L2297)

```go
// 라인 2297-2416: SyncTerminatingPod()
func (kl *Kubelet) SyncTerminatingPod(ctx, pod, podStatus, gracePeriod, podStatusFn) error {

    // 1. 최종 상태 기록 (라인 2319)
    kl.statusManager.SetPodStatus(ctx, pod, apiPodStatus)

    // 2. Liveness/Startup probe 중지 (라인 2331)
    kl.probeManager.StopLivenessAndStartup(pod)
    // readiness는 계속 실행 (관찰 목적)

    // 3. Pod 종료 (라인 2334)
    kl.killPod(ctx, pod, podStatus, gracePeriod)
    // ↓
    // kuberuntime_manager.go:KillPodByID()
    //   └─ 각 컨테이너에 SIGTERM 전송
    //   └─ gracePeriod 초 대기 (기본 30초)
    //   └─ 남은 컨테이너에 SIGKILL
    //   └─ StopPodSandbox() CRI gRPC

    // 4. 모든 probe 제거 (라인 2344)
    kl.probeManager.RemovePod(pod)

    // 5. 모든 컨테이너 정지 확인 (라인 2365-2410)
    // 미정지 컨테이너 있으면 에러 반환 (재시도)
}
```

**graceful termination 타임라인:**
```
DeletionTimestamp 설정
    ├─ PreStop 훅 실행 (있으면)
    ├─ SIGTERM 전송
    ├─ terminationGracePeriodSeconds 대기 (기본 30초)
    └─ SIGKILL 전송 (아직 실행 중이면)
```

**강제 종료 시간 단축:**
```
실제 grace period = min(
    pod.Spec.TerminationGracePeriodSeconds,
    --force-delete 플래그의 gracePeriodSeconds  ← kubectl delete --grace-period=0
)
```

#### SyncTerminatedPod — 리소스 정리

**파일:** [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go#L2466)

```go
// 라인 2466-2533: SyncTerminatedPod()
func (kl *Kubelet) SyncTerminatedPod(ctx, pod, podStatus) error {

    // 1. 최종 상태 표시 (라인 2480)
    kl.statusManager.SetPodStatus(ctx, pod, apiPodStatus)

    // 2. 볼륨 언마운트 대기 (라인 2486)
    kl.volumeManager.WaitForUnmount(ctx, pod)

    // 3. 볼륨 데이터 디렉토리 삭제 (라인 2493-2502)
    kl.removeOrphanedPodVolumeDirs(pod.UID)

    // 4. Secret/ConfigMap 등록 해제 (라인 2505-2510)
    kl.secretManager.UnregisterPod(pod)
    kl.configMapManager.UnregisterPod(pod)

    // 5. Cgroup 제거 (라인 2517-2524)
    pcm.Destroy(...)  // CPU/메모리 cgroup 삭제

    // 6. User namespace 해제 (라인 2526, 활성화된 경우)
    kl.usernsManager.Release(pod.UID)

    // 7. 최종 종료 표시 (라인 2529)
    kl.statusManager.TerminatePod(logger, pod)
}
```

---

### CRI (Container Runtime Interface) 통신

kubelet과 컨테이너 런타임(containerd, CRI-O) 간의 gRPC 인터페이스:

```
kubelet
    │ gRPC (unix socket)
    ▼
CRI 런타임 (containerd / CRI-O)
    │
    ├─ RuntimeService:
    │   RunPodSandbox()       → pause 컨테이너 생성
    │   CreateContainer()     → OCI 스펙 생성
    │   StartContainer()      → runc 실행
    │   StopContainer()       → SIGTERM/SIGKILL
    │   RemoveContainer()     → 컨테이너 제거
    │   StopPodSandbox()      → 네트워크 정리
    │
    └─ ImageService:
        PullImage()            → 이미지 다운로드
        ListImages()           → 로컬 이미지 목록
        RemoveImage()          → 이미지 삭제
```

---

## Pod 상태 머신 요약

```
                        [스케줄러 바인딩]
                              │
                              ▼
Pending ──────────────────────────────────────────────────────→ Running
                              │
                   [SyncPod: 샌드박스+컨테이너 시작]
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
    [모두 완료]        [liveness 실패]   [DeletionTimestamp]
         │               [OOMKilled]          │
         ▼                    │               ▼
     Succeeded        [restartPolicy]   Terminating
                             │                │ [SIGTERM→SIGKILL]
                    ┌────────┴────────┐        │
                    ▼                ▼         ▼
                 Always         OnFailure   Terminated
                 재시작            재시작   [리소스 정리]
                                  │              │
                               Never            ▼
                               종료         Pod 삭제
```

---

## 핵심 파일 경로 요약

| 단계 | 파일 | 핵심 함수 | 라인 |
|------|------|----------|------|
| 진입점 | [cmd/kubelet/kubelet.go](../cmd/kubelet/kubelet.go) | `main` | 35 |
| 초기화 | [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go) | `Run` | 1858 |
| 메인 루프 | [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go) | `syncLoopIteration` | 2703 |
| Pod Worker | [pkg/kubelet/pod_workers.go](../pkg/kubelet/pod_workers.go) | `UpdatePod`, `podWorkerLoop` | 751, 1231 |
| SyncPod | [pkg/kubelet/kuberuntime/kuberuntime_manager.go](../pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `SyncPod` | 1450 |
| 컨테이너 시작 | [pkg/kubelet/kuberuntime/kuberuntime_container.go](../pkg/kubelet/kuberuntime/kuberuntime_container.go) | `startContainer` | 199 |
| 볼륨 관리 | [pkg/kubelet/volumemanager/volume_manager.go](../pkg/kubelet/volumemanager/volume_manager.go) | `Run` | 298 |
| Probe 관리 | [pkg/kubelet/prober/prober_manager.go](../pkg/kubelet/prober/prober_manager.go) | `AddPod` | 185 |
| Graceful 종료 | [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go) | `SyncTerminatingPod` | 2297 |
| 리소스 정리 | [pkg/kubelet/kubelet.go](../pkg/kubelet/kubelet.go) | `SyncTerminatedPod` | 2466 |

---

## 관련 시나리오

- [시나리오 2: Pod 스케줄링](02-pod-scheduling.md) — kubelet이 Pod를 받기 전 스케줄링 흐름
- [시나리오 5: Service 네트워크 라우팅](05-service-network-routing.md) — readinessProbe와 EndpointSlice의 연관
