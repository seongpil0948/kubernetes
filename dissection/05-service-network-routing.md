# 시나리오 5: Service 생성 및 네트워크 트래픽 라우팅

Service가 생성된 후 kube-proxy가 iptables 규칙을 만들어 Pod 간 통신이 이루어지기까지의 흐름을 추적합니다.

## 전체 흐름도

```
kubectl apply -f service.yaml
        │
[1] API 서버: Service 저장
        │ (Watch 이벤트)
        │
        ├──────────────────────────────────────────────┐
        ▼                                              ▼
[2a] pkg/controller/endpoint/                   [2b] pkg/controller/endpointslice/
     endpoints_controller.go                          endpointslice_controller.go
     syncService()                                    syncService()
        │                                              │
        │ Service selector로 Pod 매칭                  │ max 100개씩 슬라이스
        ▼                                              ▼
     Endpoints 객체 생성/업데이트              EndpointSlice 객체 생성/업데이트
        │                                              │
        └───────────────────┬──────────────────────────┘
                            │ (Watch 이벤트)
                            ▼
[3] 각 노드의 kube-proxy
    pkg/proxy/iptables/proxier.go
        │
        ├─ OnServiceUpdate() → serviceChanges 누적
        ├─ OnEndpointSliceUpdate() → endpointSliceCache 갱신
        └─ Sync() → syncRunner.Run()
                        │
                        ▼
[4] syncProxyRules()
        │
        ├─ serviceChanges/endpointChanges → svcPortMap, endpointsMap 업데이트
        ├─ 서비스별 iptables 체인/규칙 생성 (메모리 버퍼)
        └─ iptables-restore 원자적 적용
        │
        ▼
[5] 커널 iptables
    패킷: 클라이언트 → ClusterIP → DNAT → Pod IP
```

---

## 단계별 상세 분석

### [2a] Endpoint 컨트롤러 (Legacy)

**파일:** [pkg/controller/endpoint/endpoints_controller.go](../pkg/controller/endpoint/endpoints_controller.go#L79)

```go
// 라인 79-129: NewEndpointController() — 3개 Informer 등록
func NewEndpointController(ctx, podInformer, serviceInformer, endpointsInformer, ...) *Controller {
    e.queue = workqueue.NewTypedRateLimitingQueue(...)

    // Service 변경 → 즉시 큐 추가
    serviceInformer.Informer().AddEventHandler(...)
    // Pod 변경 → 관련 Service 큐 추가 (selector 매칭)
    podInformer.Informer().AddEventHandler(...)
}
```

**핵심 함수: syncService() (라인 334-542)**

```go
func (e *Controller) syncService(ctx, key) error {

    // 1. Service 조회 (라인 345)
    service, err := e.serviceLister.Services(namespace).Get(name)

    // 2. Service의 selector로 Pod 조회 (라인 378)
    pods, err := e.podLister.Pods(service.Namespace).List(
        labels.Set(service.Spec.Selector).AsSelectorPreValidated())

    // 3. 각 Pod를 EndpointAddress로 변환 (라인 390-430)
    for _, pod := range pods {
        if !podutil.IsPodReady(pod) {
            // ready가 아닌 Pod는 NotReadyAddresses에 추가
        }
        epAddress := podToEndpointAddressForService(svc, pod)
        // Pod IP, 포트, TargetRef(Pod 참조) 설정
    }

    // 4. 기존 Endpoints와 비교하여 변경 시 업데이트 (라인 503-509)
    if createEndpoints {
        e.client.CoreV1().Endpoints(namespace).Create(ctx, newEndpoints, ...)
    } else if endpointsChanged(currentEndpoints, newEndpoints) {
        e.client.CoreV1().Endpoints(namespace).Update(ctx, newEndpoints, ...)
    }
}
```

---

### [2b] EndpointSlice 컨트롤러 (Modern, 권장)

**파일:** [pkg/controller/endpointslice/endpointslice_controller.go](../pkg/controller/endpointslice/endpointslice_controller.go#L84)

```go
// 라인 84-190: NewController() — 4개 Informer 등록
func NewController(ctx, podInformer, serviceInformer, nodeInformer, endpointSliceInformer, ...) *Controller {

    c.reconciler = endpointslicerec.NewReconciler(
        c.client,
        c.nodeLister,
        c.maxEndpointsPerSlice,  // 기본 100
        c.endpointSliceTracker,
        c.topologyCache,         // 노드 토폴로지 (zone 정보)
        c.eventRecorder,
    )
}
```

**EndpointSlice 특징:**
- Endpoints 대비 최대 100개 단위 분할 → 대규모 클러스터 성능 개선
- 노드 토폴로지 정보 포함 (zone 기반 트래픽 최적화)
- 변경 시 전체 재작성 없이 부분 업데이트

---

### [3] kube-proxy 초기화

**파일:** [cmd/kube-proxy/app/server.go](../cmd/kube-proxy/app/server.go#L183)

```go
// 라인 183-293: newProxyServer()
func newProxyServer(ctx, config, ...) (*ProxyServer, error) {
    s := &ProxyServer{
        Config:      config,
        Client:      createClient(...),
        NodeManager: proxy.NewNodeManager(...),
        Proxier:     s.createProxier(ctx, config, ...),  // iptables/ipvs 프록시 생성
    }
}
```

**파일:** [cmd/kube-proxy/app/server_linux.go](../cmd/kube-proxy/app/server_linux.go#L129)

```go
// 라인 129-181: createProxier()
func (s *ProxyServer) createProxier(ctx, config, ...) (proxy.Provider, error) {
    if config.Mode == proxyconfigapi.ProxyModeIPTables {
        return iptables.NewProxier(
            ctx, s.PrimaryIPFamily, ipts[s.PrimaryIPFamily],
            config.SyncPeriod.Duration,     // 전체 재동기화 주기 (기본 30초)
            config.MinSyncPeriod.Duration,  // 최소 동기화 간격
            config.Linux.MasqueradeAll,
            localDetectors[s.PrimaryIPFamily],
            s.NodeName, s.NodeIPs[s.PrimaryIPFamily], ...)
    }
}
```

**파일:** [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L214)

```go
// 라인 214-324: NewProxier()
func NewProxier(ctx, ipFamily, ipt, syncPeriod, minSyncPeriod, ...) (*Proxier, error) {
    proxier := &Proxier{
        svcPortMap:       make(proxy.ServicePortMap),
        endpointsMap:     make(proxy.EndpointsMap),
        serviceChanges:   proxy.NewServiceChangeTracker(ipFamily, newServiceInfo, nil),
        endpointsChanges: proxy.NewEndpointsChangeTracker(ipFamily, nodeName, newEndpointInfo, nil),

        // BoundedFrequencyRunner: 최소 간격과 최대 간격 사이에서 syncProxyRules 실행
        syncRunner: runner.NewBoundedFrequencyRunner(
            "sync-runner", proxier.syncProxyRules, minSyncPeriod, syncPeriod, proxyutil.FullSyncPeriod),
    }

    // iptables 외부 수정 감지 (라인 314)
    go ipt.Monitor(kubeProxyCanaryChain,
        []utiliptables.Table{TableMangle, TableNAT, TableFilter},
        proxier.forceSyncProxyRules,  // 감지 시 강제 Full Sync
        syncPeriod, wait.NeverStop)
}
```

---

### [3] Watch 콜백

**파일:** [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L459)

```go
// 라인 459-498: 변경 감지 콜백

// Service 변경
func (proxier *Proxier) OnServiceUpdate(oldService, service *v1.Service) {
    if proxier.serviceChanges.Update(oldService, service) && proxier.isInitialized() {
        proxier.Sync()  // syncRunner.Run() → 가능한 빨리 syncProxyRules 실행
    }
}

// EndpointSlice 변경
func (proxier *Proxier) OnEndpointSliceAdd(endpointSlice *discovery.EndpointSlice) {
    if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, false) && proxier.isInitialized() {
        proxier.Sync()
    }
}
```

---

### [4] syncProxyRules() — iptables 규칙 생성 핵심

**파일:** [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L638)

#### 4.1 초기화 및 변경 적용

```go
// 라인 638-720: syncProxyRules()
func (proxier *Proxier) syncProxyRules() (retryError error) {
    proxier.mu.Lock()
    defer proxier.mu.Unlock()

    if !proxier.isInitialized() { return }  // Service/Endpoint 수신 전 대기

    // 전체 동기화 필요 여부 (라인 660)
    doFullSync := proxier.needFullSync ||
        (time.Since(proxier.lastFullSync) > proxyutil.FullSyncPeriod && !proxier.largeClusterMode)

    // 변경 사항 병합 (라인 668)
    serviceUpdateResult := proxier.svcPortMap.Update(proxier.serviceChanges)
    endpointUpdateResult := proxier.endpointsMap.Update(proxier.endpointsChanges)
```

#### 4.2 Jump 규칙 생성 (Full Sync 시)

```go
// 라인 687-713: iptables 점프 체인 설정
// NAT table:
//   PREROUTING → KUBE-SERVICES
//   OUTPUT     → KUBE-SERVICES
//   POSTROUTING → KUBE-POSTROUTING

// Filter table:
//   INPUT   → KUBE-EXTERNAL-SERVICES, KUBE-NODE-PORTS
//   FORWARD → KUBE-SERVICES, KUBE-FORWARD
//   OUTPUT  → KUBE-SERVICES
```

#### 4.3 서비스별 규칙 생성 (라인 829-1230)

```go
for svcName, svc := range proxier.svcPortMap {
    svcInfo, _ := svc.(*servicePortInfo)

    // Endpoint 분류
    clusterEndpoints, localEndpoints, hasEndpoints := proxy.CategorizeEndpoints(
        allEndpoints, svcInfo, proxier.nodeName, proxier.topologyLabels)

    // ClusterIP 규칙 (라인 938-957)
    if hasInternalEndpoints {
        natRules.Write(
            "-A", "KUBE-SERVICES",
            "-m", "comment", "--comment", fmt.Sprintf(`"%s cluster IP"`, svcPortNameString),
            "-m", protocol, "-p", protocol,
            "-d", svcInfo.ClusterIP().String(),
            "--dport", strconv.Itoa(svcInfo.Port()),
            "-j", string(internalTrafficChain))  // KUBE-SVC-xxx 체인
    } else {
        // Endpoint 없으면 REJECT
        filterRules.Write("-A", "KUBE-SERVICES", ..., "-j", "REJECT")
    }

    // NodePort 규칙 (라인 1025-1061)
    if svcInfo.NodePort() != 0 && hasEndpoints {
        natRules.Write(
            "-A", "KUBE-NODE-PORTS",
            "-m", protocol, "-p", protocol,
            "--dport", strconv.Itoa(svcInfo.NodePort()),
            "-j", string(externalTrafficChain))  // KUBE-EXT-xxx 체인
    }

    // LoadBalancer IP 규칙 (라인 987-1023)
    for _, lbip := range svcInfo.LoadBalancerVIPs() {
        natRules.Write(
            "-A", "KUBE-SERVICES",
            "-d", lbip.String(),
            "--dport", strconv.Itoa(svcInfo.Port()),
            "-j", string(loadBalancerTrafficChain))
    }
}
```

#### 4.4 Endpoint 로드밸런싱 규칙

**파일:** [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L1446)

```go
// 라인 1446-1490: writeServiceToEndpointRules()
func (proxier *Proxier) writeServiceToEndpointRules(natRules, svcPortNameString,
    svcInfo, svcChain, endpoints, args) {

    // Session Affinity (ClientIP 기반)
    if svcInfo.SessionAffinityType() == ServiceAffinityClientIP {
        for _, ep := range endpoints {
            natRules.Write(
                "-A", string(svcChain),
                "-m", "recent", "--name", string(epInfo.ChainName),
                "--rcheck", "--seconds", strconv.Itoa(svcInfo.StickyMaxAgeSeconds()),
                "--reap",
                "-j", string(epInfo.ChainName))  // 같은 클라이언트 → 같은 EP
        }
    }

    // 확률적 로드밸런싱 (라인 1468)
    numEndpoints := len(endpoints)
    for i, ep := range endpoints {
        args = append(args[:0], "-A", string(svcChain))

        if i < numEndpoints-1 {
            // i번째 규칙: 1/(numEndpoints-i) 확률
            args = append(args,
                "-m", "statistic", "--mode", "random",
                "--probability", proxier.probability(numEndpoints-i))
        }
        natRules.Write(args, "-j", string(epInfo.ChainName))
    }
}
```

**로드밸런싱 규칙 예시 (3개 Endpoint):**
```bash
# KUBE-SVC-XXXXXXXX (Service 체인)
-A KUBE-SVC-XXXXXXXX -m statistic --mode random --probability 0.33333333349 \
   -j KUBE-SEP-AAAAAAAAAAAA   # Pod A (33%)
-A KUBE-SVC-XXXXXXXX -m statistic --mode random --probability 0.50000000000 \
   -j KUBE-SEP-BBBBBBBBBBBB   # Pod B (50% of remaining = 33%)
-A KUBE-SVC-XXXXXXXX \
   -j KUBE-SEP-CCCCCCCCCCCC   # Pod C (100% of remaining = 33%)

# KUBE-SEP-AAAAAAAAAAAA (Endpoint 체인) - DNAT
-A KUBE-SEP-AAAAAAAAAAAA -m comment --comment "default/nginx" \
   -s 10.244.0.2 -j KUBE-MARK-MASQ   # 자기 자신에서 오는 요청 마스커레이드
-A KUBE-SEP-AAAAAAAAAAAA -m tcp -p tcp \
   -j DNAT --to-destination 10.244.0.2:8080   # DNAT
```

#### 4.5 iptables-restore 원자적 적용

```go
// 라인 1250+: 규칙 버퍼를 iptables-restore에 전달
proxier.iptablesData.Reset()
// 체인 정의 + 규칙을 단일 버퍼에 작성
// └─ *filter
// └─ :KUBE-FORWARD - [0:0]
// └─ -A KUBE-FORWARD ...
// └─ COMMIT
// └─ *nat
// └─ ...

err := proxier.iptables.Restore(table, proxier.iptablesData.Bytes(), ...)
// └─ iptables-restore --noflush --counters 실행
// └─ 원자적 적용: 중간 상태 없음
```

---

## [5] 실제 패킷 흐름

### ClusterIP 접근 시

```
Pod A (10.244.0.2) → Service ClusterIP (10.0.0.1:80)

1. SYN 패킷: 10.244.0.2:12345 → 10.0.0.1:80
   │
   └─ PREROUTING (NAT)
      └─ KUBE-SERVICES
         └─ "-d 10.0.0.1 --dport 80 -j KUBE-SVC-ABC"
            │
            └─ KUBE-SVC-ABC (로드밸런싱)
               └─ "-m statistic --probability 0.333 -j KUBE-SEP-POD-B"
                  │
                  └─ KUBE-SEP-POD-B
                     └─ DNAT: 10.0.0.1:80 → 10.244.0.3:8080
                        │
                        └─ 라우팅: Pod B로 전달

2. conntrack에 기록:
   src=10.244.0.2 dst=10.244.0.3 sport=12345 dport=8080 [ESTABLISHED]

3. 응답: 10.244.0.3:8080 → 10.244.0.2:12345 (conntrack이 역변환)
```

### NodePort 접근 시

```
외부 클라이언트 (203.0.113.5) → NodeIP:30001

1. 패킷: 203.0.113.5:54321 → 192.168.1.10:30001
   │
   └─ INPUT (NAT)
      └─ KUBE-NODE-PORTS
         └─ "--dport 30001 -j KUBE-EXT-ABC"
            │
            └─ KUBE-EXT-ABC
               └─ (externalTrafficPolicy=Cluster) → KUBE-SVC-ABC
                  └─ DNAT + 마스커레이드

2. 마스커레이드 (Source NAT):
   외부 클라이언트 IP를 노드 IP로 교체
   (Pod가 응답을 올바른 노드로 돌려보내기 위함)
```

---

## 변경 추적 구조

### ServiceChangeTracker

**파일:** [pkg/proxy/servicechangetracker.go](../pkg/proxy/servicechangetracker.go#L31)

```go
// 변경이 누적될 때까지 pending 상태 유지
type serviceChange struct {
    previous ServicePortMap  // 이전 상태
    current  ServicePortMap  // 현재 상태
}

// Update 시 delta만 기록 (변경 없으면 제거)
func (sct *ServiceChangeTracker) Update(previous, current *v1.Service) bool {
    if reflect.DeepEqual(change.previous, change.current) {
        delete(sct.items, namespacedName)
        return false  // 실제 변경 없음
    }
    return true
}
```

### EndpointSliceCache

**파일:** [pkg/proxy/endpointslicecache.go](../pkg/proxy/endpointslicecache.go#L34)

```go
type EndpointSliceCache struct {
    trackerByServiceMap map[types.NamespacedName]*endpointSliceTracker
}

type endpointSliceTracker struct {
    applied endpointSliceDataByName  // 이미 iptables에 적용된 슬라이스
    pending endpointSliceDataByName  // 다음 syncProxyRules에서 적용될 슬라이스
}
```

---

## Partial Sync vs Full Sync

| 항목 | Partial Sync | Full Sync |
|------|-------------|-----------|
| 언제 | 개별 Service/Endpoint 변경 시 | 주기적(기본 30분) 또는 외부 수정 감지 시 |
| 범위 | 변경된 Service 체인만 업데이트 | 모든 규칙 재생성 |
| 점프 규칙 | 재생성 안 함 | 재생성 |
| 성능 | 빠름 | 느리지만 일관성 보장 |

**Large Cluster Mode (1000개 이상 Endpoint):**
- 전체 동기화 주기 늘림
- 메모리 효율 최적화

---

## 주요 iptables 체인 구조

```
NAT table:

PREROUTING ──→ KUBE-SERVICES ──→ KUBE-SVC-{hash}    (ClusterIP 트래픽)
OUTPUT     ──→                └──→ KUBE-EXT-{hash}    (외부 트래픽 + NodePort)
                              └──→ KUBE-FW-{hash}     (LoadBalancer with source range)

KUBE-SVC-{hash} ──→ KUBE-SEP-{hash}  (각 Endpoint 체인, DNAT 수행)

POSTROUTING ──→ KUBE-POSTROUTING ──→ KUBE-MARK-MASQ (마스커레이드)

Filter table:

FORWARD ──→ KUBE-FORWARD   (Pod 간 트래픽 허용)
INPUT   ──→ KUBE-NODE-PORTS (NodePort 트래픽 허용)
```

---

## 핵심 파일 경로 요약

| 단계 | 파일 | 핵심 함수 | 라인 |
|------|------|----------|------|
| Endpoint 컨트롤러 | [pkg/controller/endpoint/endpoints_controller.go](../pkg/controller/endpoint/endpoints_controller.go) | `syncService` | 334 |
| EndpointSlice 컨트롤러 | [pkg/controller/endpointslice/endpointslice_controller.go](../pkg/controller/endpointslice/endpointslice_controller.go) | `NewController` | 84 |
| kube-proxy 시작 | [cmd/kube-proxy/app/server.go](../cmd/kube-proxy/app/server.go) | `newProxyServer` | 183 |
| iptables Proxier | [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go) | `NewProxier` | 214 |
| Watch 콜백 | [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go) | `OnServiceUpdate`, `OnEndpointSliceAdd` | 459 |
| 규칙 생성 핵심 | [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go) | `syncProxyRules` | 638 |
| 로드밸런싱 규칙 | [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go) | `writeServiceToEndpointRules` | 1446 |
| Service 변경 추적 | [pkg/proxy/servicechangetracker.go](../pkg/proxy/servicechangetracker.go) | `Update` | 76 |
| EndpointSlice 캐시 | [pkg/proxy/endpointslicecache.go](../pkg/proxy/endpointslicecache.go) | `checkoutChanges` | 122 |

---

## 관련 시나리오

- [시나리오 4: kubelet Pod 라이프사이클](04-kubelet-pod-lifecycle.md) — readinessProbe가 Endpoint 상태에 영향
- [시나리오 1: API 요청 흐름](01-api-request-flow.md) — Service 객체가 저장되는 흐름
