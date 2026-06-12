# Kubernetes Source Code Dissection

Kubernetes 오픈소스 코드를 주요 시나리오별로 End-to-End 분석한 문서 모음입니다.
각 문서는 실제 파일 경로, 함수명, 라인 번호를 기반으로 코드 흐름을 추적합니다.

## 시나리오 목록

| # | 시나리오 | 문서 | 핵심 컴포넌트 |
|---|---------|------|-------------|
| 1 | API 요청 흐름 | [01-api-request-flow.md](01-api-request-flow.md) | kubectl → client-go → kube-apiserver → etcd |
| 2 | Pod 스케줄링 | [02-pod-scheduling.md](02-pod-scheduling.md) | kube-scheduler → Filter → Score → Bind |
| 3 | Deployment 롤링 업데이트 | [03-deployment-rolling-update.md](03-deployment-rolling-update.md) | DeploymentController → ReplicaSetController → Pod |
| 4 | kubelet Pod 라이프사이클 | [04-kubelet-pod-lifecycle.md](04-kubelet-pod-lifecycle.md) | kubelet → CRI → Volume → Probe → Graceful shutdown |
| 5 | Service 네트워크 라우팅 | [05-service-network-routing.md](05-service-network-routing.md) | EndpointSlice → kube-proxy → iptables |
| 6 | 인증/인가 흐름 | [06-auth-flow.md](06-auth-flow.md) | Authenticator → RBAC → Admission Webhook |

## 코드 베이스 구조 요약

```
kubernetes/
├── cmd/                         # 각 컴포넌트 바이너리 진입점
│   ├── kube-apiserver/          # API 서버
│   ├── kube-scheduler/          # 스케줄러
│   ├── kube-controller-manager/ # 컨트롤러 매니저
│   ├── kube-proxy/              # 프록시
│   ├── kubelet/                 # 노드 에이전트
│   └── kubectl/                 # CLI 클라이언트
├── pkg/                         # 내부 패키지
│   ├── scheduler/               # 스케줄러 핵심 로직
│   ├── controller/              # 컨트롤러들 (deployment, rs, endpoint 등)
│   ├── kubelet/                 # kubelet 핵심 로직
│   ├── proxy/                   # kube-proxy 핵심 로직
│   ├── kubeapiserver/           # API 서버 설정
│   └── auth/                    # 인증/인가
├── staging/src/k8s.io/          # 독립 라이브러리 (향후 별도 repo)
│   ├── client-go/               # Go 클라이언트 라이브러리
│   ├── apiserver/               # 범용 API 서버 프레임워크
│   └── api/                     # API 타입 정의
└── plugin/pkg/auth/authorizer/  # RBAC 인가기
```

## 공통 패턴

### Informer + WorkQueue 패턴
모든 컨트롤러가 사용하는 기본 패턴:
```
API Server
    │ Watch (Long Polling)
    ▼
Informer (로컬 캐시)
    │ 이벤트 (Add/Update/Delete)
    ▼
WorkQueue (Rate-limited)
    │ key (namespace/name)
    ▼
syncXxx() 함수
    │ 현재 상태 vs 원하는 상태 비교
    ▼
API 호출로 상태 조정
```

### Reconciliation Loop
```go
// 컨트롤러의 핵심 패턴
for {
    desired := getDesiredState()  // spec에서
    actual  := getActualState()   // 현재 클러스터 상태
    if desired != actual {
        reconcile(desired, actual)  // 차이를 메움
    }
}
```
