# 시나리오 1: API 요청 흐름 (kubectl → kube-apiserver → etcd)

`kubectl create -f pod.yaml` 실행부터 etcd 저장까지의 전체 경로를 추적합니다.

## 전체 흐름도

```
kubectl create -f pod.yaml
        │
[1] cmd/kubectl/kubectl.go:main()
        │
[2] staging/.../kubectl/pkg/cmd/cmd.go:NewDefaultKubectlCommand()
        │ cobra Command 생성 + kubeconfig 로드
        │
[3] staging/.../client-go/rest/request.go:NewRequest()
        │ HTTP 요청 객체 구성
        │
[4] staging/.../client-go/rest/request.go:request()
        │ rate limit → retry → HTTP 전송
        │
        │ HTTPS POST /api/v1/namespaces/default/pods
        ▼
[5] cmd/kube-apiserver/app/server.go:Run()
        │ CreateServerChain()
        │
[6] staging/.../apiserver/pkg/server/config.go:DefaultBuildHandlerChain()
        │ 필터 체인 (인증 → 인가 → Admission → 핸들러)
        │
[7] staging/.../apiserver/pkg/endpoints/handlers/create.go:createHandler()
        │ 역직렬화 → Admission → Storage.Create()
        │
[8] staging/.../apiserver/pkg/registry/generic/registry/store.go:Store.Create()
        │ BeforeCreate → Validation → etcd key 생성
        │
[9] staging/.../apiserver/pkg/storage/etcd3/store.go:store.Create()
        │ Codec 직렬화 → 암호화 → etcd PUT
        │
        ▼
     etcd: /registry/pods/default/my-pod = {protobuf}
```

---

## 단계별 상세 분석

### [1] kubectl 진입점

**파일:** [cmd/kubectl/kubectl.go](../cmd/kubectl/kubectl.go#L31)

```go
// 라인 31-44
func main() {
    logs.GlogSetter(cmd.GetLogVerbosity(os.Args))
    command := cmd.NewDefaultKubectlCommand()
    if err := cli.RunNoErrOutput(command); err != nil {
        util.CheckErr(err)
    }
}
```

### [2] Cobra 명령 구성

**파일:** [staging/src/k8s.io/kubectl/pkg/cmd/cmd.go](../staging/src/k8s.io/kubectl/pkg/cmd/cmd.go#L96)

```go
// 라인 96-104
func NewDefaultKubectlCommand() *cobra.Command {
    ioStreams := genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
    return NewDefaultKubectlCommandWithArgs(KubectlOptions{
        PluginHandler: NewDefaultPluginHandler(plugin.ValidPluginFilenamePrefixes),
        Arguments:     os.Args,
        ConfigFlags:   defaultConfigFlags().WithWarningPrinter(ioStreams),
        IOStreams:      ioStreams,
    })
}
```

**핵심 역할:**
- `ConfigFlags`: `~/.kube/config`에서 인증 정보 로드
- `PluginHandler`: `kubectl-*` 플러그인 바이너리 처리

### [3] HTTP 요청 객체 생성

**파일:** [staging/src/k8s.io/client-go/rest/request.go](../staging/src/k8s.io/client-go/rest/request.go#L133)

```go
// 라인 133-176: NewRequest()
r := &Request{
    c:           c,
    rateLimiter: c.rateLimiter,   // 요청 속도 제한
    timeout:     timeout,
    pathPrefix:  path.Join("/", c.base.Path, c.versionedAPIPath),
    maxRetries:  10,
    retryFn:     defaultRequestRetryFn,
}
```

**URL 구성 예시:**
```
https://192.168.0.1:6443/api/v1/namespaces/default/pods
```

### [4] HTTP 전송 로직

**파일:** [staging/src/k8s.io/client-go/rest/request.go](../staging/src/k8s.io/client-go/rest/request.go#L1030)

```go
// 라인 1030-1121: request()
func (r *Request) request(ctx context.Context, fn func(*http.Request, *http.Response)) error {
    // 1. Rate limiting
    if err := r.tryThrottle(ctx); err != nil { return err }

    // 2. Timeout 설정
    if r.timeout > 0 {
        ctx, cancel = context.WithTimeout(ctx, r.timeout)
    }

    // 3. 재시도 루프
    retry := r.retryFn(r.maxRetries)
    for {
        req, err := r.newHTTPRequest(ctx)  // 라인 980: HTTP Request 생성
        resp, err := client.Do(req)         // 라인 1088: 실제 전송
        if retry.IsNextRetry(...) { continue }
        fn(req, resp)
        return nil
    }
}
```

**데이터 변환:**
```
pod.yaml (YAML)
    → JSON (kubectl이 Content-Type: application/json으로 전송)
    → HTTP Body
```

---

### [5] kube-apiserver 시작 및 서버 체인

**파일:** [cmd/kube-apiserver/app/server.go](../cmd/kube-apiserver/app/server.go#L148)

```go
// 라인 148-173: Run()
func Run(ctx context.Context, opts options.CompletedOptions) error {
    config, err := NewConfig(opts)
    completed, err := config.Complete()

    // 라인 162: 3-tier 서버 체인 생성
    server, err := CreateServerChain(completed)
    //  └─ APIExtensionsServer (CRD 처리)
    //      └─ KubeAPIServer (Pod, Service, Node 등 코어 API)
    //          └─ AggregatorServer (외부 API 서버 통합)

    prepared, err := server.PrepareRun()
    return prepared.Run(ctx)  // HTTPS 리스너 시작
}
```

### [6] Handler 필터 체인

**파일:** [staging/src/k8s.io/apiserver/pkg/server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go#L1036)

```
DefaultBuildHandlerChain() 라인 1036-1116 — 체인 적용 순서 (안→밖):

APIHandler (실제 처리)
    ← WithAuthorization          (라인 1040) ← RBAC 권한 확인
    ← WithImpersonation          (라인 1056) ← 사용자 위장
    ← WithAudit                  (라인 1064) ← 감시 로깅
    ← WithAuthentication         (라인 1075) ← 인증
    ← WithCORS                   (라인 1078)
    ← WithTimeoutForNonLongRunning (라인 1086)
    ← WithRequestDeadline        (라인 1088)
    ← WithWaitGroup              (라인 1090)
    ← WithPriorityAndFairness    (라인 1051) ← 요청 우선순위/공정성
    ← WithHTTPLogging            (라인 1102)
    ← WithRequestInfo            (라인 1110) ← 요청 메타데이터 추출
    ← WithPanicRecovery          (라인 1113)
    ← WithAuditInit              (라인 1114)
```

**인증 필터:**

**파일:** [staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go#L46)

```go
// 라인 46-125: WithAuthentication()
resp, ok, err := auth.AuthenticateRequest(req)  // 라인 67: 인증 체인 실행

// 인증 성공 시
req.Header.Del("Authorization")                  // 라인 89: 토큰 제거
req = req.WithContext(
    genericapirequest.WithUser(req.Context(), resp.User))  // 라인 122: 사용자 정보 주입
```

**인가 필터:**

**파일:** [staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go#L55)

```go
// 라인 55-97: withAuthorization()
attributes, err := GetAuthorizerAttributes(ctx)        // 라인 64: 요청 속성 추출
authorized, reason, err := a.Authorize(ctx, attributes) // 라인 69: 권한 확인

if authorized == authorizer.DecisionAllow {
    handler.ServeHTTP(w, req)  // 라인 82: 허용 → 다음 핸들러
    return
}
responsewriters.Forbidden(...)  // 라인 91: 403 Forbidden
```

### [7] Create 요청 핸들러

**파일:** [staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go](../staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go#L53)

```go
// 라인 53-250+: createHandler()
func createHandler(r rest.NamedCreater, scope *RequestScope, admit admission.Interface, ...) http.HandlerFunc {
    return func(w http.ResponseWriter, req *http.Request) {

        // 1. 입력 미디어 타입 협상 (라인 88)
        s, err := negotiation.NegotiateInputSerializer(req, false, scope.Serializer)

        // 2. 요청 바디 읽기 (라인 94)
        body, err := limitedReadBodyWithRecordMetric(ctx, req, scope.MaxRequestBodyBytes, ...)

        // 3. 역직렬화: body → runtime.Object (라인 129)
        obj, gvk, err := decoder.Decode(body, &defaultGVK, original)

        // 4. Audit 로깅 (라인 160)
        audit.LogRequestObject(req.Context(), obj, objGV, scope.Resource, ...)

        // 5. Admission 속성 생성 (라인 182)
        admissionAttributes := admission.NewAttributesRecord(
            obj, nil, scope.Kind, namespace, name,
            scope.Resource, scope.Subresource, admission.Create, options, ...)

        // 6. Storage에 저장 (라인 183)
        requestFunc := func() (runtime.Object, error) {
            return r.Create(ctx, name, obj,
                rest.AdmissionToValidateObjectFunc(admit, admissionAttributes, scope),
                options)
        }

        // 7. Managed Fields + Admission (라인 194)
        result, err := finisher.FinishRequest(ctx, func() (runtime.Object, error) {
            obj = scope.FieldManager.UpdateNoErrors(liveObj, obj, ...)
            return requestFunc()
        })
    }
}
```

**Admission 실행 순서:**
```
Mutating Webhooks (객체 수정 가능)
    → Validating Webhooks (검증만)
    → 내장 플러그인: ServiceAccount, LimitRanger, ResourceQuota, ...
```

### [8] Storage Layer — Store.Create()

**파일:** [staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go](../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go#L454)

```go
// 라인 485-566: store.create()
func (e *Store) create(ctx context.Context, obj runtime.Object, ...) (runtime.Object, error) {

    // 1. 메타데이터 초기화 (라인 489)
    rest.FillObjectMetaSystemFields(objectMeta)  // UID, CreationTimestamp 설정

    // 2. GenerateName 처리 (라인 494)
    objectMeta.SetName(e.CreateStrategy.GenerateName(objectMeta.GetGenerateName()))

    // 3. BeforeCreate 훅 (라인 509)
    rest.BeforeCreate(e.CreateStrategy, ctx, obj)

    // 4. Validation 실행 (라인 514)
    createValidation(ctx, obj.DeepCopyObject())

    // 5. etcd 키 생성 (라인 524)
    key, err := e.KeyFunc(ctx, name)
    // 예: "/registry/pods/default/my-pod"

    // 6. etcd 저장 (라인 534) ← 핵심
    e.Storage.Create(ctx, key, obj, out, ttl, dryrun.IsDryRun(options.DryRun))
}
```

### [9] etcd3 직접 저장

**파일:** [staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go](../staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L269)

```go
// 라인 269-334: store.Create()
func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {

    // 1. Protobuf 직렬화 (라인 289)
    data, err := runtime.Encode(s.codec, obj)

    // 2. encryption-at-rest 암호화 (라인 304)
    newData, err := s.transformer.TransformToStorage(ctx, data, authenticatedDataString(preparedKey))

    // 3. etcd 원자적 PUT (라인 311) ← 최종 저장
    txnResp, err := s.client.Kubernetes.OptimisticPut(
        ctx, preparedKey, newData, 0, kubernetes.PutOptions{LeaseID: lease})

    // 4. 저장된 데이터 역직렬화하여 반환 (라인 324)
    s.decoder.Decode(data, out, txnResp.Revision)
}
```

**데이터 변환 전체 흐름:**
```
YAML 파일
  → JSON (kubectl 직렬화)
  → runtime.Object (apiserver 역직렬화)
  → Admission (수정 가능)
  → Protobuf (etcd 저장용 codec)
  → 암호화 (encryption-at-rest, 선택적)
  → etcd 저장
```

---

## 핵심 파일 경로 요약

| 단계 | 파일 | 핵심 함수 | 라인 |
|------|------|----------|------|
| 1. CLI 진입 | [cmd/kubectl/kubectl.go](../cmd/kubectl/kubectl.go) | `main` | 31 |
| 2. 명령 구성 | [staging/.../kubectl/pkg/cmd/cmd.go](../staging/src/k8s.io/kubectl/pkg/cmd/cmd.go) | `NewDefaultKubectlCommand` | 96 |
| 3. 요청 객체 | [staging/.../client-go/rest/request.go](../staging/src/k8s.io/client-go/rest/request.go) | `NewRequest` | 133 |
| 4. HTTP 전송 | [staging/.../client-go/rest/request.go](../staging/src/k8s.io/client-go/rest/request.go) | `request` | 1030 |
| 5. 서버 시작 | [cmd/kube-apiserver/app/server.go](../cmd/kube-apiserver/app/server.go) | `Run` | 148 |
| 6. 필터 체인 | [staging/.../apiserver/pkg/server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go) | `DefaultBuildHandlerChain` | 1036 |
| 6a. 인증 | [staging/.../filters/authentication.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go) | `WithAuthentication` | 46 |
| 6b. 인가 | [staging/.../filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go) | `WithAuthorization` | 51 |
| 7. CREATE 핸들러 | [staging/.../handlers/create.go](../staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go) | `createHandler` | 53 |
| 8. Store | [staging/.../registry/store.go](../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go) | `Store.Create` | 454 |
| 9. etcd | [staging/.../storage/etcd3/store.go](../staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go) | `store.Create` | 269 |

---

## 관련 시나리오

- [시나리오 2: Pod 스케줄링](02-pod-scheduling.md) — 저장된 Pod를 스케줄러가 감지하는 흐름
- [시나리오 6: 인증/인가 상세](06-auth-flow.md) — 인증/인가 체인 상세 분석
