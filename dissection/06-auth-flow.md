# 시나리오 6: 인증(Authentication) 및 인가(Authorization) 흐름

API 서버로 HTTP 요청이 들어올 때 인증 → 인가 → Admission Webhook 순으로 처리되는 흐름을 추적합니다.

## 전체 흐름도

```
HTTP 요청 (kubectl / client-go)
        │
[1] TLS 핸드셰이크 (클라이언트 인증서 추출 가능)
        │
[2] Handler 필터 체인
    staging/.../apiserver/pkg/server/config.go:DefaultBuildHandlerChain()
        │
        ├─ WithRequestInfo() — URI에서 verb/resource/namespace 추출
        │
        ├─ [3] WithAuthentication() — 사용자 식별
        │       │
        │       ├─ RequestHeader (X-Remote-User 헤더)
        │       ├─ X.509 클라이언트 인증서
        │       ├─ Bearer Token
        │       │    ├─ TokenFile (static)
        │       │    ├─ ServiceAccount JWT (RSA 서명 검증)
        │       │    ├─ BootstrapToken
        │       │    ├─ OIDC JWT (외부 IdP)
        │       │    └─ Webhook Token (외부 서비스)
        │       └─ Anonymous (인증 실패 시 system:anonymous)
        │
        │ 실패 → 401 Unauthorized
        │ 성공 → Context에 UserInfo 저장
        │
        ├─ [4] WithAuthorization() — 권한 확인
        │       │
        │       ├─ 속성 추출: verb, resource, namespace, name ...
        │       │
        │       └─ RBAC 인가기
        │           ├─ ClusterRoleBinding 순회
        │           ├─ RoleBinding 순회 (namespace별)
        │           └─ PolicyRule 매칭
        │
        │ Deny → 403 Forbidden
        │ Allow → 다음 단계
        │
        └─ [5] Admission
                │
                ├─ Mutating Webhooks (객체 수정 가능)
                ├─ Validating Webhooks (정책 검사)
                └─ 내장 플러그인: ServiceAccount, LimitRanger, ResourceQuota, ...
                │
                Reject → 400/403/422
                Allow → 요청 핸들러 실행
```

---

## 단계별 상세 분석

### [2] 핸들러 필터 체인 구성

**파일:** [staging/src/k8s.io/apiserver/pkg/server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go#L1036)

```go
// 라인 1036-1116: DefaultBuildHandlerChain()
// 체인 적용 순서 (안에서 밖으로 감싸기):
handler = genericapifilters.WithAuthorization(handler, ...)      // 라인 1040
handler = genericapifilters.WithImpersonation(handler, ...)      // 라인 1056
handler = genericapifilters.WithAudit(handler, ...)              // 라인 1064
handler = genericapifilters.WithAuthentication(handler, ...)     // 라인 1075
handler = genericfilters.WithCORS(handler, ...)                  // 라인 1078
handler = genericfilters.WithTimeoutForNonLongRunning(handler, ...) // 라인 1086
handler = genericfilters.WithRequestDeadline(handler, ...)       // 라인 1088
handler = genericfilters.WithWaitGroup(handler, ...)             // 라인 1090
handler = genericapifilters.WithRequestInfo(handler, ...)        // 라인 1110
handler = genericapifilters.WithPanicRecovery(handler, ...)      // 라인 1113
handler = genericapifilters.WithAuditInit(handler, ...)          // 라인 1114
```

**WithRequestInfo()가 추출하는 정보:**

**파일:** [staging/src/k8s.io/apiserver/pkg/endpoints/request/requestinfo.go](../staging/src/k8s.io/apiserver/pkg/endpoints/request/requestinfo.go)

```go
type RequestInfo struct {
    IsResourceRequest bool
    Path              string   // e.g. "/api/v1/namespaces/default/pods"
    Verb              string   // get, list, watch, create, update, patch, delete
    APIPrefix         string   // "api" or "apis"
    APIGroup          string   // "" or "apps"
    APIVersion        string   // "v1" or "v1beta1"
    Namespace         string   // "default"
    Resource          string   // "pods"
    Subresource       string   // "status", "log", ""
    Name              string   // "my-pod"
}
```

---

### [3] 인증 (Authentication)

**파일:** [staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go#L46)

```go
// 라인 46-125: WithAuthentication() / withAuthentication()
func withAuthentication(handler http.Handler, auth authenticator.Request, failed http.Handler, ...) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

        authenticationStart := time.Now()

        // 라인 67: 인증 체인 실행
        resp, ok, err := auth.AuthenticateRequest(req)

        if err != nil || !ok {
            failed.ServeHTTP(w, req)  // 401 Unauthorized
            return
        }

        // 라인 89: 인증 성공 후 Authorization 헤더 제거 (보안)
        req.Header.Del("Authorization")

        // audience 검증 (OIDC 등)
        if !audiencesAreAcceptable(apiAuds, resp.Audiences) {
            failed.ServeHTTP(w, req)
            return
        }

        // 라인 122: Context에 사용자 정보 저장
        req = req.WithContext(genericapirequest.WithUser(req.Context(), resp.User))
        handler.ServeHTTP(w, req)
    })
}
```

**UserInfo 인터페이스:**
```go
type Info interface {
    GetName()   string    // e.g. "alice", "system:serviceaccount:default:myapp"
    GetUID()    string
    GetGroups() []string  // e.g. ["system:masters", "system:authenticated"]
    GetExtra()  map[string][]string
}
```

#### [3a] 인증기 구성

**파일:** [pkg/kubeapiserver/authenticator/config.go](../pkg/kubeapiserver/authenticator/config.go#L107)

```go
// 라인 107-249: Config.New() — 인증기 체인 구성
func (config Config) New(ctx context.Context, ...) (authenticator.Request, ...) {

    var authenticators []authenticator.Request

    // 1. Front Proxy (라인 115): X-Remote-User 헤더 (신뢰된 프록시에서)
    if config.RequestHeaderConfig != nil {
        authenticators = append(authenticators,
            headerrequest.NewDynamicVerifyOptionsSecure(...))
    }

    // 2. X.509 클라이언트 인증서 (라인 128)
    if config.ClientCAContentProvider != nil {
        authenticators = append(authenticators,
            x509.NewDynamic(config.ClientCAContentProvider.VerifyOptions, x509.CommonNameUserConversion))
    }

    // 3. Bearer Token들 (라인 134-201)
    var tokenAuthenticators []authenticator.Token

    // 3a. 정적 토큰 파일
    if len(config.TokenAuthFile) != 0 {
        tokenAuthenticators = append(tokenAuthenticators,
            tokenfile.NewCSV(...))
    }

    // 3b. ServiceAccount 토큰 (라인 153)
    if config.ServiceAccountConfig.Lookup {
        tokenAuthenticators = append(tokenAuthenticators,
            serviceaccount.NewValidator(
                serviceaccount.LegacyTokenValidator(...),
                serviceaccount.ExtendedTokenValidator(...)))
    }

    // 3c. OIDC JWT (라인 167)
    if len(config.OIDCConfig.IssuerURL) != 0 {
        oidcAuth, err := oidc.New(ctx, config.OIDCConfig)
        tokenAuthenticators = append(tokenAuthenticators, oidcAuth)
    }

    // 3d. Webhook Token (라인 195)
    if config.WebhookTokenAuthnConfigFile != "" {
        webhookTokenAuth, _ := webhook.New(config.WebhookTokenAuthnConfigFile, ...)
        tokenAuthenticators = append(tokenAuthenticators, webhookTokenAuth)
    }

    // Token 인증기들을 union으로 묶음
    tokenAuth := tokenunion.New(tokenAuthenticators...)

    // Token 결과 캐싱 (라인 208)
    if config.TokenSuccessCacheTTL > 0 || config.TokenFailureCacheTTL > 0 {
        tokenAuth = tokencache.New(tokenAuth, true,
            config.TokenSuccessCacheTTL, config.TokenFailureCacheTTL)
    }

    // Bearer Token 인증기 추가
    authenticators = append(authenticators,
        bearertoken.New(tokenAuth))

    // 4. Anonymous 인증기 (라인 242): 마지막 fallback
    if config.Anonymous.Enabled {
        authenticators = append(authenticators,
            anonymous.NewAuthenticator(config.Anonymous.Conditions))
    }

    // 전체 chain 반환
    return union.New(authenticators...), ...
}
```

#### [3b] ServiceAccount JWT 검증 흐름

```
요청 헤더: Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
    │
    ▼
bearertoken.AuthenticateRequest()
    │ 헤더에서 토큰 추출
    ▼
serviceaccount.Validator.AuthenticateToken()
    │
    ├─ JWT 파싱 (header.payload.signature)
    ├─ 서명 검증 (API 서버의 공개키로)
    ├─ 만료 시간 확인
    ├─ API 서버에 ServiceAccount 실존 확인 (Lookup=true 시)
    └─ UserInfo 반환:
       Name:   "system:serviceaccount:default:myapp"
       Groups: ["system:serviceaccounts", "system:serviceaccounts:default"]
```

#### [3c] X.509 인증서 검증 흐름

**파일:** [staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go](../staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go#L138)

```go
// 라인 138-150: AuthenticateRequest()
func (a *Authenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {

    // TLS 없거나 인증서 없으면 통과
    if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
        return nil, false, nil
    }

    // 인증서 검증 (CA 체인)
    optsCopy, ok := a.verifyOptionsFn()
    chains, err := req.TLS.PeerCertificates[0].Verify(optsCopy)

    // CommonName → username, Organization → groups
    // e.g. CN=alice, O=system:masters
    return a.user.User(req.TLS.PeerCertificates[0], chains)
}
```

---

### [4] 인가 (Authorization)

**파일:** [staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go#L55)

```go
// 라인 55-97: withAuthorization()
func withAuthorization(handler http.Handler, a authorizer.UnconditionalAuthorizer, ...) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

        // 라인 64: 요청 속성 추출
        attributes, err := GetAuthorizerAttributes(ctx)

        // 라인 69: 인가 결정
        authorized, reason, err := a.Authorize(ctx, attributes)

        // 라인 79: Allow
        if authorized == authorizer.DecisionAllow {
            audit.AddAuditAnnotations(ctx, "authorization.k8s.io/decision", "allow", ...)
            handler.ServeHTTP(w, req)
            return
        }

        // 라인 91: Forbidden
        audit.AddAuditAnnotations(ctx, "authorization.k8s.io/decision", "forbid", ...)
        responsewriters.Forbidden(attributes, w, req, reason, s)
    })
}
```

**속성 추출 (라인 99-149):**
```go
func GetAuthorizerAttributes(ctx context.Context) (authorizer.Attributes, error) {
    attribs := authorizer.AttributesRecord{
        User:              user,           // 인증된 사용자
        Verb:              requestInfo.Verb,         // get/list/create/...
        Resource:          requestInfo.Resource,     // pods/services/...
        Subresource:       requestInfo.Subresource,  // status/log/...
        Namespace:         requestInfo.Namespace,
        Name:              requestInfo.Name,
        APIGroup:          requestInfo.APIGroup,
        APIVersion:        requestInfo.APIVersion,
        ResourceRequest:   requestInfo.IsResourceRequest,
        Path:              requestInfo.Path,
    }
}
```

#### [4a] RBAC 인가기

**파일:** [plugin/pkg/auth/authorizer/rbac/rbac.go](../plugin/pkg/auth/authorizer/rbac/rbac.go#L78)

```go
// 라인 78-130: Authorize()
func (r *RBACAuthorizer) Authorize(ctx context.Context, requestAttributes authorizer.Attributes) (authorizer.Decision, string, error) {

    // 라인 79: 규칙 방문자 패턴
    ruleCheckingVisitor := &authorizingVisitor{requestAttributes: requestAttributes}

    r.authorizationRuleResolver.VisitRulesFor(ctx,
        requestAttributes.GetUser(),
        requestAttributes.GetNamespace(),
        ruleCheckingVisitor.visit)

    // 라인 82: 매칭 성공
    if ruleCheckingVisitor.allowed {
        return authorizer.DecisionAllow, ruleCheckingVisitor.reason, nil
    }

    // 라인 100+: 실패 사유 구성
    // "user \"alice\" cannot create pods in namespace \"default\""
    return authorizer.DecisionNoOpinion, reason, nil
}
```

**규칙 방문 (pkg/registry/rbac/validation/rule.go):**

```go
// VisitRulesFor() 동작:
1. ClusterRoleBinding 목록 순회
   └─ user/group이 Subject에 포함되면
      → ClusterRole의 rules 추출
      → visit(nil, rule) 호출

2. namespace가 있으면 RoleBinding 목록 순회
   └─ user/group이 Subject에 포함되면
      → Role/ClusterRole의 rules 추출
      → visit(namespace, rule) 호출
```

**규칙 매칭 (라인 181-196):**
```go
// RuleAllows() — 요청이 규칙에 매칭되는지 확인
func RuleAllows(requestAttributes authorizer.Attributes, rule *rbacv1.PolicyRule) bool {
    if requestAttributes.IsResourceRequest() {
        combinedResource := resource + "/" + subresource  // e.g. "pods/log"
        return VerbMatches(rule, verb) &&        // "get" in ["get","list"]
            APIGroupMatches(rule, apiGroup) &&   // "" in [""]
            ResourceMatches(rule, resource, subresource) &&  // "pods" in ["pods","services"]
            ResourceNameMatches(rule, name)      // "" (전체) or "my-pod"
    }
    // Non-resource URL 요청
    return VerbMatches(rule, verb) &&
        NonResourceURLMatches(rule, path)
}
```

**RBAC 예시:**

```yaml
# ClusterRole
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch"]

# 요청: GET /api/v1/namespaces/default/pods/my-pod
# Verb: get, Resource: pods, APIGroup: "", Namespace: default, Name: my-pod
# → VerbMatches("get", ["get","list","watch"]) = true
# → APIGroupMatches("", [""]) = true
# → ResourceMatches("pods", ["pods"]) = true
# → ResourceNameMatches("my-pod", []) = true (빈 배열 = 모두 허용)
# → ALLOW
```

---

### [5] Admission 제어

**파일:** [staging/src/k8s.io/apiserver/pkg/admission/chain.go](../staging/src/k8s.io/apiserver/pkg/admission/chain.go#L31)

```go
// 라인 31-44: Admit() — Mutating
func (admissionHandler chainAdmissionHandler) Admit(ctx, a Attributes, o ObjectInterfaces) error {
    for _, handler := range admissionHandler {
        if !handler.Handles(a.GetOperation()) { continue }
        if mutator, ok := handler.(MutationInterface); ok {
            if err := mutator.Admit(ctx, a, o); err != nil {
                return err  // 하나라도 실패 → 요청 거부
            }
        }
    }
    return nil
}

// 라인 47-60: Validate() — Validating
func (admissionHandler chainAdmissionHandler) Validate(ctx, a Attributes, o ObjectInterfaces) error {
    for _, handler := range admissionHandler {
        if !handler.Handles(a.GetOperation()) { continue }
        if validator, ok := handler.(ValidationInterface); ok {
            if err := validator.Validate(ctx, a, o); err != nil {
                return err
            }
        }
    }
    return nil
}
```

**Admission 실행 순서:**
```
1. Mutating Admission Webhooks (병렬 → 직렬 처리)
   └─ 객체 수정 가능 (defaulting, injection 등)
   └─ 예: Istio sidecar 주입, ServiceAccount 볼륨 마운트 추가

2. Schema Validation (수정된 객체 재검증)

3. Validating Admission Webhooks (병렬 처리)
   └─ 수정 불가, 허용/거부만
   └─ 예: OPA Gatekeeper, Kyverno

4. 내장 플러그인:
   - ServiceAccount: SA 토큰 시크릿 마운트
   - LimitRanger: 기본 resource request/limit 설정
   - ResourceQuota: 네임스페이스 쿼터 확인
   - PodSecurity: PSS 정책 확인
   - NamespaceLifecycle: 삭제 중인 네임스페이스에 새 리소스 방지
```

**Admission 속성:**

**파일:** [staging/src/k8s.io/apiserver/pkg/admission/interfaces.go](../staging/src/k8s.io/apiserver/pkg/admission/interfaces.go#L29)

```go
type Attributes interface {
    GetName() string
    GetNamespace() string
    GetResource() schema.GroupVersionResource
    GetSubresource() string
    GetOperation() Operation           // CREATE, UPDATE, DELETE, CONNECT
    GetObject() runtime.Object        // 요청 객체 (수정 가능 - Mutating)
    GetOldObject() runtime.Object     // 기존 객체 (UPDATE/DELETE)
    GetUserInfo() user.Info
    IsDryRun() bool
    GetKind() schema.GroupVersionKind
}
```

---

### Impersonation (위장)

**파일:** [staging/src/k8s.io/apiserver/pkg/server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go#L1056)

```go
// 라인 1056-1060: WithImpersonation()
// 헤더 Impersonate-User, Impersonate-Group 처리
// 인가 조건: 요청자가 impersonation RBAC 권한 보유 시 다른 사용자로 작업
```

```bash
# 예시: admin이 alice로 가장하여 요청
kubectl get pods --as=alice --as-group=developers
# → 헤더: Impersonate-User: alice, Impersonate-Group: developers
```

---

## 에러 처리 요약

| 단계 | 에러 | HTTP 코드 |
|------|------|---------|
| 인증 실패 (토큰 없음) | - | 401 Unauthorized |
| 인증 실패 (잘못된 토큰) | - | 401 Unauthorized |
| 인가 실패 | "user X cannot Y Z" | 403 Forbidden |
| Admission Validation 실패 | "field X: value Y is invalid" | 422 Unprocessable Entity |
| Admission Policy 거부 | Webhook 응답 메시지 | 403 Forbidden |
| Admission 내부 오류 | - | 500 Internal Server Error |

---

## 감사(Audit) 로그

인증/인가 결과는 Audit Log에 자동 기록됩니다:

```json
{
  "kind": "Event",
  "apiVersion": "audit.k8s.io/v1",
  "requestURI": "/api/v1/namespaces/default/pods",
  "verb": "create",
  "user": {"username": "alice", "groups": ["system:authenticated"]},
  "objectRef": {"resource": "pods", "namespace": "default"},
  "annotations": {
    "authorization.k8s.io/decision": "allow",
    "authorization.k8s.io/reason": "RBAC: allowed by ClusterRoleBinding"
  },
  "responseStatus": {"code": 201}
}
```

---

## 핵심 파일 경로 요약

| 단계 | 파일 | 핵심 함수 | 라인 |
|------|------|----------|------|
| 필터 체인 | [staging/.../server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go) | `DefaultBuildHandlerChain` | 1036 |
| 인증 필터 | [staging/.../filters/authentication.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go) | `WithAuthentication` | 46 |
| 인증기 구성 | [pkg/kubeapiserver/authenticator/config.go](../pkg/kubeapiserver/authenticator/config.go) | `Config.New` | 107 |
| X.509 인증 | [staging/.../authentication/request/x509/x509.go](../staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go) | `AuthenticateRequest` | 138 |
| Bearer 토큰 | [staging/.../authentication/request/bearertoken/bearertoken.go](../staging/src/k8s.io/apiserver/pkg/authentication/request/bearertoken/bearertoken.go) | `AuthenticateRequest` | 42 |
| 인가 필터 | [staging/.../filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go) | `WithAuthorization` | 51 |
| 속성 추출 | [staging/.../filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go) | `GetAuthorizerAttributes` | 99 |
| RBAC 인가 | [plugin/pkg/auth/authorizer/rbac/rbac.go](../plugin/pkg/auth/authorizer/rbac/rbac.go) | `Authorize` | 78 |
| 규칙 매칭 | [plugin/pkg/auth/authorizer/rbac/rbac.go](../plugin/pkg/auth/authorizer/rbac/rbac.go) | `RuleAllows` | 181 |
| 규칙 방문 | [pkg/registry/rbac/validation/rule.go](../pkg/registry/rbac/validation/rule.go) | `VisitRulesFor` | 179 |
| Admission 체인 | [staging/.../admission/chain.go](../staging/src/k8s.io/apiserver/pkg/admission/chain.go) | `Admit`, `Validate` | 31, 47 |

---

## 관련 시나리오

- [시나리오 1: API 요청 흐름](01-api-request-flow.md) — 인증/인가 이후의 처리 흐름
- [시나리오 3: Deployment 롤링 업데이트](03-deployment-rolling-update.md) — 컨트롤러가 API 호출 시 인증되는 방식 (ServiceAccount)
