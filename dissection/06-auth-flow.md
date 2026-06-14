# Scenario 6: Authentication and Authorization Flow

Traces how an HTTP request arriving at the API server is processed in the order authentication → authorization → admission webhooks.

## Big Picture

This path is a layered security gate. Authentication establishes identity, authorization evaluates permissions for that identity, and admission enforces object-level policy before storage handlers execute. Each layer receives richer context from the previous layer and can terminate the request independently.

## Interface Resolution Guide (This Scenario)

Security pipeline code crosses many interfaces (`authenticator.Request`, `authorizer`, `admission.Interface`). Use this order:
1. Locate the interface method invoked in the filter.
2. Find concrete implementers via assertions when available.
3. Follow factory/registry assembly for chains where assertions are absent.
4. Confirm runtime chain composition order because order changes behavior.

### Worked Example: `authenticator.Request` -> `*unionAuthRequestHandler`

The API server's request authenticator is a good example of a hidden concrete type:

1. Interface: `type Request interface` in [../staging/src/k8s.io/apiserver/pkg/authentication/authenticator/interfaces.go](../staging/src/k8s.io/apiserver/pkg/authentication/authenticator/interfaces.go#L34).
2. Concrete struct: `unionAuthRequestHandler` in [../staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go](../staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go#L27).
3. Factory that hides the type: `union.New(...) authenticator.Request` in [../staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go](../staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go#L36) returns `&unionAuthRequestHandler{...}` in [../staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go](../staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go#L40).
4. DI wiring: kube-apiserver appends concrete authenticators and wraps them with `union.New(authenticators...)` in [../pkg/kubeapiserver/authenticator/config.go](../pkg/kubeapiserver/authenticator/config.go#L211) and [../pkg/kubeapiserver/authenticator/config.go](../pkg/kubeapiserver/authenticator/config.go#L238).
5. Runtime call site: `WithAuthentication()` invokes `auth.AuthenticateRequest(req)` in [../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go#L67), so the concrete union handler iterates the request authenticators it was wired with.

This is why "follow the wiring" matters here: there is usually no `var _ authenticator.Request = &...{}` breadcrumb, but the constructor and chain assembly still show the real runtime object.

## Reading Guide (Beginner)

- **Trigger:** every API request traverses this chain before resource handlers run.
- **Stage ownership:** authentication identifies caller, authorization checks permission, admission checks/mutates object.
- **Error-code map:** authn failure -> 401, authz denial -> 403, admission/schema/policy rejection -> 4xx (often 422/400/403).
- **Success criterion for this scenario:** request reaches the storage handler with an accepted identity, allowed action, and admissible object.
- **Most common confusion:** valid credentials do not imply RBAC permission.

## Overall Flow Diagram

```
HTTP request (kubectl / client-go)
        │
[1] TLS handshake (client certificate can be extracted)
        │
[2] Handler filter chain
    staging/.../apiserver/pkg/server/config.go:DefaultBuildHandlerChain()
        │
        ├─ WithRequestInfo() — extracts verb/resource/namespace from the URI
        │
        ├─ [3] WithAuthentication() — identifies the user
        │       │
        │       ├─ RequestHeader (X-Remote-User header)
        │       ├─ X.509 client certificate
        │       ├─ Bearer Token
        │       │    ├─ TokenFile (static)
        │       │    ├─ ServiceAccount JWT (RSA signature verification)
        │       │    ├─ BootstrapToken
        │       │    ├─ OIDC JWT (external IdP)
        │       │    └─ Webhook Token (external service)
        │       └─ Anonymous (system:anonymous on authentication failure)
        │
        │ Failure → 401 Unauthorized
        │ Success → UserInfo stored in Context
        │
        ├─ [4] WithAuthorization() — permission check
        │       │
        │       ├─ Attribute extraction: verb, resource, namespace, name ...
        │       │
        │       └─ RBAC authorizer
        │           ├─ Iterate ClusterRoleBindings
        │           ├─ Iterate RoleBindings (per namespace)
        │           └─ PolicyRule matching
        │
        │ Deny → 403 Forbidden
        │ Allow → next stage
        │
        └─ [5] Admission
                │
                ├─ Mutating Webhooks (may modify objects)
                ├─ Validating Webhooks (policy checks)
                └─ Built-in plugins: ServiceAccount, LimitRanger, ResourceQuota, ...
                │
                Reject → 400/403/422
                Allow → request handler executes
```

---

## Detailed Step-by-Step Analysis

### [2] Handler Filter Chain Construction

**File:** [staging/src/k8s.io/apiserver/pkg/server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go#L1036)

```go
// Lines 1036-1116: DefaultBuildHandlerChain()
// Chain application order (wrapping from inside out):
handler = genericapifilters.WithAuthorization(handler, ...)      // line 1040
handler = genericapifilters.WithImpersonation(handler, ...)      // line 1056
handler = genericapifilters.WithAudit(handler, ...)              // line 1064
handler = genericapifilters.WithAuthentication(handler, ...)     // line 1075
handler = genericfilters.WithCORS(handler, ...)                  // line 1078
handler = genericfilters.WithTimeoutForNonLongRunning(handler, ...) // line 1086
handler = genericfilters.WithRequestDeadline(handler, ...)       // line 1088
handler = genericfilters.WithWaitGroup(handler, ...)             // line 1090
handler = genericapifilters.WithRequestInfo(handler, ...)        // line 1110
handler = genericapifilters.WithPanicRecovery(handler, ...)      // line 1113
handler = genericapifilters.WithAuditInit(handler, ...)          // line 1114
```

**Information extracted by WithRequestInfo():**

**File:** [staging/src/k8s.io/apiserver/pkg/endpoints/request/requestinfo.go](../staging/src/k8s.io/apiserver/pkg/endpoints/request/requestinfo.go)

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

### [3] Authentication

**File:** [staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go#L46)

```go
// Lines 46-125: WithAuthentication() / withAuthentication()
func withAuthentication(handler http.Handler, auth authenticator.Request, failed http.Handler, ...) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

        authenticationStart := time.Now()

        // Line 67: run the authentication chain
        resp, ok, err := auth.AuthenticateRequest(req)

        if err != nil || !ok {
            failed.ServeHTTP(w, req)  // 401 Unauthorized
            return
        }

        // Line 89: remove the Authorization header after successful authentication (security)
        req.Header.Del("Authorization")

        // audience validation (OIDC etc.)
        if !audiencesAreAcceptable(apiAuds, resp.Audiences) {
            failed.ServeHTTP(w, req)
            return
        }

        // Line 122: store user info in Context
        req = req.WithContext(genericapirequest.WithUser(req.Context(), resp.User))
        handler.ServeHTTP(w, req)
    })
}
```

**UserInfo interface:**
```go
type Info interface {
    GetName()   string    // e.g. "alice", "system:serviceaccount:default:myapp"
    GetUID()    string
    GetGroups() []string  // e.g. ["system:masters", "system:authenticated"]
    GetExtra()  map[string][]string
}
```

> ⚠️ **Concrete `user.Info` implementation in this path:** `user.DefaultInfo` ([authentication/user/user.go:46](../staging/src/k8s.io/apiserver/pkg/authentication/user/user.go#L46)). You can see it instantiated directly by concrete request authenticators, for example anonymous auth ([anonymous.go:43](../staging/src/k8s.io/apiserver/pkg/authentication/request/anonymous/anonymous.go#L43)) and x509 CN conversion ([x509.go:293](../staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go#L293)).

#### [3a] Authenticator Configuration

**File:** [pkg/kubeapiserver/authenticator/config.go](../pkg/kubeapiserver/authenticator/config.go#L107)

```go
// Lines 107-249: Config.New() — builds the authenticator chain
func (config Config) New(ctx context.Context, ...) (authenticator.Request, ...) {

    var authenticators []authenticator.Request

    // 1. Front Proxy (line 115): X-Remote-User header (from a trusted proxy)
    if config.RequestHeaderConfig != nil {
        authenticators = append(authenticators,
            headerrequest.NewDynamicVerifyOptionsSecure(...))
    }

    // 2. X.509 client certificate (line 128)
    if config.ClientCAContentProvider != nil {
        authenticators = append(authenticators,
            x509.NewDynamic(config.ClientCAContentProvider.VerifyOptions, x509.CommonNameUserConversion))
    }

    // 3. Bearer Tokens (lines 134-201)
    var tokenAuthenticators []authenticator.Token

    // 3a. Static token file
    if len(config.TokenAuthFile) != 0 {
        tokenAuthenticators = append(tokenAuthenticators,
            tokenfile.NewCSV(...))
    }

    // 3b. ServiceAccount token (line 153)
    if config.ServiceAccountConfig.Lookup {
        tokenAuthenticators = append(tokenAuthenticators,
            serviceaccount.NewValidator(
                serviceaccount.LegacyTokenValidator(...),
                serviceaccount.ExtendedTokenValidator(...)))
    }

    // 3c. OIDC JWT (line 167)
    if len(config.OIDCConfig.IssuerURL) != 0 {
        oidcAuth, err := oidc.New(ctx, config.OIDCConfig)
        tokenAuthenticators = append(tokenAuthenticators, oidcAuth)
    }

    // 3d. Webhook Token (line 195)
    if config.WebhookTokenAuthnConfigFile != "" {
        webhookTokenAuth, _ := webhook.New(config.WebhookTokenAuthnConfigFile, ...)
        tokenAuthenticators = append(tokenAuthenticators, webhookTokenAuth)
    }

    // Combine the token authenticators into a union
    tokenAuth := tokenunion.New(tokenAuthenticators...)

    // Token result caching (line 208)
    if config.TokenSuccessCacheTTL > 0 || config.TokenFailureCacheTTL > 0 {
        tokenAuth = tokencache.New(tokenAuth, true,
            config.TokenSuccessCacheTTL, config.TokenFailureCacheTTL)
    }

    // Add the Bearer Token authenticator
    authenticators = append(authenticators,
        bearertoken.New(tokenAuth))

    // 4. Anonymous authenticator (line 242): final fallback
    if config.Anonymous.Enabled {
        authenticators = append(authenticators,
            anonymous.NewAuthenticator(config.Anonymous.Conditions))
    }

    // Return the full chain
    return union.New(authenticators...), ...
}
```

> ⚠️ **What does `union.New(...)` return?** Do a full interface trace:
> 1. Interface: `authenticator.Request` ([authenticator/interfaces.go:34](../staging/src/k8s.io/apiserver/pkg/authentication/authenticator/interfaces.go#L34)).
> 2. Concrete struct: `unionAuthRequestHandler` ([request/union/union.go:27](../staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go#L27)).
> 3. Factory that hides the type: `New(... ) authenticator.Request` ([union.go:36](../staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go#L36)) returns `&unionAuthRequestHandler{...}` ([union.go:40](../staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go#L40)).
> 4. DI wiring: kube-apiserver appends concrete request authenticators (`x509.NewDynamic`, `bearertoken.New`, `anonymous.NewAuthenticator`) in `Config.New` ([pkg/kubeapiserver/authenticator/config.go:128](../pkg/kubeapiserver/authenticator/config.go#L128), [config.go:211](../pkg/kubeapiserver/authenticator/config.go#L211), [config.go:233](../pkg/kubeapiserver/authenticator/config.go#L233)), then wraps them with `union.New(authenticators...)` ([config.go:238](../pkg/kubeapiserver/authenticator/config.go#L238)).
> 5. Concrete examples under that interface boundary: `x509.Authenticator` ([x509.go:120](../staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go#L120)), `bearertoken.Authenticator` ([bearertoken.go:32](../staging/src/k8s.io/apiserver/pkg/authentication/request/bearertoken/bearertoken.go#L32)), and `anonymous.Authenticator` ([anonymous.go:32](../staging/src/k8s.io/apiserver/pkg/authentication/request/anonymous/anonymous.go#L32)).
>
> In this auth path, there is usually no `var _ authenticator.Request = &...{}` assertion, so constructor return types + runtime wiring are the reliable way to find implementers.

#### [3b] ServiceAccount JWT Verification Flow

```
Request header: Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
    │
    ▼
bearertoken.AuthenticateRequest()
    │ extracts the token from the header
    ▼
serviceaccount.Validator.AuthenticateToken()
    │
    ├─ Parse JWT (header.payload.signature)
    ├─ Verify signature (with the API server's public key)
    ├─ Check expiration time
    ├─ Confirm the ServiceAccount exists in the API server (when Lookup=true)
    └─ Return UserInfo:
       Name:   "system:serviceaccount:default:myapp"
       Groups: ["system:serviceaccounts", "system:serviceaccounts:default"]
```

#### [3c] X.509 Certificate Verification Flow

**File:** [staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go](../staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go#L138)

```go
// Lines 138-150: AuthenticateRequest()
func (a *Authenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {

    // Pass through if there is no TLS or no certificate
    if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
        return nil, false, nil
    }

    // Verify the certificate (CA chain)
    optsCopy, ok := a.verifyOptionsFn()
    chains, err := req.TLS.PeerCertificates[0].Verify(optsCopy)

    // CommonName → username, Organization → groups
    // e.g. CN=alice, O=system:masters
    return a.user.User(req.TLS.PeerCertificates[0], chains)
}
```

---

### [4] Authorization

**File:** [staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go#L55)

```go
// Lines 55-97: withAuthorization()
func withAuthorization(handler http.Handler, a authorizer.UnconditionalAuthorizer, ...) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

        // Line 64: extract request attributes
        attributes, err := GetAuthorizerAttributes(ctx)

        // Line 69: authorization decision
        authorized, reason, err := a.Authorize(ctx, attributes)

        // Line 79: Allow
        if authorized == authorizer.DecisionAllow {
            audit.AddAuditAnnotations(ctx, "authorization.k8s.io/decision", "allow", ...)
            handler.ServeHTTP(w, req)
            return
        }

        // Line 91: Forbidden
        audit.AddAuditAnnotations(ctx, "authorization.k8s.io/decision", "forbid", ...)
        responsewriters.Forbidden(attributes, w, req, reason, s)
    })
}
```

**Attribute extraction (lines 99-149):**
```go
func GetAuthorizerAttributes(ctx context.Context) (authorizer.Attributes, error) {
    attribs := authorizer.AttributesRecord{
        User:              user,           // authenticated user
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

#### [4a] RBAC Authorizer

**File:** [plugin/pkg/auth/authorizer/rbac/rbac.go](../plugin/pkg/auth/authorizer/rbac/rbac.go#L78)

```go
// Lines 78-130: Authorize()
func (r *RBACAuthorizer) Authorize(ctx context.Context, requestAttributes authorizer.Attributes) (authorizer.Decision, string, error) {

    // Line 79: rule visitor pattern
    ruleCheckingVisitor := &authorizingVisitor{requestAttributes: requestAttributes}

    r.authorizationRuleResolver.VisitRulesFor(ctx,
        requestAttributes.GetUser(),
        requestAttributes.GetNamespace(),
        ruleCheckingVisitor.visit)

    // Line 82: match succeeded
    if ruleCheckingVisitor.allowed {
        return authorizer.DecisionAllow, ruleCheckingVisitor.reason, nil
    }

    // Lines 100+: construct the failure reason
    // "user \"alice\" cannot create pods in namespace \"default\""
    return authorizer.DecisionNoOpinion, reason, nil
}
```

**Rule visiting (pkg/registry/rbac/validation/rule.go):**

```go
// VisitRulesFor() behavior:
1. Iterate over the ClusterRoleBinding list
   └─ If the user/group is included in a Subject
      → extract the ClusterRole's rules
      → call visit(nil, rule)

2. If a namespace is present, iterate over the RoleBinding list
   └─ If the user/group is included in a Subject
      → extract the Role/ClusterRole's rules
      → call visit(namespace, rule)
```

**Rule matching (lines 181-196):**
```go
// RuleAllows() — checks whether the request matches the rule
func RuleAllows(requestAttributes authorizer.Attributes, rule *rbacv1.PolicyRule) bool {
    if requestAttributes.IsResourceRequest() {
        combinedResource := resource + "/" + subresource  // e.g. "pods/log"
        return VerbMatches(rule, verb) &&        // "get" in ["get","list"]
            APIGroupMatches(rule, apiGroup) &&   // "" in [""]
            ResourceMatches(rule, resource, subresource) &&  // "pods" in ["pods","services"]
            ResourceNameMatches(rule, name)      // "" (all) or "my-pod"
    }
    // Non-resource URL request
    return VerbMatches(rule, verb) &&
        NonResourceURLMatches(rule, path)
}
```

**RBAC example:**

```yaml
# ClusterRole
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch"]

# Request: GET /api/v1/namespaces/default/pods/my-pod
# Verb: get, Resource: pods, APIGroup: "", Namespace: default, Name: my-pod
# → VerbMatches("get", ["get","list","watch"]) = true
# → APIGroupMatches("", [""]) = true
# → ResourceMatches("pods", ["pods"]) = true
# → ResourceNameMatches("my-pod", []) = true (empty array = allow all)
# → ALLOW
```

---

### [5] Admission Control

**File:** [staging/src/k8s.io/apiserver/pkg/admission/chain.go](../staging/src/k8s.io/apiserver/pkg/admission/chain.go#L31)

```go
// Lines 31-44: Admit() — Mutating
func (admissionHandler chainAdmissionHandler) Admit(ctx, a Attributes, o ObjectInterfaces) error {
    for _, handler := range admissionHandler {
        if !handler.Handles(a.GetOperation()) { continue }
        if mutator, ok := handler.(MutationInterface); ok {
            if err := mutator.Admit(ctx, a, o); err != nil {
                return err  // any failure → request rejected
            }
        }
    }
    return nil
}

// Lines 47-60: Validate() — Validating
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

> ⚠️ **`handler` is `admission.Interface`; find concrete plugin structs through registry wiring.**
> 1. Interface boundary: `type Interface interface` ([interfaces.go:123](../staging/src/k8s.io/apiserver/pkg/admission/interfaces.go#L123)).
> 2. Runtime checks in the loop: `handler.(MutationInterface)` and `handler.(ValidationInterface)` in [chain.go:35](../staging/src/k8s.io/apiserver/pkg/admission/chain.go#L35) and [chain.go:51](../staging/src/k8s.io/apiserver/pkg/admission/chain.go#L51).
> 3. Construction path: `NewFromPlugins(...)` builds `handlers []Interface` and classifies each plugin by those interfaces ([plugins.go:127](../staging/src/k8s.io/apiserver/pkg/admission/plugins.go#L127), [plugins.go:148](../staging/src/k8s.io/apiserver/pkg/admission/plugins.go#L148), [plugins.go:151](../staging/src/k8s.io/apiserver/pkg/admission/plugins.go#L151)).
> 4. Factory hop: each plugin `Register` callback returns `admission.Interface` from its concrete constructor, for example mutating webhook ([mutating/plugin.go:36](../staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/mutating/plugin.go#L36), [mutating/plugin.go:71](../staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/mutating/plugin.go#L71)) and validating webhook ([validating/plugin.go:36](../staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/validating/plugin.go#L36), [validating/plugin.go:71](../staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/validating/plugin.go#L71)).
> 5. Compile-time proof where present: `var _ admission.MutationInterface = &Plugin{}` and `var _ admission.ValidationInterface = &Plugin{}` ([mutating/plugin.go:52](../staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/mutating/plugin.go#L52), [validating/plugin.go:52](../staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/validating/plugin.go#L52)).
> 6. Final wiring into the generic loop: `newReinvocationHandler(chainAdmissionHandler(handlers))` ([plugins.go:162](../staging/src/k8s.io/apiserver/pkg/admission/plugins.go#L162)).

**Admission execution order:**
```
1. Mutating Admission Webhooks (parallel → serial processing)
   └─ May modify objects (defaulting, injection, etc.)
   └─ Examples: Istio sidecar injection, adding ServiceAccount volume mounts

2. Schema Validation (re-validates the modified object)

3. Validating Admission Webhooks (parallel processing)
   └─ Cannot modify; allow/deny only
   └─ Examples: OPA Gatekeeper, Kyverno

4. Built-in plugins:
   - ServiceAccount: mounts SA token secrets
   - LimitRanger: sets default resource requests/limits
   - ResourceQuota: checks namespace quota
   - PodSecurity: checks PSS policies
   - NamespaceLifecycle: prevents new resources in a namespace being deleted
```

#### [5a] Mutating Webhook Dispatcher — Inside the Webhook Loop

**File:** [staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/mutating/dispatcher.go](../staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/mutating/dispatcher.go#L108)

```go
// Lines 108-250+: Dispatch()
func (a *mutatingDispatcher) Dispatch(ctx, attr, o, hooks []webhook.WebhookAccessor) error {
    // Reinvocation context — tracks which webhooks have already run
    webhookReinvokeCtx := /* load or create */

    // If any in-tree plugin has mutated the object since the last webhook round,
    // mark all eligible webhooks for re-invocation.
    if reinvokeCtx.IsReinvoke() &&
        webhookReinvokeCtx.IsOutputChangedSinceLastWebhookInvocation(attr.GetObject()) {
        webhookReinvokeCtx.RequireReinvokingPreviouslyInvokedPlugins()
    }

    for i, hook := range hooks {
        // Skip if the webhook's rules/namespaceSelector don't match (line 130)
        invocation, _ := a.plugin.ShouldCallHook(ctx, hook, attr, o, v)
        if invocation == nil { continue }

        // Skip reinvocations for webhooks that did not opt in (line 143)
        if reinvokeCtx.IsReinvoke() &&
            !webhookReinvokeCtx.ShouldReinvokeWebhook(invocation.Webhook.GetUID()) {
            continue
        }

        // Convert object to the version the webhook expects (line 152)
        versionedAttr, _ := v.VersionedAttribute(invocation.Kind)

        // Make the HTTPS POST call to the webhook endpoint (line 163)
        changed, err := a.callAttrMutatingHook(ctx, hook, invocation, versionedAttr, ...)

        // Error handling depends on failurePolicy (lines 164-220)
        if err != nil {
            if *hook.FailurePolicy == admissionregistrationv1.Ignore {
                // fail-open: log the error but continue
                klog.Warningf("Failed calling webhook, failing open %v: %v", hook.Name, err)
                continue
            }
            // fail-closed (default): reject the request
            return apierrors.NewInternalError(err)
        }

        // If the webhook mutated the object, trigger reinvocation of all
        // prior webhooks that have ReinvocationPolicy=IfNeeded (line 224)
        if changed {
            webhookReinvokeCtx.RequireReinvokingPreviouslyInvokedPlugins()
            reinvokeCtx.SetShouldReinvoke()
        }
        if *hook.ReinvocationPolicy == admissionregistrationv1.IfNeededReinvocationPolicy {
            webhookReinvokeCtx.AddReinvocableWebhookToPreviouslyInvoked(uid)
        }
    }
    return nil
}
```

**Reinvocation model:**
```
Round 0: webhooks A → B → C  (A mutates object)
              └─ B sees A's mutation
              └─ C sees A+B mutations
              └─ C mutates object → SetShouldReinvoke()

Round 1 (reinvoke): webhooks A → B  (only IfNeeded ones, not C since C triggered it)
              └─ A re-runs because object changed
              └─ B re-runs because object changed
```

> ⚠️ **Reinvocation is bounded.** The framework limits rounds to prevent infinite loops. If the object is still mutating after the allowed reinvocation passes, the admission chain aborts with an error.

**`failurePolicy` semantics:**

| Value | Webhook unreachable or returns 5xx | Webhook returns 4xx/reject |
|-------|-----------------------------------|--------------------------|
| `Fail` (default) | Request rejected (fail-closed) | Request rejected |
| `Ignore` | Request continues (fail-open) | Request rejected |

**Admission attributes:**

**File:** [staging/src/k8s.io/apiserver/pkg/admission/interfaces.go](../staging/src/k8s.io/apiserver/pkg/admission/interfaces.go#L29)

```go
type Attributes interface {
    GetName() string
    GetNamespace() string
    GetResource() schema.GroupVersionResource
    GetSubresource() string
    GetOperation() Operation           // CREATE, UPDATE, DELETE, CONNECT
    GetObject() runtime.Object        // request object (modifiable - Mutating)
    GetOldObject() runtime.Object     // existing object (UPDATE/DELETE)
    GetUserInfo() user.Info
    IsDryRun() bool
    GetKind() schema.GroupVersionKind
}
```

---

### Impersonation

**File:** [staging/src/k8s.io/apiserver/pkg/server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go#L1056)

```go
// Lines 1056-1060: WithImpersonation()
// Handles the Impersonate-User and Impersonate-Group headers
// Authorization condition: the requester can act as another user if they hold impersonation RBAC permissions
```

```bash
# Example: admin impersonates alice for a request
kubectl get pods --as=alice --as-group=developers
# → Headers: Impersonate-User: alice, Impersonate-Group: developers
```

---

## Error Handling Summary

| Stage | Error | HTTP Code |
|------|------|---------|
| Authentication failure (no token) | - | 401 Unauthorized |
| Authentication failure (invalid token) | - | 401 Unauthorized |
| Authorization failure | "user X cannot Y Z" | 403 Forbidden |
| Admission validation failure | "field X: value Y is invalid" | 422 Unprocessable Entity |
| Admission policy denial | Webhook response message | 403 Forbidden |
| Admission internal error | - | 500 Internal Server Error |

---

## Audit Log

Authentication/authorization results are automatically recorded in the Audit Log:

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

## Key File Path Summary

| Stage | File | Key Function | Line |
|------|------|----------|------|
| Filter chain | [staging/.../server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go) | `DefaultBuildHandlerChain` | 1036 |
| Authentication filter | [staging/.../filters/authentication.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go) | `WithAuthentication` | 46 |
| Authenticator configuration | [pkg/kubeapiserver/authenticator/config.go](../pkg/kubeapiserver/authenticator/config.go) | `Config.New` | 107 |
| X.509 authentication | [staging/.../authentication/request/x509/x509.go](../staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go) | `AuthenticateRequest` | 138 |
| Bearer token | [staging/.../authentication/request/bearertoken/bearertoken.go](../staging/src/k8s.io/apiserver/pkg/authentication/request/bearertoken/bearertoken.go) | `AuthenticateRequest` | 42 |
| Authorization filter | [staging/.../filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go) | `WithAuthorization` | 51 |
| Attribute extraction | [staging/.../filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go) | `GetAuthorizerAttributes` | 99 |
| RBAC authorization | [plugin/pkg/auth/authorizer/rbac/rbac.go](../plugin/pkg/auth/authorizer/rbac/rbac.go) | `Authorize` | 78 |
| Rule matching | [plugin/pkg/auth/authorizer/rbac/rbac.go](../plugin/pkg/auth/authorizer/rbac/rbac.go) | `RuleAllows` | 181 |
| Rule visiting | [pkg/registry/rbac/validation/rule.go](../pkg/registry/rbac/validation/rule.go) | `VisitRulesFor` | 179 |
| Admission chain | [staging/.../admission/chain.go](../staging/src/k8s.io/apiserver/pkg/admission/chain.go) | `Admit`, `Validate` | 31, 47 |

---

## Related Concepts

- **Three distinct gates: AuthN → AuthZ → Admission.** Authentication asks "*who are you?*" (→ 401), authorization asks "*are you allowed this verb on this resource?*" (→ 403), and admission asks "*is this specific object acceptable, and should it be mutated?*" (→ 4xx). Different questions, different failure codes.
- **Authenticator union.** The server tries each method in order (client cert, bearer token, …) until one succeeds; any single success authenticates the request. That ordered "first to succeed wins" chain is the `union` authenticator.
- **The RBAC model.** Roles/ClusterRoles hold *rules* (verbs × resources); RoleBindings/ClusterRoleBindings attach those rules to *subjects* (users, groups, ServiceAccounts). RBAC is **additive and deny-by-default** — there are no deny rules, only the absence of an allow.
- **ServiceAccount tokens.** Pods receive projected, **audience-bound, expiring** JWTs; the API server verifies the signature with its keys and can confirm the SA still exists. This is how in-cluster controllers authenticate (ties back to Scenario 3).
- **Mutating vs. validating admission.** Mutating plugins/webhooks may *change* the object (defaulting, sidecar injection); validating ones may only *accept or reject*. Order is fixed: mutate → re-validate schema → validate, so a later validator always sees the mutated object.
- **Impersonation.** With the right RBAC, a caller can act as another user/group via `Impersonate-*` headers (`kubectl --as`) — useful for delegated access and `kubectl auth can-i --as` debugging.
- **Audit.** Every authn/authz decision is annotated and written to the audit log — the authoritative "who did what, and was it allowed" trail.

> ⚠️ **Authentication never decides permissions.** A perfectly valid token still gets a 403 if no RBAC rule allows the verb. "Logged in" and "allowed" are separate stages — don't debug an authz 403 by re-checking the token.

---

## Related Scenarios

- [Scenario 1: API Request Flow](01-api-request-flow.md) — processing flow after authentication/authorization
- [Scenario 3: Deployment Rolling Update](03-deployment-rolling-update.md) — how controllers authenticate when calling the API (ServiceAccount)
