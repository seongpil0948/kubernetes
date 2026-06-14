# Scenario 1: API Request Flow (kubectl → kube-apiserver → etcd)

Traces the full path from running `kubectl create -f pod.yaml` to persistence in etcd.

## Big Picture

This flow is a layered pipeline: `kubectl` builds and sends an HTTP request, kube-apiserver applies cross-cutting filters (authn/authz/admission), then a resource-specific REST storage implementation persists the object through the generic registry into etcd. If you keep this three-stage model in mind (client -> server filters -> storage), every call hop in the trace becomes easier to place.

## Interface Resolution Guide (This Scenario)

When you hit an interface boundary in this document, use this order:
1. Find the interface type definition.
2. Look for `var _ Interface = &Struct{}` compile-time assertions.
3. If no assertion exists, follow constructor return types and assignment wiring.
4. Confirm runtime selection points (maps, registries, or switch branches) that choose the concrete value.

### Worked Example: `rest.Creater` for `pods`

One real trace in this scenario is the POST `/api/v1/namespaces/default/pods` storage path:

1. Interface: `type Creater interface` in [../staging/src/k8s.io/apiserver/pkg/registry/rest/rest.go](../staging/src/k8s.io/apiserver/pkg/registry/rest/rest.go#L202).
2. Concrete behavior: pod storage's `REST` struct embeds `*genericregistry.Store` in [../pkg/registry/core/pod/storage/storage.go](../pkg/registry/core/pod/storage/storage.go#L70), and `Store` provides `New()` and `Create()` in [../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go](../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go#L371) and [../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go](../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go#L514).
3. Factory wiring: `podstore.NewStorage(...)` builds the store and returns `PodStorage{Pod: &REST{store, ...}}` in [../pkg/registry/core/pod/storage/storage.go](../pkg/registry/core/pod/storage/storage.go#L76).
4. Registration wiring: core API installation puts that exact value into `storage["pods"] = podStorage.Pod` in [../pkg/registry/core/rest/storage_core.go](../pkg/registry/core/rest/storage_core.go#L240).
5. Runtime selection: the endpoint installer type-asserts `storage.(rest.Creater)` in [../staging/src/k8s.io/apiserver/pkg/endpoints/installer.go](../staging/src/k8s.io/apiserver/pkg/endpoints/installer.go#L337), and for POST uses `restfulCreateResource(creater, ...)` in [../staging/src/k8s.io/apiserver/pkg/endpoints/installer.go](../staging/src/k8s.io/apiserver/pkg/endpoints/installer.go#L925).

This path has no single `var _ rest.Creater = ...` breadcrumb. The reliable proof is the combination of embedded methods, storage-map registration, and the installer's runtime type assertion.

## Reading Guide (Beginner)

- **Trigger:** a client command (`kubectl create/apply`) emits one HTTP request.
- **First durable state change:** nothing is "real" until the storage layer writes to etcd.
- **Security gates run before business logic:** authentication/authorization/admission can reject before the resource handler is reached.
- **Success criterion for this scenario:** object bytes are persisted under `/registry/...` and visible via GET/list/watch.
- **Most common confusion:** HTTP `201 Created` means persistence success, not workload readiness.

## Overall Flow Diagram

```
kubectl create -f pod.yaml
        │
[1] cmd/kubectl/kubectl.go:main()
        │
[2] staging/.../kubectl/pkg/cmd/cmd.go:NewDefaultKubectlCommand()
        │ Create cobra Command + load kubeconfig
        │
[3] staging/.../client-go/rest/request.go:NewRequest()
        │ Build the HTTP request object
        │
[4] staging/.../client-go/rest/request.go:request()
        │ rate limit → retry → HTTP send
        │
        │ HTTPS POST /api/v1/namespaces/default/pods
        ▼
[5] cmd/kube-apiserver/app/server.go:Run()
        │ CreateServerChain()
        │
[6] staging/.../apiserver/pkg/server/config.go:DefaultBuildHandlerChain()
        │ Filter chain (authentication → authorization → admission → handler)
        │
[7] staging/.../apiserver/pkg/endpoints/handlers/create.go:createHandler()
        │ Deserialization → Admission → Storage.Create()
        │
[8] staging/.../apiserver/pkg/registry/generic/registry/store.go:Store.Create()
        │ BeforeCreate → Validation → etcd key generation
        │
[9] staging/.../apiserver/pkg/storage/etcd3/store.go:store.Create()
        │ Codec serialization → encryption → etcd PUT
        │
        ▼
     etcd: /registry/pods/default/my-pod = {protobuf}
```

---

## Step-by-Step Analysis

### [1] kubectl Entry Point

**File:** [cmd/kubectl/kubectl.go](../cmd/kubectl/kubectl.go#L31)

```go
// Lines 31-44
func main() {
    logs.GlogSetter(cmd.GetLogVerbosity(os.Args))
    command := cmd.NewDefaultKubectlCommand()
    if err := cli.RunNoErrOutput(command); err != nil {
        util.CheckErr(err)
    }
}
```

### [2] Cobra Command Construction

**File:** [staging/src/k8s.io/kubectl/pkg/cmd/cmd.go](../staging/src/k8s.io/kubectl/pkg/cmd/cmd.go#L96)

```go
// Lines 96-104
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

**Key responsibilities:**
- `ConfigFlags`: loads credentials from `~/.kube/config`
- `PluginHandler`: handles `kubectl-*` plugin binaries

### [3] HTTP Request Object Creation

**File:** [staging/src/k8s.io/client-go/rest/request.go](../staging/src/k8s.io/client-go/rest/request.go#L133)

```go
// Lines 133-176: NewRequest()
r := &Request{
    c:           c,
    rateLimiter: c.rateLimiter,   // request rate limiting
    timeout:     timeout,
    pathPrefix:  path.Join("/", c.base.Path, c.versionedAPIPath),
    maxRetries:  10,
    retryFn:     defaultRequestRetryFn,
}
```

**Example URL construction:**
```
https://192.168.0.1:6443/api/v1/namespaces/default/pods
```

### [4] HTTP Send Logic

**File:** [staging/src/k8s.io/client-go/rest/request.go](../staging/src/k8s.io/client-go/rest/request.go#L1030)

```go
// Lines 1030-1121: request()
func (r *Request) request(ctx context.Context, fn func(*http.Request, *http.Response)) error {
    // 1. Rate limiting
    if err := r.tryThrottle(ctx); err != nil { return err }

    // 2. Set timeout
    if r.timeout > 0 {
        ctx, cancel = context.WithTimeout(ctx, r.timeout)
    }

    // 3. Retry loop
    retry := r.retryFn(r.maxRetries)
    for {
        req, err := r.newHTTPRequest(ctx)  // Line 980: create the HTTP request
        resp, err := client.Do(req)         // Line 1088: actual send
        if retry.IsNextRetry(...) { continue }
        fn(req, resp)
        return nil
    }
}
```

**Data transformation:**
```
pod.yaml (YAML)
    → JSON (kubectl sends with Content-Type: application/json)
    → HTTP Body
```

---

### [5] kube-apiserver Startup and Server Chain

**File:** [cmd/kube-apiserver/app/server.go](../cmd/kube-apiserver/app/server.go#L148)

```go
// Lines 148-173: Run()
func Run(ctx context.Context, opts options.CompletedOptions) error {
    config, err := NewConfig(opts)
    completed, err := config.Complete()

    // Line 162: create the 3-tier server chain
    server, err := CreateServerChain(completed)
    //  └─ APIExtensionsServer (handles CRDs)
    //      └─ KubeAPIServer (core APIs: Pod, Service, Node, etc.)
    //          └─ AggregatorServer (integrates external API servers)

    prepared, err := server.PrepareRun()
    return prepared.Run(ctx)  // start the HTTPS listener
}
```

### [6] Handler Filter Chain

**File:** [staging/src/k8s.io/apiserver/pkg/server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go#L1036)

```
DefaultBuildHandlerChain() lines 1036-1116 — chain application order (inner→outer):

APIHandler (actual processing)
    ← WithAuthorization          (line 1040) ← RBAC permission check
    ← WithImpersonation          (line 1056) ← user impersonation
    ← WithAudit                  (line 1064) ← audit logging
    ← WithAuthentication         (line 1075) ← authentication
    ← WithCORS                   (line 1078)
    ← WithTimeoutForNonLongRunning (line 1086)
    ← WithRequestDeadline        (line 1088)
    ← WithWaitGroup              (line 1090)
    ← WithPriorityAndFairness    (line 1051) ← request priority/fairness
    ← WithHTTPLogging            (line 1102)
    ← WithRequestInfo            (line 1110) ← request metadata extraction
    ← WithPanicRecovery          (line 1113)
    ← WithAuditInit              (line 1114)
```

**Authentication filter:**

**File:** [staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go#L46)

```go
// Lines 46-125: WithAuthentication()
resp, ok, err := auth.AuthenticateRequest(req)  // Line 67: run the authentication chain

// On successful authentication
req.Header.Del("Authorization")                  // Line 89: remove the token
req = req.WithContext(
    genericapirequest.WithUser(req.Context(), resp.User))  // Line 122: inject user info
```

**Authorization filter:**

**File:** [staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go#L55)

```go
// Lines 55-97: withAuthorization()
attributes, err := GetAuthorizerAttributes(ctx)        // Line 64: extract request attributes
authorized, reason, err := a.Authorize(ctx, attributes) // Line 69: permission check

if authorized == authorizer.DecisionAllow {
    handler.ServeHTTP(w, req)  // Line 82: allowed → next handler
    return
}
responsewriters.Forbidden(...)  // Line 91: 403 Forbidden
```

### [7] Create Request Handler

**File:** [staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go](../staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go#L53)

```go
// Lines 53-250+: createHandler()
func createHandler(r rest.NamedCreater, scope *RequestScope, admit admission.Interface, ...) http.HandlerFunc {
    return func(w http.ResponseWriter, req *http.Request) {

        // 1. Negotiate input media type (line 88)
        s, err := negotiation.NegotiateInputSerializer(req, false, scope.Serializer)

        // 2. Read the request body (line 94)
        body, err := limitedReadBodyWithRecordMetric(ctx, req, scope.MaxRequestBodyBytes, ...)

        // 3. Deserialize: body → runtime.Object (line 129)
        obj, gvk, err := decoder.Decode(body, &defaultGVK, original)

        // 4. Audit logging (line 160)
        audit.LogRequestObject(req.Context(), obj, objGV, scope.Resource, ...)

        // 5. Create admission attributes (line 182)
        admissionAttributes := admission.NewAttributesRecord(
            obj, nil, scope.Kind, namespace, name,
            scope.Resource, scope.Subresource, admission.Create, options, ...)

        // 6. Persist to storage (line 183)
        requestFunc := func() (runtime.Object, error) {
            return r.Create(ctx, name, obj,
                rest.AdmissionToValidateObjectFunc(admit, admissionAttributes, scope),
                options)
        }

        // 7. Managed Fields + Admission (line 194)
        result, err := finisher.FinishRequest(ctx, func() (runtime.Object, error) {
            obj = scope.FieldManager.UpdateNoErrors(liveObj, obj, ...)
            return requestFunc()
        })
    }
}
```

> ⚠️ **Where does `r.Create` actually go?** The handler receives `r rest.NamedCreater` ([rest.go:212](../staging/src/k8s.io/apiserver/pkg/registry/rest/rest.go#L212)) — an interface, so the concrete storage is invisible here. Trace it: most resources are backed by the generic `genericregistry.Store` ([store.go:101](../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go#L101)), proven to satisfy the REST interfaces by `var _ rest.StandardStorage = &Store{}` ([store.go:253](../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go#L253)). Each resource wraps it in a thin `*REST` — e.g. the Pod `REST` embeds `*genericregistry.Store` ([pkg/registry/core/pod/storage/storage.go:70](../pkg/registry/core/pod/storage/storage.go#L70)). That `*REST` is registered into the per-resource storage map at route-build time, so `r.Create(...)` dispatches to `(*Store).Create` → `store.create()` (Step [8]).

**Admission execution order:**
```
Mutating Webhooks (may modify the object)
    → Validating Webhooks (validation only)
    → Built-in plugins: ServiceAccount, LimitRanger, ResourceQuota, ...
```

### [8] Storage Layer — Store.Create()

**File:** [staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go](../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go#L454)

```go
// Lines 485-566: store.create()
func (e *Store) create(ctx context.Context, obj runtime.Object, ...) (runtime.Object, error) {

    // 1. Initialize metadata (line 489)
    rest.FillObjectMetaSystemFields(objectMeta)  // set UID, CreationTimestamp

    // 2. Handle GenerateName (line 494)
    objectMeta.SetName(e.CreateStrategy.GenerateName(objectMeta.GetGenerateName()))

    // 3. BeforeCreate hook (line 509)
    rest.BeforeCreate(e.CreateStrategy, ctx, obj)

    // 4. Run validation (line 514)
    createValidation(ctx, obj.DeepCopyObject())

    // 5. Generate the etcd key (line 524)
    key, err := e.KeyFunc(ctx, name)
    // e.g.: "/registry/pods/default/my-pod"

    // 6. Persist to etcd (line 534) ← the crux
    e.Storage.Create(ctx, key, obj, out, ttl, dryrun.IsDryRun(options.DryRun))
}
```

### [9] Direct etcd3 Persistence

**File:** [staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go](../staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L269)

```go
// Lines 269-334: store.Create()
func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {

    // 1. Protobuf serialization (line 289)
    data, err := runtime.Encode(s.codec, obj)

    // 2. encryption-at-rest encryption (line 304)
    newData, err := s.transformer.TransformToStorage(ctx, data, authenticatedDataString(preparedKey))

    // 3. Atomic etcd PUT (line 311) ← final persistence
    txnResp, err := s.client.Kubernetes.OptimisticPut(
        ctx, preparedKey, newData, 0, kubernetes.PutOptions{LeaseID: lease})

    // 4. Decode the stored data and return it (line 324)
    s.decoder.Decode(data, out, txnResp.Revision)
}
```

**Full data transformation flow:**
```
YAML file
  → JSON (kubectl serialization)
  → runtime.Object (apiserver deserialization)
  → Admission (may modify)
  → Protobuf (codec for etcd storage)
  → Encryption (encryption-at-rest, optional)
  → etcd persistence
```

---

## Key File Path Summary

| Step | File | Key Function | Line |
|------|------|----------|------|
| 1. CLI entry | [cmd/kubectl/kubectl.go](../cmd/kubectl/kubectl.go) | `main` | 31 |
| 2. Command construction | [staging/.../kubectl/pkg/cmd/cmd.go](../staging/src/k8s.io/kubectl/pkg/cmd/cmd.go) | `NewDefaultKubectlCommand` | 96 |
| 3. Request object | [staging/.../client-go/rest/request.go](../staging/src/k8s.io/client-go/rest/request.go) | `NewRequest` | 133 |
| 4. HTTP send | [staging/.../client-go/rest/request.go](../staging/src/k8s.io/client-go/rest/request.go) | `request` | 1030 |
| 5. Server startup | [cmd/kube-apiserver/app/server.go](../cmd/kube-apiserver/app/server.go) | `Run` | 148 |
| 6. Filter chain | [staging/.../apiserver/pkg/server/config.go](../staging/src/k8s.io/apiserver/pkg/server/config.go) | `DefaultBuildHandlerChain` | 1036 |
| 6a. Authentication | [staging/.../filters/authentication.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go) | `WithAuthentication` | 46 |
| 6b. Authorization | [staging/.../filters/authorization.go](../staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go) | `WithAuthorization` | 51 |
| 7. CREATE handler | [staging/.../handlers/create.go](../staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go) | `createHandler` | 53 |
| 8. Store | [staging/.../registry/store.go](../staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go) | `Store.Create` | 454 |
| 9. etcd | [staging/.../storage/etcd3/store.go](../staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go) | `store.Create` | 269 |

---

## Related Concepts

Background ideas this scenario assumes. Skim them if a step felt like it skipped a "why".

- **Declarative API & desired state.** A Kubernetes object records *desired* state in `spec`; controllers later reconcile *actual* state (`status`) toward it. A successful create therefore just durably records intent — no Pod runs yet.
- **Resources vs. kinds (GVR vs. GVK).** The URL path maps to a Group/Version/**Resource** (`apps/v1/deployments`), while the body carries a Group/Version/**Kind** (`apps/v1`, `Deployment`). The RESTMapper converts between them — which is why both `pods` (resource) and `Pod` (kind) exist.
- **Internal vs. external versions & conversion.** Incoming bytes decode into a versioned *external* type, convert to a single *internal* type for processing, then convert back out on read. This is what lets one server serve `v1beta1` and `v1` of the same object.
- **Content negotiation & codecs.** Clients send JSON/YAML and pick a response format via `Accept`; etcd stores Protobuf. The codec factory drives encode/decode at every boundary.
- **Optimistic concurrency (`resourceVersion`).** Every write carries the version it read; a stale write fails with HTTP 409 Conflict instead of silently overwriting. This is the foundation of `kubectl apply` and controller retry loops.
- **Encryption at rest (transformers).** An optional transformer encrypts the serialized object just before the etcd write and decrypts on read, configured via `EncryptionConfiguration` (aescbc/KMS).
- **Watch & informers.** After this scenario stores the object, schedulers, controllers, and kubelets learn about it through a list+watch stream keyed on `resourceVersion` — the mechanism every later scenario depends on.

> ⚠️ **A `201 Created` does not mean anything is running.** This flow ends at "bytes durably in etcd." The Pod/Deployment only becomes real when the matching controller (Scenario 2/3) and kubelet (Scenario 4) observe it via watch and act.

---

## Related Scenarios

- [Scenario 2: Pod Scheduling](02-pod-scheduling.md) — how the scheduler detects the persisted Pod
- [Scenario 6: Authentication/Authorization Details](06-auth-flow.md) — detailed analysis of the authentication/authorization chain
