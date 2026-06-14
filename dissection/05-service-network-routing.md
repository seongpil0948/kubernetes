# Scenario 5: Service Creation and Network Traffic Routing

Traces the flow from Service creation through kube-proxy building iptables rules until Pod-to-Pod communication is established.

## Big Picture

Service routing is split between control-plane object convergence and node-local packet programming. Controllers convert Service selectors into endpoint objects, while kube-proxy watches those objects and incrementally materializes deterministic iptables state. Data-plane packets then follow kernel NAT rules, not Go code, at request time.

## Interface Resolution Guide (This Scenario)

The main interface boundary is proxier selection. Use this order:
1. Start from `proxy.Provider`.
2. Enumerate concrete implementations via compile-time assertions.
3. Follow `createProxier` mode-based selection.
4. Verify concrete method execution in the selected proxier (`syncProxyRules`).

### Worked Example: `proxy.Provider` -> mode-specific `*Proxier`

This is the exact wiring that decides whether kube-proxy uses iptables, IPVS, or nftables:

1. Interface: `type Provider interface` in [../pkg/proxy/types.go](../pkg/proxy/types.go#L28).
2. Compile-time proofs: `var _ proxy.Provider = &Proxier{}` in [../pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L206), [../pkg/proxy/ipvs/proxier.go](../pkg/proxy/ipvs/proxier.go#L239), and [../pkg/proxy/nftables/proxier.go](../pkg/proxy/nftables/proxier.go#L199).
3. Factory functions: each mode exposes `NewProxier(...)` in [../pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L209), [../pkg/proxy/ipvs/proxier.go](../pkg/proxy/ipvs/proxier.go#L242), and [../pkg/proxy/nftables/proxier.go](../pkg/proxy/nftables/proxier.go#L202).
4. Runtime selection wiring: `createProxier(...) (proxy.Provider, error)` switches on `config.Mode` and assigns one concrete proxier in [../cmd/kube-proxy/app/server_linux.go](../cmd/kube-proxy/app/server_linux.go#L129).
5. Runtime use: after startup, the rest of kube-proxy only holds the `proxy.Provider` interface, but methods like `syncProxyRules` run on whichever concrete proxier `createProxier` selected.

When you need the real implementation, the `config.Mode` branch is more important than the interface itself.

## Reading Guide (Beginner)

- **Trigger:** Service/EndpointSlice informer changes enqueue kube-proxy sync work.
- **Control-plane vs data-plane:** Go controllers/proxy code compute rules; the Linux kernel executes packet forwarding.
- **Eventual consistency:** endpoint changes are not instantaneous(동시에 일어나는); they appear after the next sync pass applies rules.
- **Success criterion for this scenario:** ClusterIP/NodePort traffic is DNATed(traffic sent to a Service VIP like 10.0.0.1:80 gets DNATed to a real Pod endpoint like 10.244.0.3:8080) to a healthy backend Pod.
- **Most common confusion:** kube-proxy is a rule programmer(writes kernel networking rules (iptables, IPVS, or nftables).), not a per-request userspace forwarder.(kube-proxy is like a compiler that generates networking rules; Linux kernel is the runtime that executes them for every packet.)


## Overall Flow

```
kubectl apply -f service.yaml
        │
[1] API server: store Service
        │ (Watch event)
        │
        ├──────────────────────────────────────────────┐
        ▼                                              ▼
[2a] pkg/controller/endpoint/                   [2b] pkg/controller/endpointslice/
     endpoints_controller.go                          endpointslice_controller.go
     syncService()                                    syncService()
        │                                              │
        │ Match Pods via Service selector              │ Slices of max 100 each
        ▼                                              ▼
     Create/update Endpoints object           Create/update EndpointSlice object
        │                                              │
        └───────────────────┬──────────────────────────┘
                            │ (Watch event)
                            ▼
[3] kube-proxy on each node
    pkg/proxy/iptables/proxier.go
        │
        ├─ OnServiceUpdate() → accumulate serviceChanges
        ├─ OnEndpointSliceUpdate() → refresh endpointSliceCache
        └─ Sync() → syncRunner.Run()
                        │
                        ▼
[4] syncProxyRules()
        │
        ├─ serviceChanges/endpointChanges → update svcPortMap, endpointsMap
        ├─ Generate per-service iptables chains/rules (in-memory buffer)
        └─ Apply atomically via iptables-restore
        │
        ▼
[5] Kernel iptables
    Packet: client → ClusterIP → DNAT → Pod IP
```

---

## Step-by-Step Analysis

### [2a] Endpoint Controller (Legacy)

**File:** [pkg/controller/endpoint/endpoints_controller.go](../pkg/controller/endpoint/endpoints_controller.go#L79)

```go
// Lines 79-129: NewEndpointController() — registers 3 informers
func NewEndpointController(ctx, podInformer, serviceInformer, endpointsInformer, ...) *Controller {
    e.queue = workqueue.NewTypedRateLimitingQueue(...)

    // Service change → enqueue immediately
    serviceInformer.Informer().AddEventHandler(...)
    // Pod change → enqueue related Services (selector matching)
    podInformer.Informer().AddEventHandler(...)
}
```

**Key function: syncService() (lines 334-542)**

```go
func (e *Controller) syncService(ctx, key) error {

    // 1. Look up the Service (line 345)
    service, err := e.serviceLister.Services(namespace).Get(name)

    // 2. List Pods using the Service's selector (line 378)
    pods, err := e.podLister.Pods(service.Namespace).List(
        labels.Set(service.Spec.Selector).AsSelectorPreValidated())

    // 3. Convert each Pod to an EndpointAddress (lines 390-430)
    for _, pod := range pods {
        if !podutil.IsPodReady(pod) {
            // Pods that are not ready go into NotReadyAddresses
        }
        epAddress := podToEndpointAddressForService(svc, pod)
        // Set Pod IP, port, TargetRef (Pod reference)
    }

    // 4. Compare with existing Endpoints and update on change (lines 503-509)
    if createEndpoints {
        e.client.CoreV1().Endpoints(namespace).Create(ctx, newEndpoints, ...)
    } else if endpointsChanged(currentEndpoints, newEndpoints) {
        e.client.CoreV1().Endpoints(namespace).Update(ctx, newEndpoints, ...)
    }
}
```

---

### [2b] EndpointSlice Controller (Modern, Recommended)

**File:** [pkg/controller/endpointslice/endpointslice_controller.go](../pkg/controller/endpointslice/endpointslice_controller.go#L84)

```go
// Lines 84-190: NewController() — registers 4 informers
func NewController(ctx, podInformer, serviceInformer, nodeInformer, endpointSliceInformer, ...) *Controller {

    c.reconciler = endpointslicerec.NewReconciler(
        c.client,
        c.nodeLister,
        c.maxEndpointsPerSlice,  // default 100
        c.endpointSliceTracker,
        c.topologyCache,         // node topology (zone info)
        c.eventRecorder,
    )
}
```

**EndpointSlice characteristics:**
- Split into chunks of at most(최대) 100, unlike Endpoints → better performance in large clusters
- Includes node topology information (zone-based traffic optimization)
- Partial updates on change instead of rewriting the whole object

---

### [3] kube-proxy Initialization

**File:** [cmd/kube-proxy/app/server.go](../cmd/kube-proxy/app/server.go#L183)

```go
// Lines 183-293: newProxyServer()
func newProxyServer(ctx, config, ...) (*ProxyServer, error) {
    s := &ProxyServer{
        Config:      config,
        Client:      createClient(...),
        NodeManager: proxy.NewNodeManager(...),
        Proxier:     s.createProxier(ctx, config, ...),  // create iptables/ipvs proxier
    }
}
```

**File:** [cmd/kube-proxy/app/server_linux.go](../cmd/kube-proxy/app/server_linux.go#L129)

```go
// Lines 129-181: createProxier()
func (s *ProxyServer) createProxier(ctx, config, ...) (proxy.Provider, error) {
    if config.Mode == proxyconfigapi.ProxyModeIPTables {
        return iptables.NewProxier(
            ctx, s.PrimaryIPFamily, ipts[s.PrimaryIPFamily],
            config.SyncPeriod.Duration,     // full resync period (default 30s)
            config.MinSyncPeriod.Duration,  // minimum sync interval
            config.Linux.MasqueradeAll,
            localDetectors[s.PrimaryIPFamily],
            s.NodeName, s.NodeIPs[s.PrimaryIPFamily], ...)
    }
}
```

> ⚠️ **`createProxier` returns the `proxy.Provider` interface — the concrete type depends on the mode.** `proxy.Provider` is defined at [pkg/proxy/types.go:28](../pkg/proxy/types.go#L28). Each proxy mode ships its own concrete `Proxier` struct, each pinned with a compile-time assertion:
> - iptables: `Proxier` ([proxier.go:127](../pkg/proxy/iptables/proxier.go#L127)) — `var _ proxy.Provider = &Proxier{}` ([proxier.go:206](../pkg/proxy/iptables/proxier.go#L206))
> - ipvs: `Proxier` ([proxier.go:148](../pkg/proxy/ipvs/proxier.go#L148)) — assertion at [proxier.go:239](../pkg/proxy/ipvs/proxier.go#L239)
> - nftables: `Proxier` ([proxier.go:133](../pkg/proxy/nftables/proxier.go#L133)) — assertion at [proxier.go:199](../pkg/proxy/nftables/proxier.go#L199)
>
> The `var _ proxy.Provider = &Proxier{}` lines are the fastest way to enumerate every implementation; `createProxier` picks one by `config.Mode` at startup, so the rest of kube-proxy only ever sees the interface.

**File:** [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L214)

```go
// Lines 214-324: NewProxier()
func NewProxier(ctx, ipFamily, ipt, syncPeriod, minSyncPeriod, ...) (*Proxier, error) {
    proxier := &Proxier{
        svcPortMap:       make(proxy.ServicePortMap),
        endpointsMap:     make(proxy.EndpointsMap),
        serviceChanges:   proxy.NewServiceChangeTracker(ipFamily, newServiceInfo, nil),
        endpointsChanges: proxy.NewEndpointsChangeTracker(ipFamily, nodeName, newEndpointInfo, nil),

        // BoundedFrequencyRunner: runs syncProxyRules between the min and max intervals
        syncRunner: runner.NewBoundedFrequencyRunner(
            "sync-runner", proxier.syncProxyRules, minSyncPeriod, syncPeriod, proxyutil.FullSyncPeriod),
    }

    // Detect external modifications to iptables (line 314)
    go ipt.Monitor(kubeProxyCanaryChain,
        []utiliptables.Table{TableMangle, TableNAT, TableFilter},
        proxier.forceSyncProxyRules,  // force a full sync when detected
        syncPeriod, wait.NeverStop)
}
```

---

### [3] Watch Callbacks

**File:** [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L459)

```go
// Lines 459-498: change-detection callbacks

// Service change
func (proxier *Proxier) OnServiceUpdate(oldService, service *v1.Service) {
    if proxier.serviceChanges.Update(oldService, service) && proxier.isInitialized() {
        proxier.Sync()  // syncRunner.Run() → run syncProxyRules as soon as possible
    }
}

// EndpointSlice change
func (proxier *Proxier) OnEndpointSliceAdd(endpointSlice *discovery.EndpointSlice) {
    if proxier.endpointsChanges.EndpointSliceUpdate(endpointSlice, false) && proxier.isInitialized() {
        proxier.Sync()
    }
}
```

---

### [4] syncProxyRules() — Core of iptables Rule Generation

**File:** [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L638)

`syncProxyRules()` is the function that actually writes Service routing into the
Linux kernel. Everything before this step just *collected* information (which
Services exist, which Pods back them). This step *translates* that information
into iptables rules. Before reading the code, you need three mental models.

**Mental model 1 — What problem is being solved?**
A Service has a *virtual* IP (the ClusterIP, e.g. `10.96.0.10`) that no network
card actually owns. When a Pod sends a packet to `10.96.0.10:80`, something has
to rewrite the destination to a *real* Pod IP (e.g. `10.244.1.5:8080`). That
rewrite is called **DNAT** (Destination NAT). kube-proxy's entire job is to
install the DNAT rules that make ClusterIPs work.

**Mental model 2 — iptables vocabulary (the absolute minimum).**

| Term | What it means here |
| --- | --- |
| **Table** | A category of rules. kube-proxy mainly uses `nat` (for DNAT/SNAT) and `filter` (for REJECT/DROP). |
| **Chain** | An ordered list of rules. Linux has built-in chains (`PREROUTING`, `OUTPUT`, `POSTROUTING`); kube-proxy adds its own custom chains, all prefixed `KUBE-` (e.g. `KUBE-SERVICES`, `KUBE-SVC-XXXX`, `KUBE-SEP-XXXX`). |
| **Rule** | One line: "if the packet matches *these* conditions, do *this* action." |
| **Jump (`-j`)** | The action "go evaluate that other chain." This is how kube-proxy builds a decision tree: `KUBE-SERVICES` → `KUBE-SVC-<service>` → `KUBE-SEP-<endpoint>`. |
| **DNAT** | Rewrite the destination IP\:port. This is what turns a ClusterIP into a Pod IP. |

So the structure kube-proxy builds is a two-level tree: a top-level
`KUBE-SERVICES` chain matches on *which Service* (by ClusterIP+port), jumps to a
per-Service chain (`KUBE-SVC-*`) that picks *which backend Pod*, which jumps to a
per-endpoint chain (`KUBE-SEP-*`) that does the actual DNAT.

**Mental model 3 — The "change tracker" pattern.**
The informer callbacks in step [3] (`OnServiceUpdate`, `OnEndpointSliceAdd`) do
**not** edit kube-proxy's live maps directly. That would be unsafe (callbacks
fire concurrently) and wasteful (one rewrite per event). Instead each callback
*stages* its diff into a tracker (`proxier.serviceChanges`,
`proxier.endpointsChanges`) and then asks for a sync. `syncProxyRules()` later
*drains* those staged diffs in one shot. Think of the trackers as an inbox and
`syncProxyRules()` as the worker that processes the whole inbox at once.

#### 4.1 Initialization and Applying Changes

```go
// Lines 638-720: syncProxyRules()
func (proxier *Proxier) syncProxyRules() (retryError error) {
    proxier.mu.Lock()
    defer proxier.mu.Unlock()

    if !proxier.isInitialized() { return }  // wait until Services/Endpoints have been received

    // Whether a full sync is needed (line 660)
    doFullSync := proxier.needFullSync ||
        (time.Since(proxier.lastFullSync) > proxyutil.FullSyncPeriod && !proxier.largeClusterMode)

    // Merge pending changes (line 668)
    serviceUpdateResult := proxier.svcPortMap.Update(proxier.serviceChanges)
    endpointUpdateResult := proxier.endpointsMap.Update(proxier.endpointsChanges)
```

**What is `doFullSync`?**
kube-proxy can rewrite its rules two ways:

- **Full sync** (`doFullSync == true`): regenerate **every** rule for **every**
  Service from scratch, and re-create the top-level "jump" chains (see 4.2). This
  is correct but expensive — on a cluster with thousands of Services it produces
  a huge iptables ruleset.
- **Partial sync** (`doFullSync == false`): only rewrite the chains for the
  Services that actually changed since last time, and leave everything else
  untouched. Much cheaper, used for the common case of "one Service changed."

The boolean decides which mode this run uses. It is `true` when **any** of these hold:

1. `proxier.needFullSync` is set — e.g. on the very first sync after startup, or
   after a previous sync failed, or when the `Monitor` from step [2] detected
   that something **outside** kube-proxy flushed the iptables rules (so kube-proxy
   must rebuild everything).
2. More than `FullSyncPeriod` has elapsed since the last full sync — a periodic
   "safety net" rewrite to correct any drift...
3. ...**unless** `largeClusterMode` is on, in which case the periodic full sync is
   deliberately skipped because rewriting everything would be too costly. Large
   clusters rely on partial syncs plus the explicit `needFullSync` triggers.

> ⚠️ A "full sync" is not about *Services*, it is about *rules*. It does not
> re-fetch anything from the API server — all data is already in
> `proxier.svcPortMap` / `proxier.endpointsMap` (kept current by the informers).
> "Full" just means "re-emit the complete iptables ruleset" instead of a delta.

**What does "Merge pending changes" mean?**
These two lines drain the change-tracker inbox described in Mental Model 3:

```go
serviceUpdateResult := proxier.svcPortMap.Update(proxier.serviceChanges)
endpointUpdateResult := proxier.endpointsMap.Update(proxier.endpointsChanges)
```

- `proxier.serviceChanges` / `proxier.endpointsChanges` hold the **diffs**
  accumulated from the watch callbacks since the last sync (the staged inbox).
- `proxier.svcPortMap` / `proxier.endpointsMap` are the **authoritative maps** —
  kube-proxy's current belief about "all Services" and "all endpoints."
- `.Update(...)` applies (merges) the staged diffs into the authoritative maps
  and then **clears the inbox**, so the same change is never processed twice.
  Internally `Update` calls `merge` (add/replace changed entries) and `unmerge`
  (delete removed entries) — see [pkg/proxy/servicechangetracker.go](../pkg/proxy/servicechangetracker.go#L168).
- The returned `serviceUpdateResult` / `endpointUpdateResult` report **what
  changed** (e.g. which Services were updated, which stale conntrack entries must
  be cleared). A partial sync uses this to decide which chains to rewrite.

> ⚠️ "Merge" here is bookkeeping on Go maps, not anything to do with iptables
> yet. After these two lines, the maps are up to date; the actual rule-writing
> happens further down. This separation is why the inbox/worker split exists.

#### 4.2 Jump Rule Generation (on Full Sync)

```go
// Lines 687-713: set up iptables jump chains
// NAT table:
//   PREROUTING → KUBE-SERVICES
//   OUTPUT     → KUBE-SERVICES
//   POSTROUTING → KUBE-POSTROUTING

// Filter table:
//   INPUT   → KUBE-EXTERNAL-SERVICES, KUBE-NODE-PORTS
//   FORWARD → KUBE-SERVICES, KUBE-FORWARD
//   OUTPUT  → KUBE-SERVICES
```

#### 4.3 Per-Service Rule Generation (lines 829-1230)

```go
for svcName, svc := range proxier.svcPortMap {
    svcInfo, _ := svc.(*servicePortInfo)

    // Categorize endpoints
    clusterEndpoints, localEndpoints, hasEndpoints := proxy.CategorizeEndpoints(
        allEndpoints, svcInfo, proxier.nodeName, proxier.topologyLabels)

    // ClusterIP rules (lines 938-957)
    if hasInternalEndpoints {
        natRules.Write(
            "-A", "KUBE-SERVICES",
            "-m", "comment", "--comment", fmt.Sprintf(`"%s cluster IP"`, svcPortNameString),
            "-m", protocol, "-p", protocol,
            "-d", svcInfo.ClusterIP().String(),
            "--dport", strconv.Itoa(svcInfo.Port()),
            "-j", string(internalTrafficChain))  // KUBE-SVC-xxx chain
    } else {
        // REJECT if there are no endpoints
        filterRules.Write("-A", "KUBE-SERVICES", ..., "-j", "REJECT")
    }

    // NodePort rules (lines 1025-1061)
    if svcInfo.NodePort() != 0 && hasEndpoints {
        natRules.Write(
            "-A", "KUBE-NODE-PORTS",
            "-m", protocol, "-p", protocol,
            "--dport", strconv.Itoa(svcInfo.NodePort()),
            "-j", string(externalTrafficChain))  // KUBE-EXT-xxx chain
    }

    // LoadBalancer IP rules (lines 987-1023)
    for _, lbip := range svcInfo.LoadBalancerVIPs() {
        natRules.Write(
            "-A", "KUBE-SERVICES",
            "-d", lbip.String(),
            "--dport", strconv.Itoa(svcInfo.Port()),
            "-j", string(loadBalancerTrafficChain))
    }
}
```

**What are `clusterEndpoints` and `localEndpoints`?**
A Service can have a *traffic policy* that controls **which** backend Pods are
eligible to receive a request. `CategorizeEndpoints`
([pkg/proxy/topology.go](../pkg/proxy/topology.go#L48)) takes the full list of a
Service's endpoints and splits it into two buckets:

- **`clusterEndpoints`** — the backends used when the traffic policy is
  `Cluster` (the default). This is essentially *all ready endpoints in the whole
  cluster* (optionally narrowed by topology/zone hints). A request can be load-
  balanced to **any** node's Pod. Maximizes spreading, but a packet may take an
  extra hop to another node and its **source IP gets rewritten** (SNAT) on the
  way.
- **`localEndpoints`** — the backends used when the traffic policy is `Local`.
  This contains **only endpoints running on *this* node**. If a request arrives
  and there is a local Pod, it is sent there directly. This **preserves the
  client's source IP** (no extra hop, no SNAT), which matters for things like
  source-IP-based firewalls or logging — but if no local Pod exists, traffic to
  that node is dropped.
- **`hasEndpoints`** (named `hasAnyEndpoints` in the source) — a simple boolean:
  "does this Service have *any* usable backend at all?" If `false`, kube-proxy
  installs a **REJECT** rule instead of a DNAT rule, so clients get an immediate
  "connection refused" rather than a silent timeout.

There are actually two independent policies — `internalTrafficPolicy` governs
in-cluster (ClusterIP) traffic and `externalTrafficPolicy` governs traffic that
entered via NodePort/LoadBalancer — which is why the code separately checks
`hasInternalEndpoints` for the ClusterIP rule and `hasEndpoints` for the NodePort
rule.

> ⚠️ `CategorizeEndpoints` also has a "terminating endpoints" fallback: if a
> Service has **no Ready** endpoints but some Pods are still *serving while
> terminating*, those are used as a last resort so a rolling update does not
> cause a momentary outage. That is why the function checks `IsServing()` and
> `IsTerminating()`, not just `IsReady()`.

**How to read the `natRules.Write(...)` call (iptables syntax decoded).**
`natRules` is just a text buffer; each `Write(...)` appends one line that is later
fed to `iptables-restore`. The string arguments are exactly the flags you would
type on an `iptables` command line. Decoding the ClusterIP rule:

```go
natRules.Write(
    "-A", "KUBE-SERVICES",                                  // append a rule to chain KUBE-SERVICES
    "-m", "comment", "--comment", "\"<ns/name> cluster IP\"", // attach a human-readable comment
    "-m", protocol, "-p", protocol,                        // load the tcp/udp match module, match that protocol
    "-d", svcInfo.ClusterIP().String(),                    // match packets destined for the ClusterIP
    "--dport", strconv.Itoa(svcInfo.Port()),               // ...and the Service port
    "-j", string(internalTrafficChain))                    // if all matched, jump to KUBE-SVC-xxxx
```

| Argument | iptables meaning |
| --- | --- |
| `-A KUBE-SERVICES` | **A**ppend this rule to the `KUBE-SERVICES` chain. |
| `-m comment --comment "..."` | Load the `comment` match module and attach a label. Purely cosmetic — it makes `iptables-save` readable and lets you grep for a Service. |
| `-m tcp -p tcp` | Load the protocol match module (`-m tcp`) and require the packet's protocol to be TCP (`-p tcp`). For UDP Services these say `udp`. |
| `-d <ClusterIP>` | Match only packets whose **d**estination IP is this Service's ClusterIP. |
| `--dport <port>` | Match only packets whose destination port is the Service port. |
| `-j KUBE-SVC-xxxx` | The action: **j**ump to the per-Service chain, which then picks an actual backend Pod. |

Put together, that one line means: *"any TCP packet headed for this ClusterIP on
this port should be handed off to the Service's load-balancing chain."* The
per-Service chain (`KUBE-SVC-*`) then chooses a backend and the per-endpoint
chain (`KUBE-SEP-*`) performs the DNAT. The NodePort and LoadBalancer `Write`
calls are the same pattern with a different match (`--dport <nodePort>` or
`-d <loadBalancerIP>`) and a different jump target.

#### 4.4 Endpoint Load-Balancing Rules

**File:** [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go#L1446)

```go
// Lines 1446-1490: writeServiceToEndpointRules()
func (proxier *Proxier) writeServiceToEndpointRules(natRules, svcPortNameString,
    svcInfo, svcChain, endpoints, args) {

    // Session affinity (ClientIP-based)
    if svcInfo.SessionAffinityType() == ServiceAffinityClientIP {
        for _, ep := range endpoints {
            natRules.Write(
                "-A", string(svcChain),
                "-m", "recent", "--name", string(epInfo.ChainName),
                "--rcheck", "--seconds", strconv.Itoa(svcInfo.StickyMaxAgeSeconds()),
                "--reap",
                "-j", string(epInfo.ChainName))  // same client → same EP
        }
    }

    // Probabilistic load balancing (line 1468)
    numEndpoints := len(endpoints)
    for i, ep := range endpoints {
        args = append(args[:0], "-A", string(svcChain))

        if i < numEndpoints-1 {
            // i-th rule: probability 1/(numEndpoints-i)
            args = append(args,
                "-m", "statistic", "--mode", "random",
                "--probability", proxier.probability(numEndpoints-i))
        }
        natRules.Write(args, "-j", string(epInfo.ChainName))
    }
}
```

**Example load-balancing rules (3 endpoints):**
```bash
# KUBE-SVC-XXXXXXXX (Service chain)
-A KUBE-SVC-XXXXXXXX -m statistic --mode random --probability 0.33333333349 \
   -j KUBE-SEP-AAAAAAAAAAAA   # Pod A (33%)
-A KUBE-SVC-XXXXXXXX -m statistic --mode random --probability 0.50000000000 \
   -j KUBE-SEP-BBBBBBBBBBBB   # Pod B (50% of remaining = 33%)
-A KUBE-SVC-XXXXXXXX \
   -j KUBE-SEP-CCCCCCCCCCCC   # Pod C (100% of remaining = 33%)

# KUBE-SEP-AAAAAAAAAAAA (Endpoint chain) - DNAT
-A KUBE-SEP-AAAAAAAAAAAA -m comment --comment "default/nginx" \
   -s 10.244.0.2 -j KUBE-MARK-MASQ   # masquerade requests coming from itself
-A KUBE-SEP-AAAAAAAAAAAA -m tcp -p tcp \
   -j DNAT --to-destination 10.244.0.2:8080   # DNAT
```

**Why those odd probabilities (0.333, 0.5, 1.0)?**
iptables has no built-in "pick one of N at random" action. It only evaluates
rules **top to bottom**, each rule being "with probability *p*, jump; otherwise
fall through to the next rule." To make three endpoints each get an equal 1/3
share with that sequential model, kube-proxy uses **conditional** probabilities —
the `i`-th rule uses `1/(numEndpoints - i)`:

| Rule | Probability used | Chance of *reaching* this rule | Overall share |
| --- | --- | --- | --- |
| 1st (Pod A) | `1/3 ≈ 0.333` | 100% | 1/3 |
| 2nd (Pod B) | `1/2 = 0.5` | 2/3 (A didn't match) | 2/3 × 1/2 = 1/3 |
| 3rd (Pod C) | none (always jump) | 1/3 (A and B didn't match) | 1/3 |

The last endpoint has **no** `--probability` (it is the unconditional catch-all),
which guarantees that *some* endpoint is always chosen. This is why the code only
adds the `statistic` match when `i < numEndpoints-1`.

> ⚠️ This is **per-packet** randomness, but a connection does **not** flip
> between Pods. Only the very first packet (the SYN) runs the random gauntlet;
> conntrack then records the chosen Pod and every later packet of that connection
> is translated to the same Pod automatically. Load balancing is therefore
> per-connection, not per-packet.

**The two lines inside a `KUBE-SEP-*` (per-endpoint) chain:**

- `-s 10.244.0.2 -j KUBE-MARK-MASQ` — *if the request's **source** is the
  endpoint Pod itself*, mark it for masquerade (SNAT). This handles the
  "hairpin" case where a Pod reaches a Service that load-balances back to that
  same Pod; without the SNAT the Pod would see its own IP as both source and
  destination and the reply would never come back.
- `-j DNAT --to-destination 10.244.0.2:8080` — the actual destination rewrite:
  send the packet to this Pod's real IP and port. This is the line that makes the
  virtual ClusterIP "become" a real Pod.

**Session affinity (`-m recent`).** When a Service sets
`sessionAffinity: ClientIP`, kube-proxy adds a rule *before* the random rules
that uses the kernel `recent` module to remember which endpoint a given client IP
was last sent to (`--rcheck` within `--seconds <StickyMaxAgeSeconds>`). A
returning client skips load balancing and is pinned to the same endpoint chain,
so all of one client's connections land on the same Pod until the timer expires.

#### 4.5 Atomic Application via iptables-restore

```go
// Lines 1250+: hand the rule buffer to iptables-restore
proxier.iptablesData.Reset()
// Write chain definitions + rules into a single buffer
// └─ *filter
// └─ :KUBE-FORWARD - [0:0]
// └─ -A KUBE-FORWARD ...
// └─ COMMIT
// └─ *nat
// └─ ...

err := proxier.iptables.Restore(table, proxier.iptablesData.Bytes(), ...)
// └─ executes iptables-restore --noflush --counters
// └─ atomic application: no intermediate state
```

---

## [5] Actual Packet Flow

### Accessing a ClusterIP

```
Pod A (10.244.0.2) → Service ClusterIP (10.0.0.1:80)

1. SYN packet: 10.244.0.2:12345 → 10.0.0.1:80
   │
   └─ PREROUTING (NAT)
      └─ KUBE-SERVICES
         └─ "-d 10.0.0.1 --dport 80 -j KUBE-SVC-ABC"
            │
            └─ KUBE-SVC-ABC (load balancing)
               └─ "-m statistic --probability 0.333 -j KUBE-SEP-POD-B"
                  │
                  └─ KUBE-SEP-POD-B
                     └─ DNAT: 10.0.0.1:80 → 10.244.0.3:8080
                        │
                        └─ Routing: forwarded to Pod B

2. Recorded in conntrack:
   src=10.244.0.2 dst=10.244.0.3 sport=12345 dport=8080 [ESTABLISHED]

3. Response: 10.244.0.3:8080 → 10.244.0.2:12345 (conntrack reverses the translation)
```

### Accessing a NodePort

```
External client (203.0.113.5) → NodeIP:30001

1. Packet: 203.0.113.5:54321 → 192.168.1.10:30001
   │
   └─ INPUT (NAT)
      └─ KUBE-NODE-PORTS
         └─ "--dport 30001 -j KUBE-EXT-ABC"
            │
            └─ KUBE-EXT-ABC
               └─ (externalTrafficPolicy=Cluster) → KUBE-SVC-ABC
                  └─ DNAT + masquerade

2. Masquerade (Source NAT):
   Replace the external client IP with the node IP
   (so the Pod sends responses back to the correct node)
```

---

## Change-Tracking Structures

### ServiceChangeTracker

**File:** [pkg/proxy/servicechangetracker.go](../pkg/proxy/servicechangetracker.go#L31)

```go
// Stays pending until changes are accumulated
type serviceChange struct {
    previous ServicePortMap  // previous state
    current  ServicePortMap  // current state
}

// On Update, only the delta is recorded (removed if there is no change)
func (sct *ServiceChangeTracker) Update(previous, current *v1.Service) bool {
    if reflect.DeepEqual(change.previous, change.current) {
        delete(sct.items, namespacedName)
        return false  // no actual change
    }
    return true
}
```

### EndpointSliceCache

**File:** [pkg/proxy/endpointslicecache.go](../pkg/proxy/endpointslicecache.go#L34)

```go
type EndpointSliceCache struct {
    trackerByServiceMap map[types.NamespacedName]*endpointSliceTracker
}

type endpointSliceTracker struct {
    applied endpointSliceDataByName  // slices already applied to iptables
    pending endpointSliceDataByName  // slices to be applied in the next syncProxyRules
}
```

---

## Partial Sync vs Full Sync

| Aspect | Partial Sync | Full Sync |
|------|-------------|-----------|
| When | On individual Service/Endpoint changes | Periodically (default 30 min) or when external modification is detected |
| Scope | Updates only the changed Service chains | Regenerates all rules |
| Jump rules | Not regenerated | Regenerated |
| Performance | Fast | Slower, but guarantees consistency |

**Large Cluster Mode (1000+ endpoints):**
- Increases the full sync period
- Optimizes memory efficiency

---

## Key iptables Chain Structure

```
NAT table:

PREROUTING ──→ KUBE-SERVICES ──→ KUBE-SVC-{hash}    (ClusterIP traffic)
OUTPUT     ──→                └──→ KUBE-EXT-{hash}    (external traffic + NodePort)
                              └──→ KUBE-FW-{hash}     (LoadBalancer with source range)

KUBE-SVC-{hash} ──→ KUBE-SEP-{hash}  (per-Endpoint chain, performs DNAT)

POSTROUTING ──→ KUBE-POSTROUTING ──→ KUBE-MARK-MASQ (masquerade)

Filter table:

FORWARD ──→ KUBE-FORWARD   (allow Pod-to-Pod traffic)
INPUT   ──→ KUBE-NODE-PORTS (allow NodePort traffic)
```

---

## Key File Path Summary

| Step | File | Key Function | Line |
|------|------|----------|------|
| Endpoint controller | [pkg/controller/endpoint/endpoints_controller.go](../pkg/controller/endpoint/endpoints_controller.go) | `syncService` | 334 |
| EndpointSlice controller | [pkg/controller/endpointslice/endpointslice_controller.go](../pkg/controller/endpointslice/endpointslice_controller.go) | `NewController` | 84 |
| kube-proxy startup | [cmd/kube-proxy/app/server.go](../cmd/kube-proxy/app/server.go) | `newProxyServer` | 183 |
| iptables Proxier | [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go) | `NewProxier` | 214 |
| Watch callbacks | [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go) | `OnServiceUpdate`, `OnEndpointSliceAdd` | 459 |
| Core rule generation | [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go) | `syncProxyRules` | 638 |
| Load-balancing rules | [pkg/proxy/iptables/proxier.go](../pkg/proxy/iptables/proxier.go) | `writeServiceToEndpointRules` | 1446 |
| Service change tracking | [pkg/proxy/servicechangetracker.go](../pkg/proxy/servicechangetracker.go) | `Update` | 76 |
| EndpointSlice cache | [pkg/proxy/endpointslicecache.go](../pkg/proxy/endpointslicecache.go) | `checkoutChanges` | 122 |

---

## Related Concepts

- **A Service is a virtual IP, not a process.** A ClusterIP is bound to no interface and answers no pings on its own; it exists *only* as iptables/IPVS/nftables rules that rewrite packet destinations. Nothing is "listening" at the VIP.
- **kube-proxy modes.** `iptables` (rule-based, the common default), `ipvs` (kernel hash tables, scales to many Services), and `nftables` (the modern successor) all implement the same `proxy.Provider` interface — only the data-plane mechanism differs.
- **Endpoints vs. EndpointSlice.** EndpointSlice replaces the single, monolithic Endpoints object by sharding backends (≤100 per slice) and adding zone/topology hints — the basis for scalability and topology-aware routing.
- **conntrack, DNAT, and SNAT/masquerade.** The first packet of a connection is **DNAT**'d to a chosen Pod and recorded in **conntrack**, so the whole flow sticks to that Pod; **masquerade** (SNAT) rewrites the source IP when needed so replies route back correctly.
- **`externalTrafficPolicy` / `internalTrafficPolicy`.** `Local` keeps traffic on node-local endpoints, preserving the client source IP and skipping an extra hop; `Cluster` spreads load across all endpoints at the cost of an SNAT and a possible second hop.
- **Service type layering.** ClusterIP → NodePort → LoadBalancer build on each other, which is exactly the chain order you see in iptables: `KUBE-SERVICES` → `KUBE-EXT-*` (NodePort/external) → `KUBE-FW-*` (LoadBalancer source ranges).
- **Headless Services & DNS.** A Service with `clusterIP: None` skips proxying entirely; DNS returns the Pod IPs directly, which is how StatefulSets give each Pod a stable name.

> ⚠️ **kube-proxy doesn't sit in the data path.** It only *programs* kernel rules; the kernel then forwards packets with no per-packet userspace hop. If traffic breaks, inspect the rules it wrote (`iptables-save`), not a kube-proxy "connection."

---

## Related Scenarios

- [Scenario 4: kubelet Pod Lifecycle](04-kubelet-pod-lifecycle.md) — readinessProbe affects Endpoint state
- [Scenario 1: API Request Flow](01-api-request-flow.md) — the flow in which the Service object is stored
