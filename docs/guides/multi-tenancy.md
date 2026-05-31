# Multi-tenancy

A common pattern is one process **per tenant** — e.g. `Inventory_PUB1`,
`Inventory_PUB2` — routed by a [`Routable`](../concepts/graph-and-routing.md)
message or a dynamic route. The `fom-tenant` module adds a **defence-in-depth
authorization wrapper** so a caller can only touch tenants it is allowed to.

!!! info "It's a wrapper, not a replacement"
    `TenantAwareEngine` delegates to a normal `Engine`. The engine stays the
    source of truth; the wrapper adds an authz check on top of `query` /
    `queryProcess` / `trigger` / `shutdownTenant`.

## Building the wrapper

```java
import io.fom.tenant.*;

var aware = TenantAwareEngine.builder(engine)
    .tenantResolver(TenantResolver.suffixAfter("_"))                  // "Inventory_PUB1" → "PUB1"
    .authzPolicy((caller, tenant) -> caller.tenants().contains(tenant))
    .build();
```

- **`TenantResolver`** maps a process name to its `TenantId`.
  `TenantResolver.suffixAfter("_")` takes the part after the last `_`; implement
  the interface for any other convention. A name that resolves to no tenant
  (e.g. a global support process) bypasses the check.
- **`authzPolicy`** is a `BiPredicate<TenantCaller, TenantId>` deciding whether a
  caller may act on a tenant.

!!! danger "Default is deny-all"
    If you don't set an `authzPolicy`, the wrapper **denies everything** —
    fail-closed. You must opt into access explicitly.

## Callers and tenants

```java
TenantId pub1 = TenantId.of("PUB1");
TenantCaller alice = TenantCaller.of("alice", pub1);     // may act on PUB1
TenantCaller anon  = TenantCaller.anonymous();           // no tenants
```

## Authorized operations

```java
// query: only Routable messages are accepted (target known up front)
aware.query(alice, new GetInventory("PUB1"));            // ok if alice has PUB1

// explicit addressing
aware.queryProcess(alice, "Inventory_PUB1", msg);

// trigger
aware.trigger(alice, "Inventory_PUB1", new RefreshSignal("x"));

// per-tenant lifecycle
aware.shutdownTenant(alice, pub1, Duration.ofSeconds(30));
```

### Why `query` rejects non-`Routable` messages

`aware.query(caller, msg)` must know the target tenant *before* dispatch to
authorize it. A `Routable` message carries its `targetProcess()`, so the tenant
is known. A non-`Routable` message would be type-routed *inside* the engine to a
process the wrapper never checked — a fail-open hole. So `query` rejects
non-`Routable` messages with `TenantAccessDeniedException`; use
`queryProcess(caller, name, msg)` for explicit addressing.

## Denied access

A denied call fails the returned stage with `TenantAccessDeniedException` (for
`query`/`queryProcess`/`shutdownTenant`) or throws it directly (for `trigger`):

```java
aware.query(anon, new GetInventory("PUB1"))
    .whenComplete((r, err) -> {
        if (err instanceof TenantAccessDeniedException) { /* 403 */ }
    });
```

## Recap of the defaults

| Setting | Default | Implication |
|---|---|---|
| `tenantResolver` | `suffixAfter("_")` | name `X_TENANT` → tenant `TENANT` |
| `authzPolicy` | `(c, t) -> false` | **deny-all** until configured |
| non-`Routable` `query` | rejected | use `queryProcess` |

See [Security](../security.md) for the wider threat model.
