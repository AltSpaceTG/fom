# Kotlin DSL & coroutines

`fom-kotlin` adds an idiomatic `graph { … }` builder, a `suspend`-based process
base class, and suspend extensions over the engine's `CompletionStage` APIs.

## The `graph { }` DSL

```kotlin
import io.fom.kotlin.graph

val g = graph {
    process("Inventory", ::InventoryInit, ::InventoryInit)
        .handles<GetInventory>()

    process("Products", ::ProductsInit, ::ProductsInit, dependsOn = listOf("Inventory"))
        .handles<GetProductModel>()

    // parameterised node:
    processWithParam("Tenant_X", ::TenantInit, ::TenantInit, param = TenantParam("X"))

    // dynamic route:
    route<GetInventory> { q -> "Inventory_${q.pub}" }
}
```

- `process(name, ::Init, ::Load, dependsOn = …)` adds a node; `dependsOn` names
  become reactive dependencies.
- `processWithParam(...)` adds a parameterised node.
- `.handles<Q>()` attaches a static route for query type `Q` to that node — and
  it binds to the **right node by name**, so reordering or adding later nodes
  won't misroute it.
- `route<Q> { … }` registers a dynamic route. The DSL wraps your lambda so it is
  serializable (it adds `@JvmSerializableLambda` for you); your resolver must not
  capture non-serializable state.

The factory references (`::InventoryInit`) are wrapped into serializable
suppliers automatically.

## Suspending processes

Extend `SuspendingProcess` to write `compute`/`cleanUp` as `suspend` functions;
the framework bridges to/from `CompletionStage`.

```kotlin
import io.fom.kotlin.SuspendingProcess
import io.fom.api.QueryableContext

class PricingProcess : SuspendingProcess() {
    override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? {
        val stock = ctx.queryAwait("Inventory", GetStock((query as GetPrice).sku))
        return priceFor(stock)
    }
}
```

`SuspendingProcess` owns a single long-lived `CoroutineScope` (one
`SupervisorJob`), so it does **not** leak a coroutine job per `compute`/`cleanUp`
call. The default dispatcher is configurable via the constructor.

## Suspend extensions

```kotlin
import io.fom.kotlin.*

val r: Any?  = engine.queryAwait(GetInventory("PUB1"))
val typed: ProductModel = engine.queryAs(GetProductModel("PUB1"))   // reified cast
val byName: Any? = engine.queryProcessAwait("Products", msg)

// inside a process:
val dep: Any? = ctx.queryAwait("Inventory", GetStock("PUB1"))
```

`queryAwait`/`queryProcessAwait` return `Any?` because a process is free to
complete a query with `null`. `queryAs<R>` does a reified cast to `R` for
convenience.

## Build setup

```kotlin
dependencies {
    implementation("io.fom:fom-kotlin:0.1.0-SNAPSHOT")  // brings fom-core + coroutines-core
}
```

`fom-kotlin` exposes `kotlinx-coroutines-core` as `api`; the JDK8 integration is
an implementation detail.
