# Kotlin DSL и корутины

`fom-kotlin` добавляет идиоматичный билдер `graph { … }`, базовый класс процесса на
`suspend` и suspend-расширения над `CompletionStage`-API движка.

## DSL `graph { }`

```kotlin
import io.fom.kotlin.graph

val g = graph {
    process("Inventory", ::InventoryInit, ::InventoryInit)
        .handles<GetInventory>()

    process("Products", ::ProductsInit, ::ProductsInit, dependsOn = listOf("Inventory"))
        .handles<GetProductModel>()

    // параметризованный узел:
    processWithParam("Tenant_X", ::TenantInit, ::TenantInit, param = TenantParam("X"))

    // динамический маршрут:
    route<GetInventory> { q -> "Inventory_${q.pub}" }
}
```

- `process(name, ::Init, ::Load, dependsOn = …)` добавляет узел; имена в
  `dependsOn` становятся реактивными зависимостями.
- `processWithParam(...)` добавляет параметризованный узел.
- `.handles<Q>()` прикрепляет статический маршрут для типа `Q` к этому узлу — и
  привязывает его к **нужному узлу по имени**, поэтому переупорядочивание или
  добавление поздних узлов не сломает маршрут.
- `route<Q> { … }` регистрирует динамический маршрут. DSL оборачивает вашу лямбду
  так, чтобы она была сериализуемой (добавляет `@JvmSerializableLambda` за вас);
  ваш резолвер не должен захватывать несериализуемое состояние.

Ссылки на фабрики (`::InventoryInit`) автоматически оборачиваются в сериализуемые
суплаеры.

## Suspend-процессы

Наследуйте `SuspendingProcess`, чтобы писать `compute`/`cleanUp` как `suspend`-функции;
фреймворк мостит к/от `CompletionStage`.

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

`SuspendingProcess` владеет одним долгоживущим `CoroutineScope` (один
`SupervisorJob`), поэтому он **не** течёт job корутины на каждый вызов
`compute`/`cleanUp`. Диспетчер по умолчанию настраивается через конструктор.

## Suspend-расширения

```kotlin
import io.fom.kotlin.*

val r: Any?  = engine.queryAwait(GetInventory("PUB1"))
val typed: ProductModel = engine.queryAs(GetProductModel("PUB1"))   // reified-каст
val byName: Any? = engine.queryProcessAwait("Products", msg)

// внутри процесса:
val dep: Any? = ctx.queryAwait("Inventory", GetStock("PUB1"))
```

`queryAwait`/`queryProcessAwait` возвращают `Any?`, потому что процесс волен
завершить запрос значением `null`. `queryAs<R>` делает reified-каст к `R` для
удобства.

## Настройка сборки

```kotlin
dependencies {
    implementation("io.fom:fom-kotlin:0.1.0-SNAPSHOT")  // тянет fom-core + coroutines-core
}
```

`fom-kotlin` выставляет `kotlinx-coroutines-core` как `api`; интеграция JDK8 — это
деталь реализации.

> [English version](../../guides/kotlin-dsl.md)
