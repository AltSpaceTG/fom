# Kotlin DSL и корутины

`fom-kotlin` добавляет идиоматичный билдер `graph { … }`, базовый класс процесса на
`suspend` и suspend-расширения над `CompletionStage`-API движка.

## DSL `graph { }` { #dsl-graph }

```kotlin
import io.fom.kotlin.graph

val g = graph {
    process("Stations", ::StationsInit, ::StationsInit)
        .handles<GetStations>()

    // зависимость — это имя, ProcessRef или готовый Dependency; их можно смешивать:
    process("Forecasts", ::ForecastsInit, ::ForecastsInit, dependsOn = listOf(STATIONS))   // STATIONS: ProcessRef.of("Stations")
        .handles<GetForecastModel>()

    // параметризованный узел:
    processWithParam("Tenant_X", ::TenantInit, ::TenantInit, param = TenantParam("X"))

    // динамический маршрут (резолвер выбирает узел для каждого запроса):
    route<GetTenantReport> { q -> "Tenant_${q.tenantId}" }   // "Tenant_X" for tenantId = "X"
}
```

- `process(name, ::Init, ::Load, dependsOn = …, stableDependsOn = …)` добавляет
  узел; имена в `dependsOn` становятся реактивными зависимостями (новая версия
  переинициализирует узел), имена в `stableDependsOn` —
  [стабильными](../concepts/reactive-cascade.md#реактивные-и-стабильные) (к ним
  только обращаются с запросами). Оба параметра есть и у `processWithParam`.
- Необязательный последний параметр `reinitStrategy: ReinitStrategy? = null` у
  `process(...)` / `processWithParam(...)` (с именем и с `ProcessRef`)
  переопределяет [`EngineConfig.reinitStrategy`](configuration.md) для этого
  узла; `null` оставляет стратегию движка. Она не входит в определение узла,
  поэтому смена одной лишь стратегии при замене графа ничего не перезапускает:

    ```kotlin
    process("Forecasts", ::ForecastsInit, ::ForecastsInit,
            dependsOn = listOf("Stations"),
            reinitStrategy = ReinitStrategy.RELEASE_FIRST)   // слишком велик, чтобы держать две версии
    ```
- Зависимость можно задать строковым именем, типизированным `ProcessRef` или
  готовым `Dependency`, причём все три вида свободно смешиваются внутри одного
  списка — независимо от того, как назван сам узел. Поэтому узел с именем-строкой
  может зависеть от ref'ов, а узел с именем-ref'ом — от строк:

    ```kotlin
    process("Caller", ::CallerInit, ::CallerInit,
            dependsOn = listOf(STATIONS, "Alerts"),
            stableDependsOn = listOf(ProcessRef.of("Audit")))
    ```

    Элемент типа `Dependency` сохраняет свой вид: `Dependency.stable(ref)`
    останется стабильной зависимостью, даже если указать её в `dependsOn`. Всё,
    что не является `String`, `ProcessRef` или `Dependency`, приводит к
    `IllegalArgumentException`.
- `processWithParam(...)` добавляет параметризованный узел.
- `.handles<Q>()` прикрепляет статический маршрут для типа `Q` к этому узлу — и
  привязывает его к **нужному узлу по имени**, поэтому переупорядочивание или
  добавление поздних узлов не сломает маршрут.
- `route<Q> { … }` регистрирует динамический маршрут. У типа запроса может быть
  только один маршрут — либо `.handles<Q>()`, либо `route<Q> { … }`; вторая
  регистрация бросает `IllegalArgumentException` сразу же («Query type … already
  routed to …»), а не на `build()`.
- Для типа запроса, реализующего `Routable`, `.handles<Q>()` и
  `route<Q> { … }` **принимаются, но никогда не используются**:
  `engine.query(msg)` сначала спрашивает `msg.targetProcess()`, так что
  `Routable` всегда побеждает, а маршрут по типу — мёртвая конфигурация.
  Маршрутизируйте такой тип одним способом — через `Routable`.

Фабрики (`::StationsInit` или любая лямбда) и резолверы маршрутов — обычные
функции Kotlin. В лог они никогда не пишутся, поэтому могут захватывать что угодно —
DI-контейнер, клиент, конфигурацию.

### Параметрам нужно равенство по значению { #param-equality }

`param` в лог **пишется**, и при каждом рестарте движок сравнивает записанную
копию с той, что в вашем графе, чтобы выбрать между тёплой загрузкой и
холодным init. Сравнение — это `equals`, поэтому делайте параметры
**`data class`** — или **`data object`** для параметра без полей. Обычный
`object` (или обычный `class`) сравнивается по ссылке: декодированная копия —
другой экземпляр, она никогда не равна вашему, движок пишет `WARN` «… does not
override equals(), so it never equals its persisted copy and the process
cold-inits on every restart …», и процесс именно так и делает.

```kotlin
data class TenantParam(val tenant: String) : java.io.Serializable   // ✓ равен по значению
data object DefaultRegion : java.io.Serializable                    // ✓ равен по типу
object Broken : java.io.Serializable                                // ✗ холодный init на каждом рестарте
```

Что делает с этим сравнением изменение класса param, см. в
[Обновлениях](../concepts/idempotent-restart.md#upgrades).

## Suspend-процессы

Наследуйте `SuspendingProcess`, чтобы писать `compute`/`cleanUp` как `suspend`-функции;
фреймворк мостит к/от `CompletionStage`.

```kotlin
import io.fom.kotlin.SuspendingProcess
import io.fom.kotlin.queryAwait          // ctx.queryAwait — extension верхнего уровня
import io.fom.api.QueryableContext

class AlertsProcess : SuspendingProcess() {
    override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? {
        val readings = ctx.queryAwait("Stations", GetReadings((query as GetAlert).stationId))
        return alertFor(readings)
    }
}
```

`SuspendingProcess` группирует свои корутины по *поколениям*: на каждую
загруженную версию (ключ — `ctx.sid()`) — свой `CoroutineScope` с одним
`SupervisorJob`, поэтому он **не** течёт job
корутины на каждый вызов `compute`/`cleanUp`. Вычисления выполняются на
`Dispatchers.Default`, если в
конструктор не передан другой контекст, — для блокирующей работы (JDBC, файловый
ввод-вывод) передайте `Dispatchers.IO`:

```kotlin
class JdbcProcess(private val ds: DataSource) : SuspendingProcess(Dispatchers.IO) { … }
```

Контекст может нести и ваш собственный `Job` — передавать
`appScope.coroutineContext` можно:

```kotlin
class AlertsProcess(appScope: CoroutineScope) : SuspendingProcess(appScope.coroutineContext) { … }
```

fom никогда не присваивает этот `Job` себе. Каждое поколение получает свой
`SupervisorJob` (диспетчер и прочие элементы вашего контекста сохраняются),
связанный с вашим `Job` **только по отмене**:

- **Отмена вашего `Job` останавливает корутины fom.** Как только он отменён
  (или упал), отменяется каждое поколение, построенное на нём: выполняющиеся
  корутины `computeAsync` и `cleanUpAsync` отменяются, а любое последующее
  вычисление на этом экземпляре завершается `CancellationException` — узел
  остаётся сломанным, пока его не загрузят с живым контекстом. Очистка, которая
  после этого завершилась *только* этой отменой, считается успешной
  (`onCleanupCompleted(ok=true)`, без WARN): ваше приложение уже отменило всё,
  что было чистить. `cleanUpAsync`, бросивший что-то другое, по-прежнему
  считается неуспешным.
- **fom никогда не отменяет ваш `Job`.** Вывод поколения из работы при
  переинициализации или `engine.close()`, очистка, вышедшая за бюджет, или
  `computeAsync`, бросивший исключение, отменяют только собственные `Job` fom;
  ваш scope и остальная его работа продолжаются, а упавшее вычисление не влияет
  на следующее.
- **Корутины fom — не структурные дети вашего `Job`.** Поколение живёт, пока fom
  не выведет его из работы, поэтому, будь оно ребёнком, ваш scope не мог бы
  завершиться всё время, пока процесс обслуживает запросы (`runBlocking { }` или
  graceful shutdown, ждущий своих детей, повис бы на движке). Обратная сторона:
  корутины fom отменяются, когда ваш `Job` *завершил* отмену, и `job.join()` их
  не ждёт — их ждёт `engine.close()`.

Когда ответ на запрос завершается ошибкой — таймаут, отмена или унаследованный
дедлайн запроса к зависимости, — движок отменяет вычисление, и корутина
`computeAsync` отменяется.

`cleanUp` выполняет `cleanUpAsync`, а затем отменяет **только то поколение,
которое выводится из работы**, так что брошенные вычисления не живут дольше
процесса. Именно поэтому `load()` может снова вернуть **тот же** экземпляр
`SuspendingProcess` после переинициализации (например, синглтон-сервис):
во время reinit при `KEEP_OLD` новая версия начинает обслуживать раньше, чем
очищается старая, и очистка старого поколения отменяет только вычисления старого
Sid, но не новой версии.

`cleanUp` ждёт `cleanUpAsync`, но **не** ждёт завершения брошенных вычислений
уходящего поколения: они отменяются, и stage завершается сразу. Поэтому
вычисление, застрявшее в блокирующем неотменяемом коде (`Thread.sleep` или
блокирующий вызов JDBC на `Dispatchers.IO`) после таймаута своего запроса, не
задерживает паузу, переинициализацию или `engine.close()` на весь бюджет
очистки: оно доработает в фоне, а его результат будет отброшен.

Поколения учитываются **отдельно для каждого процесса-владельца** — ключ
составляют `ctx.sid()` *и* идентичность `ctx.executor()`, который движок
создаёт по одному на каждую машину состояний процесса, — поэтому один экземпляр
может использоваться и *несколькими узлами одновременно* (например, сервис из
DI-контейнера, который возвращают оба загрузчика), даже узлами с одинаковым
именем в *двух движках* одной JVM. Переинициализация одного из таких узлов или
закрытие одного из движков выводит из работы только поколение этого владельца:
`computeAsync`, ещё выполняющийся в другом месте, не отменяется. (Если вы
вызываете `compute`/`cleanUp` вручную с тестовыми контекстами, возвращайте из
обоих один и тот же экземпляр `executor()` и один и тот же `sid()` для одной
версии.)

Что совместное использование **не** меняет: `cleanUpAsync` по-прежнему
выполняется один раз на каждое выводимое из работы поколение, то есть один раз на
узел при каждой переинициализации. Поэтому общий экземпляр не должен освобождать
в нём общее состояние: закрытие пула `DataSource` в `cleanUpAsync` одного узла
сломает другой узел, который продолжает работать на том же экземпляре.

Если `cleanUpAsync` не успевает за `cleanupTimeout`, его **отменяют**:
движок отменяет stage, который вернул `cleanUp`, а `SuspendingProcess`
пробрасывает эту отмену в корутину очистки и в уходящее поколение целиком — так
что блоки `finally` выполняются.

Это ограничивает тело, которое *отменяемо* в своих точках приостановки: то,
которое стоит на такой точке в момент исчерпания бюджета, на ней и остановится,
и ничего из него не переживает `engine.close()`. Но ограничивает это **не**
любое тело. Тело, которое блокируется ещё до первой приостановки
(`Thread.sleep`, блокирующий вызов JDBC), не ограничено:
`SuspendingProcess.cleanUp` намеренно запускает корутину как `UNDISPATCHED`,
поэтому в тело входят — и его блоки `finally` становятся активны — раньше, чем
может прийти любая отмена, а тело, которое так и не дошло до точки приостановки,
доработает до конца в фоне. *Узел* при этом всё равно ограничен: движок вызывает
`cleanUp` в отдельном потоке, поэтому по истечении бюджета очистки узел идёт
дальше, `close()` возвращает управление и приходит `onCleanupCompleted(ok=false)`,
а это тело продолжает работать. Не
ограничено и тело, которое приостанавливается внутри
`withContext(NonCancellable)`: его точки приостановки по замыслу игнорируют
отмену, поэтому `close()` вернёт управление по истечении бюджета очистки (с
`onCleanupCompleted(ok=false)`), пока эта очистка продолжает работать.
Пишите `cleanUpAsync` так, чтобы он был готов к отмене *и* действительно
приостанавливался: освобождайте ресурсы в `finally`, предпочитайте отменяемые
suspend-вызовы блокирующим, а `withContext(NonCancellable)` используйте только
для короткого ограниченного дозаписывания.

## Suspend init и load

У `init` и `load` тоже есть suspend-мосты. Реализуйте `SuspendingInitializer`
(`suspend fun initAsync(ctx)`) и `SuspendingLoader`
(`suspend fun loadAsync(ctx, properties)`) — один класс может реализовать оба и
передаваться как `::MyProcess, ::MyProcess`. Для параметризованных узлов есть
`SuspendingParamInitializer<P>` / `SuspendingParamLoader<P>`, чьи функции
дополнительно принимают `param`.

```kotlin
import io.fom.kotlin.*
import io.fom.api.Process
import io.fom.api.QueryableContext
import kotlinx.coroutines.Dispatchers
import kotlin.coroutines.CoroutineContext

class Stations : SuspendingInitializer, SuspendingLoader {
    override val initContext: CoroutineContext get() = Dispatchers.IO   // init блокируется на JDBC

    override suspend fun initAsync(ctx: QueryableContext): Map<String, ByteArray> {
        val version = ctx.queryAwait(OBSERVATIONS, GetVersion) as String      // OBSERVATIONS: ProcessRef
        return mapOf("rows" to fetchRows(version))
    }

    override suspend fun loadAsync(ctx: QueryableContext, properties: Map<String, ByteArray>): Process =
        StationsProcess(properties.getValue("rows"))
}

val g = graph {
    process(OBSERVATIONS, ::Observations, ::Observations)   // зависимость тоже должна быть узлом, иначе build() упадёт
    process(STATIONS, ::Stations, ::Stations, dependsOn = listOf(OBSERVATIONS))
}
```

Каждый вызов выполняется в своей корутине на `initContext` / `loadContext`
(`Dispatchers.Default`, если не переопределено). Когда `init` исчерпывает бюджет
`initTimeout` или процесс отменяют либо он умирает, движок отменяет stage
init, и корутина `initAsync` отменяется. Stage `load` отменяется так же — и
когда он больше не нужен, и когда он просто вышел за `loadTimeout`, — так
что корутина `loadAsync` тоже отменяется; см.
[контракт отмены load](../concepts/process-lifecycle.md#load-cancellation).

`initContext` / `loadContext` могут нести `Job` на тех же условиях, что и
контекст `SuspendingProcess` выше: каждый вызов выполняется под своим
`SupervisorJob`, который отменяется при отмене вашего `Job`, а упавший
`initAsync` или `loadAsync` никогда не отменяет ваш `Job` — так что один
временный сбой не обрекает все повторные попытки. Но **уже отменённый** `Job`
заставляет каждую попытку init сразу падать с `CancellationException`, и узел
продолжает повторять — с обычным backoff, но не чаще
[нижней границы](../concepts/process-lifecycle.md#backoff) в половину
`backoffMin`, — пока не исчерпает бюджет init и не уйдёт в `Dead`.

!!! warning "Не блокируйте диспетчер, на котором работает ваш `initContext`"
    `engine.newGraph(...)` — **блокирующий** вызов: он ждёт, пока узлы дойдут
    до `Serving`. Если вызвать его на однопоточном диспетчере — например, в
    event loop `runBlocking`, — а `initContext` при этом равен
    `coroutineContext` этого цикла, корутина `initAsync` диспетчеризуется в тот
    самый поток, который блокирует `newGraph`, и никогда не запускается: вызов
    висит, пока не исчерпается бюджет init, и затем падает. Вызывайте `newGraph`
    вне цикла (`withContext(Dispatchers.IO) { engine.newGraph(g) }`) или
    оставьте `initContext` на `Dispatchers.Default` / `Dispatchers.IO`.

## Suspend-расширения

```kotlin
import io.fom.kotlin.*
import java.time.Duration

// все вызовы ниже — suspend-вызовы, то есть внутри `suspend fun` или корутины
val r: Any?  = engine.queryAwait(GetStations("PUB1"))
val typed: ForecastModel = engine.queryAs(GetForecastModel("ST-1"))   // reified-каст
val byName: Any? = engine.queryProcessAwait("Forecasts", msg)
val byRef: Any?  = engine.queryProcessAwait(FORECASTS, msg)                      // FORECASTS: ProcessRef
val bounded: Any? = engine.queryProcessAwait(FORECASTS, msg, Duration.ofSeconds(2))  // также (name, q, timeout)

// внутри процесса:
val dep: Any? = ctx.queryAwait("Stations", GetReadings("ST-1"))
val depByRef: Any? = ctx.queryAwait(STATIONS, GetReadings("ST-1"))   // STATIONS: ProcessRef
```

`queryAwait`/`queryProcessAwait` возвращают `Any?`, потому что процесс волен
завершить запрос значением `null`. У `ctx.queryAwait` нет перегрузки с таймаутом:
запрос к зависимости наследует дедлайн вызывающего. `queryAs<R>` делает reified-каст к `R` для
удобства.

**Внутри `computeAsync` запрашивайте зависимости через `ctx.queryAwait`, а не
через внешний `Engine`.** Запрос через `ctx` привязан к вычисляемому запросу:
наследует его дедлайн и отменяется, как только этот запрос больше никто не ждёт.
Движок может привязать *внешний* вызов `engine.query…` к породившему запросу,
только пока тот выполняется в потоке, вызвавшем `compute`, а `SuspendingProcess`
всегда отправляет `computeAsync` на свой диспетчер — поэтому
`engine.queryAwait(...)` оттуда становится самостоятельным запросом верхнего
уровня: получает свой `queryTimeout` вместо дедлайна породившего запроса, а
`onQuerySent` не показывает parent id. (Единственное исключение —
`Dispatchers.Unconfined`, и только до первой приостановки тела.) При этом он
**не** остаётся выполняться, когда породивший запрос падает: когда ответ того
запроса завершается ошибкой (таймаут, отмена), движок отменяет его вычисление,
это отменяет корутину `computeAsync`, а отмена корутины, ожидающей
`queryAwait`, отменяет запрос в движке (см. следующий абзац). Теряется лишь
общий дедлайн и связь с родителем у наблюдателей.

**Отмена корутины, ожидающей `queryAwait` / `queryProcessAwait`, отменяет
запрос в движке** (отменяется нижележащий future), а вместе с ним и вычисление
в целевом процессе — `computeAsync` там отменяется. Наблюдатели видят, что
запрос завершился с `reason = "cancelled"`. Так что `withTimeout { engine.queryAwait(q) }`
или отмена вызывающего scope не оставляют работу в движке.

!!! warning "`runTest` и `withTimeout` вокруг вызовов движка"
    Под `runTest` из `kotlinx-coroutines-test` `withTimeout` работает на
    **виртуальном времени**: пока тестовая корутина ждёт движок — чья работа
    идёт на настоящих потоках, — виртуальное время прыгает вперёд, и таймаут
    срабатывает сразу. Внутри `runTest` оборачивайте вызовы движка в
    `withContext(Dispatchers.Default) { … }` (реальное время) или не используйте
    `withTimeout`, полагаясь на собственный `queryTimeout` движка / таймаут
    вызова.

## Изменение работающего графа

`Engine.newGraph` и `Engine.updateGraph` **блокируют** вызывающий поток:
установка или смена графа возвращается, только когда каждый добавленный или
изменённый узел запустился (дошёл до `Serving`, иначе вызов бросает исключение).
Из корутины используйте suspend-обёртки, которые выполняют этот блокирующий вызов
на `Dispatchers.IO`:

```kotlin
import io.fom.kotlin.*

engine.newGraphAwait(graph { process("Stations", ::StationsInit, ::StationsInit) })

// read-modify-write под control-lock движка — удаление не может вклиниться между ними
val changed: Boolean = engine.updateGraphAwait { current ->
    current.extend {
        process("Reports", ::ReportsInit, ::ReportsInit, dependsOn = listOf("Stations"))
            .handles<GetReport>()
    }
}

// то же самое, короче
engine.extendGraphAwait {
    process("Audit", ::AuditInit, ::AuditInit, stableDependsOn = listOf(STATIONS))
}
```

- `updateGraphAwait(change)` — это [`Engine.updateGraph`](../concepts/graph-swap.md):
  `change` получает установленный сейчас граф и возвращает граф для установки, и
  никакой другой вызов control plane (`newGraph`, `remove`,
  `pause`, …) не выполняется между ними. `change` работает под
  control-lock движка: делайте его быстрым. Возвращает то же, что `newGraph`, —
  `false`, если новый граф ничего не меняет.
- `Graph.extend { … }` возвращает граф плюс узлы и маршруты, объявленные обычным
  [DSL `graph { }`](#dsl-graph). Новые узлы могут зависеть от существующих
  (имена, `ProcessRef` или `Dependency`, реактивные или стабильные); существующие
  узлы и маршруты остаются как есть. Имя, уже присутствующее в графе, уже
  маршрутизированный тип запроса или отсутствующая зависимость дают
  `IllegalArgumentException`. Используйте его внутри `updateGraphAwait`, а не на
  графе, прочитанном раньше через `currentGraph()`: установка устаревшей копии
  возвращает процесс, удалённый тем временем.
- `extendGraphAwait { … }` — это `updateGraphAwait { it.extend { … } }`.
- **Отмена корутины не прерывает установку или смену графа.** Точнее:
    - корутина, уже отменённая в момент вызова функции `…GraphAwait`, установку
      не запускает;
    - начавшийся блокирующий вызов движка доходит до конца в своём потоке
      `Dispatchers.IO` (узлы продолжают запускаться), и корутина возобновляется
      только после этого;
    - корутина, отменённая к этому моменту, **всегда** возобновляется с
      `CancellationException` — и при успехе, и при неудаче установки. Ошибка
      (`InitializationTimeoutException`, "Engine closed while …") прикрепляется
      к нему как suppressed-исключение, поэтому отменённый `launch` завершается
      как *отменённый*, а не упавший: он не доходит до
      `CoroutineExceptionHandler` и не роняет родителя;
    - результат установки виден через движок: `engine.currentGraph()` и
      `engine.introspect()` (состояния узлов).

    Ограничивайте медленный старт бюджетом старта движка, а не `withTimeout`.

!!! danger "Никогда не вызывайте control plane из функции `change`"
    `change` выполняется в потоке `Dispatchers.IO` вызова движка, пока движок
    держит свой control-lock. Что произойдёт, зависит от потока, в котором
    сделан вызов:

    - **В том же потоке** — прямой `newGraph`, `updateGraph`, `remove`,
      `pause`, `resume` или `updateConfig`, в том числе из
      простого `runBlocking { … }`, который остаётся в вызывающем потоке, —
      вызов **отклоняется** с `IllegalStateException` («… called from inside an
      updateGraph change function; …»). Исключение выходит из
      `updateGraphAwait`, если `change` его не перехватит, и ничего не
      устанавливается.
    - **В другом потоке, которого `change` дожидается**, — `newGraphAwait` /
      `updateGraphAwait` (они перескакивают в `Dispatchers.IO`),
      `withContext(Dispatchers.IO) { … }`, другой диспетчер или executor, —
      вызов не отклоняется: он ждёт lock, который держит поток `change`, а
      `change` ждёт его. Между потоками lock не реентерабелен, поэтому движок
      **блокируется навсегда**, включая `close()`.

    `close()` из `change` тоже не вызывайте. Вычислите всё нужное *до* вызова
    `updateGraphAwait`, а последующие вызовы control plane делайте *после* его
    возврата.

    Связанный случай: `initAsync`, вызывающий control plane во время установки
    или смены графа, не создаёт вечной блокировки, но зависает — установка
    держит lock, ожидая именно этот init, — пока не истечёт бюджет init и узел
    не упадёт с `InitializationTimeoutException`. Зависший вызов при этом
    **не** отменяется вместе с init: как только установка отпустит lock, он
    всё равно выполнится — запоздалый побочный эффект (смена графа, пауза, …)
    init, который уже упал по таймауту. Не вызывайте control plane из
    `initAsync`.

## Таймауты внутри `computeAsync`

`withTimeout` бросает `TimeoutCancellationException`, который **является
`CancellationException`**. Если он вылетает из `computeAsync`, запрос завершается
этим исключением, поэтому:

- `engine.queryAwait(...)` у вызывающего пробрасывает `CancellationException`.
  Внутри `launch { }` это *молча* завершает корутину — как отменённую, а не
  упавшую, — так что никакой обработчик исключений его не увидит, а код после
  вызова просто не выполнится;
- наблюдатели считают его как `reason = "cancelled"` (в `fom-micrometer` —
  `engine_query_cancellations_total`), а не как сбой вашего процесса.

Не давайте собственному таймауту вычисления выдавать себя за отмену: используйте
`withTimeoutOrNull` и возвращайте запасное значение либо перехватывайте таймаут и
бросайте обычное исключение.

```kotlin
override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? =
    withTimeoutOrNull(500) { fetchForecast(query) } ?: Forecast.Unavailable

// или, чтобы завершить запрос ошибкой:
override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? =
    try {
        withTimeout(500) { fetchForecast(query) }
    } catch (e: TimeoutCancellationException) {
        throw ForecastTimeoutException("forecast service timed out", e)   // не CancellationException
    }
```

Дедлайн *запроса* (`queryProcessAwait(..., timeout)`, `defaultQueryTimeout`) —
другое дело: движок завершает ответ таймаутом и отменяет вычисление; сказанное
выше его не касается.

## Элементы контекста корутин (MDC, `ThreadLocal`)

Движок — это граница Java: запрос проходит через почтовый ящик и
`CompletionStage`, поэтому **контекст корутины вызывающего её не пересекает**.
Элемент MDC или `ThreadLocal`, установленный вокруг `engine.queryAwait(...)`
(через `MDCContext()` из `kotlinx-coroutines-slf4j` или
`threadLocal.asContextElement(v)`), внутри `computeAsync` *не* виден.

Виден контекст, в котором fom запускает ваш код:

- элементы из аргумента конструктора `SuspendingProcess(context)` видят все
  `computeAsync` и `cleanUpAsync` этого экземпляра;
- элементы из `initContext` / `loadContext` видят `initAsync` / `loadAsync`.

```kotlin
val service = ThreadLocal<String?>()

class Alerts : SuspendingProcess(Dispatchers.Default + service.asContextElement("alerts")) {
    override suspend fun computeAsync(ctx: QueryableContext, query: Any): Any? {
        check(service.get() == "alerts")            // здесь виден
        …
    }
}
```

Этот контекст фиксируется при создании экземпляра, поэтому годится для
статических значений (имя сервиса, тенант для экземпляра на тенанта). Значения на
каждый запрос — id запроса или трассировки — передавайте в сообщении запроса и
устанавливайте внутри вычисления:
`withContext(MDCContext(mapOf("requestId" to q.requestId))) { … }`.

fom не даёт корутинам `SuspendingProcess` **никакого `CoroutineName`**, поэтому
в дампе корутин (`DebugProbes`, окно корутин в IDE) вычисления разных узлов
неотличимы. Если нужно их различать, добавьте имя в контекст конструктора сами —
он сохраняется, как любой другой элемент:

```kotlin
class Alerts(node: String) : SuspendingProcess(Dispatchers.Default + CoroutineName("fom-$node"))
```

Контекст фиксирован на экземпляр, поэтому экземпляр, общий для нескольких
узлов, несёт одно имя на всех.

Где имя видно: `DebugProbes.dumpCoroutinesInfo()` возвращает его в
`CoroutineInfo.context` каждой корутины, и его показывает окно корутин в IDE.
**Текстовый** дамп `DebugProbes.dumpCoroutines()` печатает имя корутины, только
если JVM запущена с `-Dkotlinx.coroutines.debug` (или с `-ea`, который включает
режим отладки); без этого дамп показывает корутины безымянными, как будто имя не
задано.

## Смена Sid как `Flow` { #sid-changes-as-a-flow }

В `fom-kotlin` **нет `Flow` API**. Чтобы наблюдать новые версии процессов как
поток, сами пробросьте `EngineObserver.onSidPromotion` в `MutableSharedFlow` или
`MutableStateFlow`. Наблюдатель — четвёртый аргумент конструктора `Engine`,
поэтому сначала создайте поток, а затем подключите его при создании движка.
Колбэки выполняются на потоках движка: публикуйте через `tryEmit` / `update` и
никогда не приостанавливайтесь и не блокируйтесь в них.

```kotlin
val promotions = MutableSharedFlow<Pair<String, Sid>>(
    extraBufferCapacity = 1024, onBufferOverflow = BufferOverflow.DROP_OLDEST)
val currentSids = MutableStateFlow<Map<String, Sid>>(emptyMap())

val engine = Engine(config, backend, serDe, object : EngineObserver {
    override fun onSidPromotion(processName: String, previousSid: Sid?, newSid: Sid) {
        promotions.tryEmit(processName to newSid)
        currentSids.update { it + (processName to newSid) }
    }
})

// где-то ещё:
promotions.filter { it.first == "Stations" }.collect { (_, sid) -> refreshCache(sid) }
```

`SharedFlow` без replay пропускает повышения, случившиеся до подписки
коллектора (обычно — первые Sid при старте); `StateFlow` текущих Sid — нет.
Чтобы совместить это с другим наблюдателем (метрики, трассировка), см.
[Комбинирование наблюдателей](observability.md#комбинирование-наблюдателей).

## Настройка сборки

```kotlin
dependencies {
    implementation("io.github.altspacetg:fom-kotlin:0.1.0-SNAPSHOT")  // тянет fom-core + coroutines-core
}
```

`fom-kotlin` объявляет и `kotlinx-coroutines-core`, и `kotlinx-coroutines-jdk8` как
`api`-зависимости, поэтому обе транзитивно попадают на ваш compile classpath.

> [English version](../../guides/kotlin-dsl.md)
