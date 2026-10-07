# Наблюдаемость

Два дополняющих механизма: push-based SPI **`EngineObserver`** для событий по мере
их возникновения и pull-based **`introspect()`** для снимка на момент времени.

## Встроенное логирование жизненного цикла

Из коробки — без всякого наблюдателя — движок логирует жизненный цикл каждого
процесса на slf4j-логгере **`io.fom.fsm.ProcessFSM`** на уровне `INFO`:

```
[INFO] Process 'Stations' init started
[INFO] Process 'Stations' init completed in 3 ms (sid=2)
[INFO] Process 'Stations' load started
[INFO] Process 'Stations' load completed in 2 ms — now Serving (sid=2)
```

Повторы и сбои тоже логируются (повторы init на `INFO`, сбои на `WARN`).
Подключите любой slf4j-биндинг (Logback, slf4j-simple, …), чтобы это увидеть;
уровень логгера `io.fom.fsm.ProcessFSM` поднимайте/опускайте по вкусу.
[`EngineObserver`](#engineobserver) нужен, когда те же события нужны как
структурированные данные (метрики, спаны), а не строки лога.

Рассчитывайте примерно на **шесть строк `INFO` на каждый (re)init каждого
процесса**. Reinit (триггер, реактивный каскад) со стратегией по умолчанию
[`KEEP_OLD`](../concepts/process-lifecycle.md#переинициализация) логирует:

```
[INFO] FSM[Stations] re-init started; Sid[processName=Stations, clock=2] keeps serving (cause=…)
[INFO] Process 'Stations' init started
[INFO] Process 'Stations' init completed in 3 ms (sid=3)
[INFO] Process 'Stations' load started
[INFO] Process 'Stations' load completed in 2 ms (sid=3)
[INFO] Process 'Stations' replaced Sid[processName=Stations, clock=2] with Sid[processName=Stations, clock=3]
```

В строке load замены нет «— now Serving»: новая версия загружена, но ещё не
отвечает. Переключение отмечает строка «replaced … with …» — старая версия
отвечает на запросы до неё. Reinit с `KEEP_OLD`, который сдался, оставляет её
обслуживать и пишет только этот один `WARN` — без строки `ERROR` «FSM[X] giving
up: …», которая остаётся для узла, уходящего в `Dead`, — а за ним —
автоматический повтор
(`EngineConfig.reinitRetryBackoffMin`/`Max`; его нет, если повторы выключены или
причина постоянная):

```
[WARN] FSM[Stations] re-init gave up; keeps serving Sid[processName=Stations, clock=2]: io.fom.api.InitializationTimeoutException: …
[INFO] FSM[Stations] retrying the re-init in 31250 ms
[INFO] FSM[Stations] retrying the re-init that failed
```

См. [Если reinit не удался](../concepts/process-lifecycle.md#when-a-re-init-fails).
При `RELEASE_FIRST` первая строка — `FSM[Stations] beginning reinit cycle
(cause=…)`, а строки «replaced» нет.

Старт логирует четыре строки init/load без строк reinit, плюс одну строку на логгере
`io.fom.fsm.GraphMachine` («warm start for X from clock N» / «cold start for
X»). Тёплый старт логирует только пару строк load. Каждый повтор добавляет по
строке. При высокой частоте триггеров или большом числе процессов это быстро
накапливается. Тогда поставьте логгеру `io.fom.fsm.ProcessFSM` уровень `WARN`
(сбои останутся видны) или отфильтруйте эти сообщения в бэкенде логирования, а
жизненный цикл берите из `EngineObserver` или [метрик](#метрики--fom-micrometer).

Строка старта («warm start for X from clock N» / «cold start for X») пишется
на потоке `fom-start-<N>`, по одному на каждый узел, который запускает
`newGraph` (возобновление пишет её на потоке вызывающего), и несёт те же
[MDC-ключи](../concepts/process-lifecycle.md#наблюдаемость) `fom.engine` /
`fom.process`, что и строки диспетчера.

Вызовы control plane пишут по одной строке `INFO` на логгер `io.fom.Engine` в
вызывающем потоке (без MDC), перечисляя процессы по порядку:

```
[INFO] Engine[fom-…] pausing [Stations_PUB1, Subscriptions_PUB1]
[INFO] Engine[fom-…] resuming [Stations_PUB1, Subscriptions_PUB1]
[INFO] Engine[fom-…] removing [Stations_PUB1, Subscriptions_PUB1]
[INFO] Engine[fom-…] closing
```

`pausing` перечисляет только процессы, которые ещё не стояли на паузе, и не
пишется, если таких нет. `removing` пишется только после того, как удаление
принято: отклонённый `remove` (от удаляемого процесса зависит оставшийся или
не осталось бы ни одного процесса) ничего не логирует. Возобновление,
которое отменила параллельная операция, пишет `resuming [..] was cancelled by
another operation: …`. Успешный снапшот на `INFO` **не** логируется — только
сбои (`WARN`), а для снапшотов по расписанию — одна строка «scheduled snapshot
succeeded after N failure(s)», когда они восстановились; если нужна запись о
каждой компакции, читайте `SnapshotResult` или события `LogSnapshot` в логе.

## EngineObserver

Передайте наблюдателя четвёртым аргументом конструктора `Engine`. Движок вызывает его
на каждом событии жизненного цикла; все методы имеют пустые реализации по
умолчанию, переопределяйте только нужные.

```java
var engine = new Engine(cfg, backend, serDe, new EngineObserver() {
    @Override public void onInitCompleted(String name, Sid sid, Duration d) {
        System.out.printf("%s init за %s%n", name, d);
    }
    @Override public void onQueryFailed(String name, UUID id, String reason, Throwable cause) {
        log.warn("запрос {} на {} упал ({})", id, name, reason, cause);
    }
});
```

Колбэки: `onStateTransition`, `onInitStarted/Completed/Failed`,
`onLoadStarted/Completed/Failed`, `onQuerySent/Completed/Failed`,
`onComputeDuration`, `onDedupCollapsed`, `onSidPromotion`, `onCleanupCompleted`,
`onProcessRemoved`, `onWatcherStopped`, `onReinitStarted`, `onReinitFailed`.

**Колбэки reinit.** При `KEEP_OLD` (по умолчанию) reinit не меняет состояние
процесса, поэтому `onStateTransition` **не** приходит ни разу. Успешный reinit
сообщает по порядку:

```
onReinitStarted(name, servingSid)        // старая версия продолжает отвечать
onInitStarted → onInitCompleted(newSid)
onLoadStarted(newSid) → onLoadCompleted(newSid)
onSidPromotion(name, servingSid, newSid) // переключение
onCleanupCompleted(name, servingSid, …)  // старая версия, когда её вычисления закончились
```

`onReinitFailed(name, keptSid, cause)` приходит вместо промоушена, когда reinit
сдался: `keptSid` продолжает обслуживать, узел помечен `stale`, и reinit
повторяется через `reinitRetryBackoffMin`, если `cause` не постоянная (потерянное
лидерство, отклонённая запись, необъявленная зависимость, `cancelInit`; см.
[Исключения](../reference/exceptions.md#re-init-failures)). Сами неудачные попытки по-прежнему
приходят через `onInitFailed` / `onLoadFailed`. Рестарт, который доводит reinit из
сохранённого кандидата, вызывает `onReinitStarted` и затем только колбэки load.
При `RELEASE_FIRST` ни один из двух колбэков не приходит: reinit проходит
`Serving → CleaningUp → Initializing → Loading → Serving` с обычными переходами.

`onSidPromotion(processName, previousSid, newSid)` вызывается один раз на каждый
новый Sid. Возобновление процесса на паузе, который тёпло загружает то же
состояние, его повторно не вызывает. При перезапуске из `Dead` или замене узла
`previousSid` — это Sid, который был у процесса до того; `null` он только для
первого Sid процесса в этом движке (холодный старт, тёплая загрузка после
рестарта JVM или первый Sid после того, как процесс удалили и добавили обратно).

`onInitFailed` для отменённого init (`cancelInit`, пауза, удаление, замена графа,
`close()`) приходит один раз, с `io.fom.api.AttemptCancelledException`
(подкласс `CancellationException`), до того как процесс покинет `Initializing`.
Этот тип бросает только движок: обычный `CancellationException` из вашего
собственного кода `init` или `load` (например, таймаут внутри него) — это
обычная неудачная попытка: её повторяют, и она может закончиться `Dead`.

Каждый `onInitStarted` и каждый `onLoadStarted` получает **ровно один**
завершающий колбэк (`onInitCompleted`/`onInitFailed`,
`onLoadCompleted`/`onLoadFailed`), в том числе когда остановка гонится с
завершением попытки — например, пауза, пришедшая как раз в момент окончания
попытки, результат которой затем отбрасывается: это сообщается как
`onInitFailed`/`onLoadFailed` с `AttemptCancelledException`. Наблюдатель, который
сопоставляет начала с концами (трассировщик, строящий span'ы), никогда не
оставляет незакрытое начало. (Непарный случай — в обратную
сторону: у `onInitFailed` отброшенного reinit нет `onInitStarted` — см.
[ниже](#трассировка--fom-otel).)

**Отброшенный reinit** приходит точно так же: `onInitFailed` с попыткой `1`, хотя
узел вообще не покидает `Serving`: он продолжает обслуживать состояние, которое
уже есть и которое заведомо устарело. Это reinit с `RELEASE_FIRST`, чей
`LogDead` лог не смог сохранить. При `KEEP_OLD` о reinit, потерявшем лидерство,
сообщается один раз на обслуживающий Sid — `onInitFailed`/`onLoadFailed` его
попытки и `onReinitFailed` с `LeadershipLostException`, — а последующие запросы
для узла затем отбрасываются молча, без новых колбэков; узел остаётся stale. По
исключению видно, какой из трёх постоянных ответов дал бэкенд:

- запись **отклонена** (лог забрал другой инстанс между триггером и самим
  reinit'ом, после дедупа) — собственное `LeadershipLostException` движка
  («cannot re-initialise `<name>`: no longer the leader of the log»);
- бэкенд сам **бросил** `LeadershipLostException` (огороженный Postgres) — его
  собственное исключение и сообщение;
- бэкенд бросил `IllegalArgumentException` (событие, которое он никогда не сможет
  сохранить) — это самое `IllegalArgumentException`.

То же исключение попадает в `NodeReport.lastException`. Приходит оно **один раз на
застрявшее состояние**: движок помнит Sid, на котором сдался, поэтому повторные
триггеры против того же состояния отбрасываются молча, без нового сообщения, и
только реально случившийся reinit сбрасывает эту память. Поэтому читайте один
колбэк как «узел до дальнейших указаний обслуживает устаревшее состояние», а не
как счётчик триггеров.
См. [Переинициализацию](../concepts/process-lifecycle.md#переинициализация).

`onCleanupCompleted(processName, sid, ok, duration)` сообщает **измеренную**
длительность очистки (прежние версии всегда передавали ноль), так что
`engine_process_cleanup_duration_seconds` пишет реальные значения. `duration`
измеряет **только стадию `cleanUp`**: слив запросов «в полёте» идёт до неё,
внутри того же `cleanupTimeout`, и в неё не входит — поэтому очистка,
вышедшая за бюджет, может сообщить очень маленький `duration` вместе с
`ok=false`. `ok` равен `false`, если `cleanUp` бросил исключение или не уложился
в `cleanupTimeout`, — а у `cleanUp`, не уложившегося в этот бюджет,
отменяется stage, и это логируется с указанием процесса и бюджета («`<name>`
cleanUp did not finish within its share of `PT…`; the budget also covers draining
in-flight queries»). `ok=false` означает «вышел за бюджет», а не «остановлен».

**Очистка, вышедшая за бюджет, сообщает по бюджету — и «встроенная» тоже.**
Движок вызывает `process.cleanUp(ctx)` в отдельном потоке, и бюджет ограничивает
и сам вызов, и возвращённый им stage. Поэтому `cleanUp`, который блокируется *до*
возврата своего stage, больше не держит узел: по истечении бюджета узел идёт
дальше — reinit при `RELEASE_FIRST` продолжается, отложенные для узла запросы
обслуживает новое поколение, `close()` возвращает управление (reinit при
`KEEP_OLD` вообще не ждёт очистки старой версии: новая уже обслуживает), — и `onCleanupCompleted(ok=false)`
приходит именно тогда, по бюджету, до возврата из `close()`, с `duration` примерно
в оставшуюся долю бюджета. Само блокирующее тело при этом ничто не прерывает: оно
доработает до конца в фоне, уже после этого колбэка и после возврата `close()`,
поэтому сообщённый `duration` — не реальная его длительность. Пишите `cleanUp`
так, чтобы он рано возвращал stage и реагировал на отмену
([Жизненный цикл процесса](../concepts/process-lifecycle.md#повторы-backoff-таймауты)).

`onProcessRemoved(String processName)` вызывается, когда процесс насовсем
покидает граф (`remove` или замена графа без него), — освободите здесь
состояние, которое храните по процессу.

`onWatcherStopped(String processName, WatcherStopReason reason)` вызывается,
когда [`ScheduledWatcher` останавливает себя сам](../concepts/triggers-and-watchers.md#watchers)
и больше не тикнет. `reason` — это `io.fom.api.WatcherStopReason`, enum ровно с
тремя константами:

| Константа | Почему watcher сдался |
|---|---|
| `PROCESS_REMOVED` | его процесса больше нет в графе; возврат процесса watcher не оживляет |
| `EXECUTOR_SHUT_DOWN` | `Executor`, переданный вызывающим, остановлен, поэтому проверку уже никогда не выполнить |
| `LEADERSHIP_LOST` | его триггер не удалось записать: этот инстанс больше не лидер лога |

Набор намеренно закрытый, поэтому reason безопасно использовать как тег метрики
или в `switch`; подробности (какой процесс, какое исключение) — в `WARN`, который
движок пишет рядом. Watcher, который закрыл сам
вызывающий (`close()` на ручке от `watch` или `Engine.close()`), **не**
сообщается: алерт на этот колбэк — это алерт только на то, что движок сдался с
опросом, который вы всё ещё ждёте.

`onQuerySent` вызывается в потоке, который отправляет запрос, и у каждого
отправленного запроса затем ровно один `onQueryCompleted` или `onQueryFailed`,
когда завершается ответ. `reason` у `onQueryFailed`: `timeout` (истёк дедлайн
вызывающего — запрос, отправленный изнутри `compute` через `ctx.query` или
внешний `Engine`, наследует дедлайн вычисляемого запроса),
`rejected` (запрос, который движок уже принял, затем отклонён — процесс дошёл до
`Dead`, останавливается либо был поставлен на паузу или удалён, пока запрос его
ждал, — или закрывается движок), `init-in-progress`, `cancelled` или
`exception` (`compute` бросил исключение); подробности — в `cause`. Причина
**определяется по типу исключения**, которым завершился ответ, а не по тому,
кто его бросил: `TimeoutException` даёт `timeout`, `QueryRejectedException` —
`rejected`, `InitInProgressException` — `init-in-progress`,
`CancellationException` — `cancelled`, всё остальное — `exception`. Поэтому
`compute`, упавший со **своим собственным** `TimeoutException` (скажем, по
таймауту HTTP-клиента), отчитывается как `timeout` — так же, как пропущенный
дедлайн запроса, — а бросивший `CancellationException` — как `cancelled`.
Если их нужно различать, оборачивайте такие ошибки в своё исключение. Запросы,
отклонённые **до** того, как они дошли до процесса, не вызывают никаких хуков:
запрос к процессу, который уже неизвестен или стоит на паузе, и запрос к
**закрытому движку** (`query` / `queryProcess` сразу возвращают
`IllegalStateException: Engine <id> is closed`, не дав `GraphMachine` вызвать
`onQuerySent`). Ни один из них не попадает ни в `rejected`, ни в любую метрику,
построенную на этих колбэках (см. [оговорку](#метрики--fom-micrometer) ниже).

!!! note "Учёт запросов согласован лишь в конечном счёте"
    `onQueryCompleted` / `onQueryFailed` подписаны на тот самый future, который
    держит вызывающий, поэтому они могут быть ещё не выполнены, когда `get()`
    вызывающего уже вернулся. Инвариант вида `sent == completed + failed` поэтому
    выполняется *в конечном счёте*, а не в тот же миг, когда запрос вернул
    результат: в тесте дожидайтесь его (Awaitility), а не проверяйте сразу после
    запроса. Оба выполняются на виртуальном потоке движка, а не в потоке
    вызывающего, поэтому медленный observer не растягивает его `get()` с таймаутом.

Сигнатура: `onQuerySent(String processName, UUID queryId, Class<?> messageType,
UUID parentQueryId)`. Запрос к зависимости выполняется на воркере вычисления, а
не в потоке, отправившем внешний запрос, поэтому `parentQueryId` указывает
запрос, чей `compute` отправил его, — через `ctx.query` или через
`Engine.query`/`queryProcess`, вызванный, пока этот `compute` выполняется, — по
нему их можно вложить. Для `Engine.query`/`queryProcess`, вызванного вне
вычисления, и для запросов из `init`/`load` он равен `null`. Дочерний запрос,
чей родительский запрос упал, отменяется (`reason` `cancelled`).

### Остановка процесса из колбэка { #stopping-from-a-callback }

Не вызывайте синхронно метод control plane, который **останавливает процесс**,
— `pause`, `remove`, `TenantAwareEngine.pauseTenant` /
`removeTenant`, `newGraph` / `updateGraph`, который его убирает или
переопределяет, или `close()` — из колбэка об **этом же процессе**. Такой
колбэк может выполняться на собственном диспетчере процесса, а остановка — это
сообщение этому диспетчеру: вызов ждал бы тот самый поток, который он блокирует.

Движок это распознаёт и сразу отказывает: если вызывающий поток — диспетчер
процесса, который вызов остановил бы, вызов бросает `IllegalStateException` до
взятия управляющей блокировки и ничего не меняя — например, «pause('X')
called on that process's own dispatcher (from an EngineObserver callback about
it); call it from another thread» или «a graph change that removes or redefines 'X' was called on that
process's own dispatcher …» для `newGraph`/`updateGraph` (проверяется по
установленному графу и ещё раз под блокировкой — по графу, к которому
применяется изменение). Пауза, удаление или изменение графа, затрагивающие
только **другие** процессы, из колбэка по-прежнему разрешены, если в этот момент
не идёт другой вызов control plane (см. ниже). `close()` на
диспетчере бросает так же («close() called on the dispatcher of 'X' …») — кроме
случая, когда движок **уже закрывается**: тогда он просто возвращается, ему
нечего делать. (До этой проверки такой вызов висел весь `cleanupTimeout`,
держа управляющую блокировку движка.)

Второе правило касается **любого** вызова control plane — `newGraph`,
`updateGraph`, `updateConfig`, `remove`, `pause`, `resume`, `resumeUnblocked`, —
сделанного из колбэка на диспетчере процесса, пока управляющую блокировку движка
держит **другой** вызов control plane: например, первый `newGraph`, который
держит её, пока ждёт, что все узлы дойдут до `Serving`. Тот вызов может ждать
именно этот диспетчер, поэтому движок не блокируется: вызов сразу бросает
`IllegalStateException` — «pause(...) called from an EngineObserver callback
while another control-plane call is in progress; call it from another thread»
(с именем вызова). Раньше такой вызов блокировался: диспетчер висел весь бюджет
старта, и первый `newGraph` падал. Если другого вызова control plane нет,
вызовы, затрагивающие только **другие** процессы, из колбэка работают. В потоке,
который не является диспетчером процесса, вызовы control plane, как и прежде,
ждут блокировку.

В обоих случаях исключение вылетает из вашего колбэка, а движок логирует и
проглатывает его, как любой сбой колбэка, — так что вызов молча не выполняется.
Передавайте вызовы control plane другому потоку (`Thread.startVirtualThread(...)`
или `Thread.ofVirtual().start(...)`):

```java
@Override public void onInitFailed(String name, int attempt, Throwable cause) {
    if (attempt >= 3) {
        Thread.ofVirtual().start(() -> engine.pause(Set.of(name)));
    }
}
```

### Запросы, ждущие узел { #queries-that-wait-for-a-node }

Запрос к узлу графа, у которого прямо сейчас **нет работающего FSM**, — узел ещё
стартует (`Starting` в `introspect()`) или находится между старым и новым FSM
[замены графа](../concepts/graph-swap.md), — тоже попадает к наблюдателям, ровно
один раз: `onQuerySent` в момент, когда запрос начинает ждать, затем
`onQueryCompleted`, когда узел его обслужит, либо `onQueryFailed` с `reason`
`timeout` (дедлайн истёк раньше, чем узел стартовал) или `rejected` (узел
удалили или поставили на паузу либо движок закрылся). FSM, который в итоге
обслужит запрос, подхватывает его с тем же `queryId`, так что дважды он не
учитывается.

Обратите внимание на тайминг: спан и таймер задержки стартуют в начале
**ожидания**, а не когда процесс получил запрос, поэтому такой запрос измеряет
ожидание в очереди плюс вычисление. В Micrometer и OpenTelemetry время старта
узла, таким образом, попадает в задержку запроса.

!!! warning "Наблюдатели должны быть быстрыми и неблокирующими"
    Колбэки выполняются на потоках движка — кроме `onQuerySent`, который
    выполняется в потоке, отправляющем запрос (для `Engine.query` — в потоке
    самого вызывающего). Движок оборачивает каждый вызов, чтобы
    исключение не сломало FSM, — исключение из `onSidPromotion` или
    `onDedupCollapsed` тоже не останавливает каскад и не теряет reinit, — но
    *медленный* наблюдатель всё равно замедляет движок. Тяжёлую работу выносите.
    Колбэк, который продолжает бросать исключения, логируется на `WARN` только
    при 1-м, 2-м, 4-м, 8-м… сбое, в остальных случаях — на `DEBUG`. Счётчик
    ведётся по колбэку: для колбэков жизненного цикла и запросов (`onInit*`,
    `onLoad*`, `onQuery*`, …) — в каждом процессе отдельно, для колбэков графа
    (`onSidPromotion`, `onDedupCollapsed`, `onProcessRemoved`) — на весь движок.
    [Композит](#комбинирование-наблюдателей) считает сбои своих делегатов сам —
    по колбэку на весь движок.

## Метрики — `fom-micrometer`

`MicrometerEngineObserver` адаптирует SPI наблюдателя к Micrometer
`MeterRegistry`:

```java
import io.fom.micrometer.MicrometerEngineObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

var registry = new SimpleMeterRegistry();
var engine = new Engine(cfg, backend, serDe,
    new MicrometerEngineObserver(registry));
```

Регистрируемые метрики, все с тегом `name=<процесс>`, кроме
`engine_watcher_stops_total` (сбои запросов также с тегом `reason`):

| Метрика | Тип |
|---|---|
| `engine_process_init_duration_seconds` | timer |
| `engine_process_load_duration_seconds` | timer |
| `engine_query_duration_seconds` | timer |
| `engine_process_compute_duration_seconds` | timer |
| `engine_process_cleanup_duration_seconds` | timer |
| `engine_process_reinit_duration_seconds` | timer (`onReinitStarted` → промоушен) |
| `engine_query_failures_total` `{reason}` | counter |
| `engine_query_cancellations_total` | counter |
| `engine_process_init_failures_total` | counter (без отмен и потери лидерства) |
| `engine_process_load_failures_total` | counter (без отмен и потери лидерства) |
| `engine_process_leadership_lost_total` | counter (попытки init/load, отклонённые, потому что узел потерял лидерство) |
| `engine_process_init_cancellations_total` | counter |
| `engine_process_load_cancellations_total` | counter |
| `engine_process_cleanup_failures_total` | counter |
| `engine_process_dead` | gauge (`1`, пока процесс Dead после сбоя, иначе `0`; `0` после паузы) |
| `engine_process_reinit_failures_total` | counter (reinit'ы, которые сдались, включая постоянные причины) |
| `engine_process_stale` | gauge (`1` от неудавшегося reinit до следующего промоушена Sid, иначе `0`) |
| `engine_dedup_collapsed_total` | counter (растёт на **число свёрнутых триггеров**) |
| `engine_watcher_stops_total` `{reason}` (без тега `name`) | counter |

Три метрики reinit следуют колбэкам `KEEP_OLD`. `engine_process_reinit_failures_total`
растёт на один с каждым `onReinitFailed`; `engine_process_stale` равен `1` с этого
момента до следующего `onSidPromotion` — узел отвечает версией, которую не
удалось заменить (свёрнутый gauge равен `1`, если устарело любое из свёрнутых
имён); `engine_process_reinit_duration_seconds` пишет время от `onReinitStarted`
до промоушена. Reinit, который не удался или который прервали пауза, удаление
или `close()`, времени не пишет. Reinit при `RELEASE_FIRST` не вызывает ни одного
из двух колбэков и эти три метрики не трогает.

`engine_query_failures_total` считает только сбои запросов, с тегом `reason`
движка (`timeout`, `rejected`, `init-in-progress`, `cancelled`, `exception`).
`engine_watcher_stops_total` считает watcher'ы, остановившиеся сами
(`onWatcherStopped`), с тегом **только** `reason` — `process_removed`,
`executor_shut_down` или `leadership_lost`, то есть
[`WatcherStopReason`](#engineobserver) в нижнем регистре, — и намеренно *без*
`name`: чаще всего причина в том, что процесс удалили, поэтому тег `name` заново
создал бы те самые per-process метрики, которые удаляет `onProcessRemoved`, а
имена процессов бывают «по тенанту» и неограниченными. Процесс назван в `WARN`
движка рядом со счётчиком.
`engine_query_cancellations_total` считает сбои запросов с `reason` `cancelled`
или с причиной `CancellationException`. `engine_query_duration_seconds` измеряется
от отправки до ответа; только вычисление — `engine_process_compute_duration_seconds`.
`engine_dedup_collapsed_total` считает **свёрнутые триггеры, а не события
свёртки**: `onDedupCollapsed(name, n)` увеличивает его на `n`, поэтому шесть
триггеров, попавших в одно окно дедупликации, добавляют `6.0` — один reinit,
поглотивший шесть запросов, а не шесть свёрток. Делите на число reinit'ов, если
нужен средний «веер».

`engine_process_compute_duration_seconds` — и стоящий за ней колбэк
`onComputeDuration` — измеряет `CompletionStage`, который вернул ваш `compute`:
от вызова до того, как этот stage завершится. Движок **отменяет** этот stage,
когда истекает дедлайн вызывающего, а отменённый `CompletableFuture`
завершается сразу же, поэтому вычисление, вышедшее за таймаут запроса,
записывается один раз со временем до *отмены*, а не со временем, которое
реально отработало его тело. Поэтому гистограмма обрезана по таймауту запроса:
она показывает, сколько заняли вычисления **в пределах** дедлайна, и
недооценивает каждое, которое за него вышло. Тело, игнорирующее отмену,
продолжает работать и после этого, и это дополнительное время не попадает ни в
одну метрику — чтобы видеть настоящую стоимость медленных вычислений,
инструментируйте само тело `compute`, а по
`engine_query_failures_total{reason="timeout"}` смотрите, как часто дедлайн
обрезал измерение. Движок вызывает `onComputeDuration` только **после** того,
как завершил ответ на запрос, и на виртуальном потоке движка — не в потоке,
завершившем stage вычисления, — поэтому медленный наблюдатель никогда не
задерживает ответ (и не превращает вычисление, ответившее вовремя, в таймаут) и
другие вычисления. Вычисление, которое нельзя было остановить, когда его запрос
упал (ещё блокировано внутри `compute()` или его stage игнорирует `cancel()`),
брошено и в `onComputeDuration` не попадает вовсе, даже если позже завершится.

!!! warning "`rejected` не считает самое частое отклонение"
    Запрос, адресованный процессу **на паузе или неизвестному**, — как и любой
    запрос к **закрытому движку** — отклоняется синхронно (в графе или ещё в
    самом `Engine`, до графа), ещё до того как в дело вступит FSM процесса,
    поэтому ни `onQuerySent`, ни `onQueryFailed` не вызываются, и
    `engine_query_failures_total{reason="rejected"}` не меняется — хотя каждый
    вызывающий получает `QueryRejectedException`. Замерено: 100 запросов к узлу
    на паузе → 100 `QueryRejectedException` у вызывающих, счётчики наблюдателя
    без изменений (`sent 1→1, failed 0→0`), счётчик `0`. Не стройте алерт «у
    вызывающих всё падает» только на этом счётчике: дополните его `introspect()`
    (узел в `state=Paused` или имя, которого в отчёте нет) или собственной
    частотой ошибок на стороне вызова. Под `rejected` *попадают* отклонения
    запросов, которые движок уже принял: запрос, припаркованный для узла,
    который затем поставили на паузу или удалили; запрос к процессу, дошедшему
    до `Dead` или останавливающемуся; и запрос, отклонённый потому, что
    закрывается сам движок.

Сбои init, load и cleanup учитываются в отдельных счётчиках и не попадают в
сбои запросов.

**Отмена — не сбой.** Когда движок сам обрывает попытку init или load — процесс
ставят на паузу, удаляют, заменяют при смене графа, движок закрывают или
вызывают `cancelInit`, — он сообщает о попытке через `onInitFailed` /
`onLoadFailed` с `AttemptCancelledException` (чтобы трассировщик мог закрыть
спан попытки). Наблюдатель считает такие попытки в
`engine_process_init_cancellations_total` / `engine_process_load_cancellations_total`
и **не** считает в счётчиках `*_failures_total`, так что алерт «сбои init» не
срабатывает на паузу или остановку. Обычный `CancellationException`, брошенный
вашим собственным `init`/`load`, — это **сбой**: он учитывается в
`*_failures_total`, и процесс, который продолжает так падать, в итоге получает
`engine_process_dead` = `1`. (В отличие от
`engine_query_cancellations_total`, который является подмножеством
`engine_query_failures_total`.)

**Потеря лидерства — тоже не сбой процесса.** Попытка init или load, упавшая
потому, что движок больше не лидер своего лога, — `LeadershipLostException`
где угодно в цепочке причин: отфенсенный или смещённый узел, включая
единственный `onInitFailed` отброшенного re-init, — учитывается в
`engine_process_leadership_lost_total{name}` и **не** учитывается в
`engine_process_init_failures_total` / `engine_process_load_failures_total`,
так что алерт «сбои init» не срабатывает на каждый failover. Это событие узла,
а не сломанный `init`: алертите по нему (или по `isLeader`, см. ниже) отдельно.

**`engine_process_dead`** равен `1`, пока процесс в `Dead` из-за сбоя — init
исчерпал бюджет, load сдался, потеряно лидерство, запрошена необъявленная
зависимость, — и `0` в остальных случаях. Процесс, попавший в `Dead` потому, что
его *остановили* (пауза, удаление, смена графа, закрытие, `cancelInit`),
показывает `0`. Gauge регистрируется, как только процесс впервые замечен,
возвращается в `0`, когда процесс ставят на **паузу** (в том числе процесс,
бывший в `Dead` после сбоя, — `introspect()` тогда показывает `Paused`) и когда
имя запускается снова (resume, повторное добавление, `newGraph` с перезапуском),
и удаляется вместе с остальными метриками процесса при его удалении.
`Engine.close()` его не сбрасывает: после закрытия gauge хранит последнее
значение, так что процесс, бывший в `Dead` в момент закрытия движка, в реестре,
который живёт дольше движка, по-прежнему показывает `1` — пока **новый движок**
с новым наблюдателем на том же реестре и с теми же общими тегами снова не сообщит
об этом процессе: с этого момента gauge следует за новым движком (его значение
хранится в общем для реестра держателе, а не в наблюдателе, который первым
зарегистрировал gauge и к которому Micrometer иначе оставил бы его привязанным;
новый наблюдатель находит существующий gauge, а не регистрирует его повторно,
поэтому Micrometer не пишет предупреждение «Gauge has been already registered»). Счётчики сбоев перестают расти, как только процесс в `Dead`, —
его никто не перезапускает, — так что алерт по их скорости затихает ровно тогда,
когда процесс окончательно лёг; алертите по gauge. Полное состояние каждого
процесса даёт `Engine.introspect()`; наблюдатель видит только события, поэтому
gauge размера почтового ящика или общего состояния у него нет.

**Счётчики жизненного цикла начинаются с `0`.** `engine_process_init_failures_total`,
`engine_process_load_failures_total`, оба `*_cancellations_total`,
`engine_process_leadership_lost_total`, `engine_process_cleanup_failures_total`,
`engine_process_reinit_failures_total`, `engine_dedup_collapsed_total` и gauge
`engine_process_dead` и `engine_process_stale` регистрируются, как только процесс впервые замечен (его
первый переход состояния или начало попытки), ещё до того, как что-либо
посчитано. Это важно для `increase()`: Prometheus нужен сэмпл *до* инкремента, а
счётчик, чьё первое собранное значение уже `1`, не показывает прироста вовсе —
разовый сбой никогда не сработал бы в рецепте ниже. `engine_watcher_stops_total`
не привязан к процессу, поэтому он инициализируется один раз: по серии на каждый
`reason` (`process_removed`, `executor_shut_down`, `leadership_lost`, с общими
тегами наблюдателя) регистрируется с `0` при создании наблюдателя, ещё до его
подключения к движку.

Рецепт алертов (PromQL):

```promql
# Процесс лёг и сам не поднимется
max by (name) (engine_process_dead) == 1

# Init/load продолжают падать (идут повторы) — отмены уже исключены
sum by (name) (increase(engine_process_init_failures_total[10m])) > 0
sum by (name) (increase(engine_process_load_failures_total[10m])) > 0

# Узел отвечает версией, которую не удалось заменить (reinit повторится или ждёт триггера)
max by (name) (engine_process_stale) == 1

# cleanUp упал или вышел по таймауту (ресурсы могли утечь)
sum by (name) (increase(engine_process_cleanup_failures_total[1h])) > 0

# Узел потерял лидерство лога (failover, фенсинг) — алерт на узел, а не на процесс
sum by (instance) (increase(engine_process_leadership_lost_total[10m])) > 0

# Watcher остановился не из-за удаления процесса (срабатывает уже на первую остановку)
sum by (instance, reason) (increase(engine_watcher_stops_total{reason!="process_removed"}[10m])) > 0
```

!!! warning "HA: у отступившего узла `engine_process_dead` хранит своё значение"
    После failover движок бывшего лидера отфенсен или закрыт, и — как описано
    выше — его серии `engine_process_dead` хранят **последнее значение**
    (процесс, умерший из-за потери лидерства, показывает `1`; обслуживавший —
    `0` и больше не обновляется). Просуммированные или взятые через `max` по
    всему кластеру, такие устаревшие серии выглядят как лёгший живой процесс
    или прячут действительно лёгший. Ограничьте область алертов: либо алертите
    по `instance` (цель скрейпа или общий тег `engine`/узла у наблюдателя) и
    только на узле-лидере — экспортируйте `engine.introspect().isLeader()` как
    собственный gauge — `introspect()` возвращает `CompletionStage<EngineReport>`
    и бросает исключение, когда движок закрыт, поэтому разверните его и
    обработайте закрытый случай:

    ```java
    Gauge.builder("engine_is_leader", engine, e -> {
        try {
            return e.introspect().toCompletableFuture().join().isLeader() ? 1 : 0;
        } catch (RuntimeException closedOrFailed) {   // закрытый движок бросает IllegalStateException
            return Double.NaN;                          // или 0, если «закрыт» = «не лидер»
        }
    }).register(registry);
    ```

    и соединяйте по нему:
    `max by (name) (engine_process_dead * on (instance) group_left engine_is_leader) == 1`, —
    либо уберите серии старого узла (удалите процесс или перезапустите его со
    свежим реестром), когда он отступил. Для «лидера нет ни на одном узле»
    алертите по самому `engine_is_leader`.

    С логами та же проблема области: строки диспетчера несут
    [MDC-ключи](../concepts/process-lifecycle.md#наблюдаемость) `fom.engine` /
    `fom.process`, но они выставляются только в потоке диспетчера процесса (и
    на строке «warm/cold start for X»).
    Строки `WARN`/`ERROR` **уровня движка** — которые пишут `io.fom.Engine` или
    `io.fom.fsm.GraphMachine` в вызывающем потоке или потоке планировщика
    (например, отказ смены графа или записи control plane после потери
    лидерства), — находятся вне MDC диспетчера и `fom.engine` не несут.
    Различайте узлы для них по хосту/инстансу (метки вашего сборщика логов) и
    имени логгера, а не по `fom.engine`.

!!! warning "Метрики запросов по-прежнему регистрируются лениво"
    `engine_query_failures_total{reason}`, `engine_query_cancellations_total` и
    все таймеры регистрируются при **первой** записи (серия с данным `reason`
    появляется только после первого сбоя запроса с этой причиной), поэтому их
    первое собранное значение уже включает это событие, и `increase()` /
    `rate()` по ним пропускают первое событие каждой серии. Для алерта «запросы
    падают» используйте долю или устойчивую скорость, а не «любой прирост», либо
    сравнение с отсутствием:
    `engine_query_failures_total > 0 unless engine_query_failures_total offset 10m`
    срабатывает в течение десяти минут после появления новой серии — то есть на
    первый сбой с данным `reason`, который `increase()` не видит.

!!! note "Нет gauge лидерства и метрики застрявшей записи в лог"
    Инстанс, потерявший лидерство, продолжает обслуживать своё состояние:
    отклонённый re-init даёт один `onInitFailed` (один раз учтённый в
    `engine_process_leadership_lost_total`), и всё — метрики `isLeader` нет. При
    `RELEASE_FIRST` re-init, чья запись `LogDead` раз за разом падает,
    повторяется молча, если не считать строк `ERROR` (при `KEEP_OLD` сбой записи —
    это неудачная попытка init, и кончается он `onReinitFailed`). Чтобы получать по ним алерты, опрашивайте
    `engine.introspect()` и экспортируйте `isLeader` и `lastException` каждого узла
    как собственные gauge, либо алертите по строкам `ERROR` в логе.

Когда процесс насовсем покидает граф
(`onProcessRemoved`), все метрики, которые *этот наблюдатель* зарегистрировал
для этого имени, удаляются из реестра, поэтому динамические имена процессов не
накапливают метрики (метрики, зарегистрированные кем-то ещё — в том числе
наблюдателем другого движка, — остаются на месте). Запоздалые колбэки
удалённого процесса (ещё завершающиеся запросы) их заново не создают; если
процесс с тем же именем добавят обратно, запись метрик возобновится при его
старте. Метрика, которую после свёртки `MeterFilter`-ом делят несколько имён
процессов (см. [кардинальность](#cardinality-and-folding-process-names)),
удаляется только вместе с последним из этих имён. Чтобы отличить запоздалый колбэк от живого, наблюдатель помнит имена
удалённых процессов, но ограниченно: имя забывается через 15 минут после
удаления, и хранится не больше 10 000 последних удалений (самые старые
вытесняются первыми), так что имена в духе тенантов, которые удалили и больше не
добавляли, занимают ограниченную память. Запоздалые колбэки отстают от удаления
не больше чем на таймауты запроса/cleanup — движок сообщает об удалении только
после того, как узел остановлен и его припаркованные запросы завершены с
ошибкой, — так что они с запасом попадают в это окно. Подключайте реестр
к Prometheus/OTLP/и т. д. как обычно для Micrometer.

### Перцентили и гистограммы { #histograms }

Наблюдатель регистрирует таймеры **без** перцентилей и гистограммы, поэтому по
умолчанию они экспортируют только `_count`, `_sum` и `_max` — этого хватает на
rate и среднее, но не на p99. Включите гистограммы через `MeterFilter` на
реестре, настроенный **до** запуска движка (фильтры применяются при регистрации
метра):

```java
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

registry.config().meterFilter(new MeterFilter() {
    @Override
    public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
        if (!id.getName().startsWith("engine_")) return config;
        return DistributionStatisticConfig.builder()
                .percentilesHistogram(true)
                .build()
                .merge(config);
    }
});
```

Тогда в Prometheus появляются серии `engine_query_duration_seconds_bucket{le=…}`,
и панель p99 по процессам строится так:

```promql
histogram_quantile(0.99,
  sum by (le, name) (rate(engine_query_duration_seconds_bucket[5m])))
```

Бакеты умножают число серий — на несколько десятков на таймер на имя процесса,
— поэтому см. [Кардинальность](#cardinality-and-folding-process-names) и на
большом графе сузьте фильтр до таймеров, которые вы рисуете (например, только
`engine_query_duration_seconds`).

### Несколько движков, один реестр

Давайте **каждому движку собственный экземпляр наблюдателя**. Экземпляр помнит,
какие имена процессов были удалены, и игнорирует их запоздалые колбэки, поэтому
один экземпляр на два движка заглушит `Subscriptions` движка B, как только движок A
удалит свой процесс `Subscriptions`.

Если эти движки пишут в один `MeterRegistry`, дайте каждому наблюдателю ещё и
различающий тег. Конструктор `(registry, Iterable<Tag>)` добавляет общие теги ко
всем метрикам, которые регистрирует наблюдатель:

```java
import io.micrometer.core.instrument.Tags;

var stations = new Engine(cfg, stationsLog, serDe, new MicrometerEngineObserver(registry, Tags.of("engine", "stations")));
var alerts   = new Engine(cfg, alertsLog,   serDe, new MicrometerEngineObserver(registry, Tags.of("engine", "alerts")));
```

Без такого тега метрики обоих движков для процесса с одинаковым именем — это одна
и та же метрика (их значения сливаются), а удаление этого процесса в одном движке
убрало бы метрику, в которую продолжает писать другой. Ключи `name` и `reason`
принадлежат самому наблюдателю и как общие теги отклоняются. Два **работающих**
движка с одинаковыми общими тегами на одном реестре не поддерживаются: помимо
слияния значений, `engine_process_dead` следует за тем наблюдателем, который
последним видел процесс. Закрыть движок и построить новый с теми же тегами —
можно, см. `engine_process_dead` выше.

### Кардинальность и свёртка имён процессов { #cardinality-and-folding-process-names }

Каждое имя процесса — это значение тега `name`, поэтому оно стоит собственного
набора метрик: **10**, как только процесс впервые замечен (восемь счётчиков
жизненного цикла и gauge `engine_process_dead` и `engine_process_stale`), **16**,
когда каждый из шести таймеров что-то записал, и до **22** с `engine_query_cancellations_total` и
`engine_query_failures_total` по каждой из пяти причин — а каждый таймер в
Prometheus сам по себе несколько серий (`_count`, `_sum`, `_max` и бакеты, если
включены гистограммы). С именами в духе тенантов (`t1/subscriptions`, `t2/subscriptions`, …)
это умножается на число тенантов.

Чтобы это ограничить, сворачивайте имена `MeterFilter`-ом на реестре и
настраивайте его **до** старта движка (Micrometer применяет фильтры при
регистрации):

```java
import io.micrometer.core.instrument.config.MeterFilter;

// t1/subscriptions, t2/subscriptions, ... пишут в name="any/subscriptions"
registry.config().meterFilter(
        MeterFilter.replaceTagValues("name", n -> n.replaceFirst("^[^/]+/", "any/")));
```

Наблюдатель с таким фильтром безопасен: для каждой зарегистрированной (после
фильтра) метрики он считает, сколько его имён процессов на неё отображается, и
`onProcessRemoved` удаляет метрику, только когда её не использует ни один
оставшийся процесс, — удаление тенанта `t1` оставляет `any/subscriptions` на месте для
`t2`. Свёрнутый `engine_process_dead` равен `1`, пока **хотя бы один** из
свёрнутых процессов в `Dead` после сбоя; он регистрируется один раз, для первого
имени, а каждое следующее имя, которое фильтры реестра отображают на него,
присоединяется к нему, а не регистрирует его заново (поэтому Micrometer не пишет
предупреждение "Gauge has been already registered"). Счётчики и тайминги свёрнутых процессов
сливаются — в этом и смысл; подробности по отдельным именам берите из логов или
`introspect()`.

!!! note "У таймеров fom нет exemplar-ов"
    Таймеры записываются не в потоке вызывающего: тайминги запросов — в
    виртуальном потоке движка `fom-observer`, тайминги init/load/cleanup — в
    собственных потоках процесса, — поэтому при записи никакой контекст
    трассировки (спан OpenTelemetry или Micrometer Tracing) не текущий. Реестры,
    прикрепляющие exemplar-ы (Prometheus с `SpanContext`), к гистограммам fom их
    **не** прикрепляют. Чтобы перейти от медленного запроса к его трассе,
    используйте спаны `fom.query` из [`fom-otel`](#трассировка--fom-otel) и их
    длительность.

## Трассировка — `fom-otel`

`OtelEngineObserver` эмитит спаны OpenTelemetry для `init`, `load`, `query` и
reinit при `KEEP_OLD`.
Спан `fom.query` — дочерний к текущему спану вызывающего (он стартует в потоке
отправки) и завершается, когда завершается ответ — в том числе по таймауту на
стороне вызывающего, — так что открытых спанов не остаётся. Запрос к зависимости,
отправленный из `compute` через `ctx.query`, получает спан `fom.query`, дочерний к
спану обслуживаемого им запроса (сопоставляется по `parentQueryId`), поэтому
весь fan-out остаётся в трассе вызывающего, а не порождает осиротевшие корневые
спаны. Это верно, даже если запрос к зависимости отправлен **после** того, как
обслуживаемый запрос завершился — из асинхронного продолжения `compute`, — при
условии, что он отправлен в течение 5 минут: наблюдатель помнит контекст
завершённых спанов запросов столько времени (не более 10 000 последних), так что
поздний дочерний запрос всё равно вкладывается в родителя. Запрос, отправленный
позже, начинает новую трассу. Однако контекст OTel внутрь `compute` **не** передаётся: внутри `compute`
`Span.current()` — это не спан `fom.query`, поэтому спан, который вы создаёте
там сами, начинает новую трассу, а не вкладывается в запрос. Когда процесс покидает граф (`onProcessRemoved`), наблюдатель забывает
время старта init/load, которое ещё держал для него, так что удаление процессов
посреди попытки ничего не оставляет; отдельного спана при этом нет — прерванную
попытку движок сообщает как `onInitFailed` / `onLoadFailed`, и она уже даёт
спан со статусом `ERROR`:

```java
import io.fom.otel.OtelEngineObserver;

var engine = new Engine(cfg, backend, serDe,
    new OtelEngineObserver(openTelemetry));            // или new OtelEngineObserver(tracer)
```

| Спан | Когда | Атрибуты |
|---|---|---|
| `fom.init` | один на завершённый init (статус не задан) и один на каждую неудачную попытку (`ERROR`, с записанным исключением) | `process.name`; `sid.clock` (завершённый); `init.attempt` (неудачная) |
| `fom.load` | то же для `load` | `process.name`; `sid.clock` (завершённый, а при неудаче — если Sid известен); `load.attempt` (неудачная) |
| `fom.reinit` | один на reinit при `KEEP_OLD`, от `onReinitStarted` до промоушена (статус не задан) или до `onReinitFailed` (`ERROR`, с записанным исключением); нет спана, если reinit прервали пауза, удаление или `close()` | `process.name`, `sid.serving.clock`; `sid.new.clock` (при успехе) |
| `fom.query` | один на запрос, от отправки до завершения ответа; `ERROR` с причиной сбоя в описании статуса | `process.name`, `query.id` (UUID строкой), `query.type` (имя класса сообщения) |

Все спаны — `SpanKind.INTERNAL`. Спаны init, load и reinit строятся задним числом
по завершающему колбэку (время старта — из `onInitStarted` / `onLoadStarted` /
`onReinitStarted`),
поэтому, пока попытка идёт, открытого спана нет.

!!! note "Отброшенный reinit виден как ERROR-спан `fom.init` почти нулевой длительности"
    **Отброшенный reinit** (`RELEASE_FIRST`) приходит как `onInitFailed` без парного
    `onInitStarted` ([выше](#engineobserver)), поэтому у наблюдателя нет времени
    старта для спана. Отсутствие старта он переживает без исключения, но спан,
    который он эмитит, начинается в момент сбоя: оператор видит спан `fom.init`
    со статусом `ERROR`, `init.attempt = 1`, почти нулевой длительностью и либо
    `LeadershipLostException` (собственное «no longer the leader of the log»
    движка или сообщение огороженного бэкенда), либо `IllegalArgumentException`
    от бэкенда, который не может сохранить `LogDead` (`RELEASE_FIRST`), — для процесса,
    который никакой init и не начинал. Такой спан приходит один раз на застрявшее
    состояние, а не на каждый триггер. Читайте это как «reinit отброшен, а узел
    продолжил обслуживать устаревшее состояние», а **не** как «init выполнился и
    упал»: процесс остаётся в `Serving` и продолжает отвечать на запросы тем
    состоянием, которое у него уже было. Это сигнал уводить трафик с этого узла
    (его `isLeader` равен `false`), а не искать медленный или сломанный `init`.

## Комбинирование наблюдателей

Движок принимает ровно одного наблюдателя, поэтому для метрик *и* трассировки
*и* собственного логирования одновременно используйте встроенный композит:

```java
import io.fom.api.EngineObserver;

var engine = new Engine(cfg, backend, serDe,
    EngineObserver.composite(
        new MicrometerEngineObserver(registry),
        new OtelEngineObserver(openTelemetry.getTracer("io.fom")),
        myAuditObserver));
```

Каждый колбэк передаётся всем делегатам по порядку. Делегат, бросивший
исключение, не останавливает остальных: ошибка логируется (WARN на 1-м, 2-м,
4-м… сбое колбэка, DEBUG между ними) — так же, как движок защищается от
одиночного наблюдателя. Считает композит сам, по одному счётчику на колбэк на
весь движок, — а не по процессу, как движок считает колбэки жизненного цикла и
запросов. `composite()` без аргументов возвращает
`EngineObserver.NOOP`, с одним наблюдателем — его самого; есть и перегрузка,
принимающая `Collection`.

Композит предпочтительнее самописного fan-out: он переопределяет все методы SPI
и продолжает передавать всё при росте SPI, тогда как самописный молча
проглатывает забытые колбэки.

## Снимок на момент времени — `introspect()`

```java
EngineReport report = engine.introspect().toCompletableFuture().get();

report.instanceId();          // id экземпляра движка
report.isLeader();            // является ли экземпляр текущим лидером лога?
for (var node : report.graph().nodes()) {       // List<NodeReport>
    System.out.printf("%s: state=%s sid=%s retries=%d/%d last=%s%n",
        node.name(), node.state(), node.sid(),
        node.initRetries(), node.loadRetries(), node.lastException());
}
report.graph().mailboxSizes();   // Map<String,Integer> глубина мейлбокса по процессам
report.log();                    // LogBackendReport: length, currentLeader, counts, last ts
```

`EngineReport` — неизменяемый record, идеален для `/debug`-эндпоинта или
health-check. Каждый `NodeReport` несёт:

- `name`, `sid` (`null`, пока у процесса нет состояния; во время reinit при
  `KEEP_OLD` — версия, которая продолжает отвечать, при `RELEASE_FIRST` — `null`
  до промоушена нового Sid), `state` (имя состояния FSM, всё время reinit при
  `KEEP_OLD` — `Serving`; `Paused`; или `Starting` для узла, который ещё ждёт старта — например,
  своих зависимостей или за упавшей зависимостью; сюда же относятся узлы,
  оставшиеся незапущенными после упавшего или прерванного старта, — они ждут
  повторного `newGraph`, триггер их не запускает). Каждый узел графа
  перечисляется ровно один раз, в том числе пока его FSM пересоздаётся. Узел в
  `Paused` показывает Sid живого состояния, которое он хранит (его тёпло
  загрузит resume). Узел, поставленный на паузу посреди reinit при `KEEP_OLD`,
  показывает версию, которая обслуживала: resume тёпло загружает её и доводит
  reinit (уже записанного кандидата он загружает без второго init — если только
  узел к тому времени не работает с `RELEASE_FIRST`: тогда кандидат отбрасывается,
  а узел инициализируется холодно). Узел,
  поставленный на паузу **посреди reinit при `RELEASE_FIRST`**, показывает
  `null`: его старое состояние уже отозвано (`LogDead`), и resume его
  переинициализирует — то же показал бы и рестарт. Узел, поставленный на паузу,
  когда новое состояние такого reinit уже загружалось или как раз в момент его
  завершения, показывает этот новый Sid;
- `replacement` — `"Initializing"` или `"Loading"`, пока reinit при `KEEP_OLD`
  делает новую версию рядом с обслуживающей, иначе `null`;
- `stale` — обслуживающую версию попросили заменить, а замены ещё нет: reinit
  идёт, стоит в очереди (в том числе ждёт в окне дедупликации или
  воспроизводится при старте) или не удался (и ждёт автоматического повтора,
  либо следующего триггера или рестарта, если причина постоянная). Узел,
  поставленный на паузу, пока загружалась его уже записанная новая версия,
  **не** `stale`: resume загрузит её без `init`;
- `initRetries` / `loadRetries` — число неудачных попыток `init` / `load` в
  последней (пере)инициализации процесса;
- `lastException` — **последний** сбой init/load, который когда-либо случался у
  этого узла, в виде `"полное.имя.Класса: message"` или `null`, если сбоев не
  было. При восстановлении узла он **не очищается**: успешный старт сбрасывает
  счётчики попыток и намеренно сохраняет этот текст, поэтому читайте его как
  «последнее, что здесь сломалось», а не как «текущая проблема». Это верно,
  пока восстанавливается тот же экземпляр процесса — повторная попытка
  `init`/`load` или reinit узла, который продолжал обслуживать. Через замену FSM
  узла это **не** переносится: `lastException` живёт на экземпляре FSM, поэтому
  узел в `Dead`, **перезапущенный триггером**, поднятый заново повтором
  `newGraph` (или заменённый сменой графа), возвращается на *новой* FSM и снова
  показывает `lastException=null`. То есть `null` здесь означает «на FSM,
  которая обслуживает этот узел сейчас, ничего не падало», а не «этот узел
  никогда не падал» — историю хранит лог.
  `initRetries` / `loadRetries`, вернувшиеся к `0` при `state` = `Serving`,
  *согласуются* с тем, что узел восстановился, но **не** доказывают этого:
  у reinit'а, застрявшего из-за временного сбоя записи (ниже), ровно та же
  подпись — `state=Serving`, счётчики на `0`, — именно потому, что ни одна
  попытка init так и не началась. Отличают их `lastException` (называет ли он
  сбой записи в лог и остался ли `sid` старым) вместе с `ERROR`-строками движка
  о цикле повторов. Замерено: узел, у которого
  первая попытка `init` упала, а вторая прошла, показывает
  `state=Serving, initRetries=1,
  lastException="java.lang.IllegalStateException: feed unavailable (attempt
  1)"`; после более позднего, полностью чистого reinit'а по триггеру он
  показывает `initRetries=0` с **тем же** `lastException`. После `cancelInit` в нём
  `"io.fom.api.InitInProgressException: Init of process '<name>' was
  cancelled"`. У узла, умершего потому, что при записи результата его init или
  load лог забрал другой инстанс, там
  `"io.fom.api.LeadershipLostException: Lost leadership for <name> during init"`
  (или `load`) — **если** бэкенд отклонил запись; бэкенд, который сообщает о
  перехвате броском исключения (отстранённый `PostgresLogBackend`), кладёт туда
  своё сообщение («… no longer holds the advisory lock for &lt;table&gt; …»),
  поэтому сопоставляйте тип исключения, а не одну точную строку, — см.
  [Исключения](../reference/exceptions.md#leadership). У узла, чей `state` всё ещё
  `Serving`, там может быть
  `"io.fom.api.LeadershipLostException: cannot re-initialise <name>: no longer
  the leader of the log"`: reinit, который отверг лог, — узел продолжает
  обслуживать заведомо устаревшее состояние. При `KEEP_OLD` reinit, который
  первым обнаружил, что лог забрали, оставляет `"…LeadershipLostException: Lost
  leadership for <name> during init"` (или `load`) у узла, который всё ещё
  `Serving`, со `stale=true`.

При `RELEASE_FIRST` reinit, застрявший из-за **временного** сбоя записи в лог,
выглядит почти так же, но это другой случай. Пока бэкенд продолжает валить запись `LogDead`,
например `UncheckedIOException`, движок повторяет попытки с backoff'ом (попытки
1, 2, 4, 8, … логируются на `ERROR`, остальные на `DEBUG`), а узел продолжает
обслуживать своё старое состояние. `introspect()` тогда показывает
`state=Serving`, **старый** `sid`, `initRetries=0` — ни одна попытка init не
начиналась — и `lastException` с этим сбоем записи. Читайте эту комбинацию как
«узел обслуживает состояние, которое его просили заменить». В отличие от
постоянного отказа выше, здесь не вызывается `onInitFailed` (цикл повторов ещё
идёт), поэтому лог и `lastException` — единственные места, где это видно. При
`KEEP_OLD` до нового init ничего не пишется, так что сбой записи его
`LogInitialized` — обычная неудачная попытка init: её считает `initRetries`, а
когда бюджет кончается, узел показывает `stale=true` и приходит `onReinitFailed`.

!!! note "Времени в состоянии нет: зависший init и медленный выглядят одинаково"
    В `NodeReport` нет поля «в этом состоянии с» — только `state`, счётчики
    повторов и `lastException`. По одному `introspect()` нельзя отличить
    зависший `init` от просто медленного. Если нужно алертить на это,
    отслеживайте время сами через `EngineObserver.onStateTransition` (или
    `onInitStarted`; reinit при `KEEP_OLD` переходов не даёт — используйте
    `onReinitStarted`). Бесконечно зависание всё же не длится: init или load,
    блокирующийся **до возврата своего stage** — синхронный вызов без
    таймаута, — завершается по бюджету сторожем на стороне планировщика
    («… had not even returned its stage»), после чего повторяется или уходит
    в `Dead`, как при любом другом таймауте (см.
    [Жизненный цикл процесса](../concepts/process-lifecycle.md#повторы-backoff-таймауты)).

`mailboxSizes()` считает только конверты, ждущие в **mailbox** каждого
процесса. Запросы, которые узел **отложил** (stash), пока он инициализируется,
загружается или переинициализируется при `RELEASE_FIRST` (reinit при `KEEP_OLD`
ничего не откладывает; см.
[Запросы, ждущие узел](#queries-that-wait-for-a-node)), уже покинули mailbox и
не учитываются, поэтому узел, у которого сотни запросов ждут его init, может
показывать `0`; узлы `Paused` и `Starting` всегда показывают `0`. Чтобы увидеть
этот хвост, считайте его по наблюдателю (`onQuerySent` минус
`onQueryCompleted` / `onQueryFailed` по процессу).

`LogBackendReport` даёт `length`, `currentLeader`, `eventCounts`,
`maxTimestampMillis`.

> [English version](../../guides/observability.md)
