# Исключения

Все исключения FOM — unchecked (наследники `RuntimeException`). Они делятся на три
группы: те, что **бросаете вы** из кода процесса, сигнализируя о сбое фазы; те, что
**движок бросает обратно** вызывающему; и те, что из помощников
сериализации / типизированных ячеек.

## Бросаются вашим кодом процесса

| Исключение | Пакет | Откуда бросать | Эффект |
|---|---|---|---|
| `InitializationException` | `io.fom.api` | `ProcessInitializer.init` | движок делает backoff и [повторяет](../concepts/process-lifecycle.md) до `initTimeout` |
| `LoadException` | `io.fom.api` | `ProcessLoader.load` | до `maxLoadRetries` попыток всего (по умолчанию `1`: без повторов), затем откат к `init` |

Бросание этих (или любого исключения) из `init`/`load` запускает механику
повторов/отката. `compute`, упавший с ошибкой, завершает stage запроса
исключительно вашим исключением.

### Единственный сбой init или load, который не повторяется { #undeclared-dependency-in-init }

`ctx.query(name, …)` к имени, которое узел не объявил своей зависимостью,
бросает `UndeclaredDependencyException`. Если это произошло во время `init`
**или `load`**, движок считает это **ошибкой в коде**, а не временным сбоем:
никакой backoff не заставит граф отрастить зависимость. Поэтому попытка не
повторяется — ни повтора init, ни повтора load, ни отката от падающего `load` к
свежему `init`, — и процесс сразу уходит в `Dead` (при reinit с `KEEP_OLD`
сдаётся только reinit, а старая версия продолжает обслуживать; см.
[ниже](#re-init-failures)). В исключении названы
запрошенное имя и зависимости, которые узел *действительно* объявил.

Оно распознаётся, даже если собственный код узла **обернул и перебросил** его
(движок проходит по цепочке причин). Что при этом видит каждая сторона:

| Кто | Получает |
|---|---|
| `newGraph` (и всё остальное, что ждёт старта узла) | голый `UndeclaredDependencyException` |
| `onInitFailed` / `onLoadFailed`, `NodeReport.lastException` | то, что попытка реально бросила, — **вашу обёртку**, если вы обернули |
| запрос, ждавший узел | `QueryRejectedException` («&lt;name&gt; is Dead») с `UndeclaredDependencyException` в качестве причины |

Вердикт принадлежит узлу, допустившему опечатку, а не его потребителям.
Исключение помнит, **кто спрашивал**: `requester()` называет процесс, чей
собственный `ctx.query` назвал необъявленную зависимость (конструктор
`UndeclaredDependencyException(String requester, String message)`), и движок
считает сбой окончательным **только для этого процесса**. Чужая опечатка может
дойти и до потребителя — внутри `QueryRejectedException`, который он получает,
когда эта зависимость умирает, или просто как ответ `compute` зависимости,
запросившего необъявленное имя при обслуживании запроса, без всякого отклонения
вокруг. В обоих случаях для потребителя это **не** окончательно: его
`init`/`load` падает и повторяется, как после любого другого неудачного запроса к
зависимости, и восстанавливается, когда зависимость исправят (в пределах своих
бюджетов).

Раньше ту же ошибку повторяли, пока не истечёт `initTimeout`, и наружу
выходил `InitializationTimeoutException` — медленный отказ, чьё сообщение
указывало на бюджет, а не на опечатку.

## Бросаются обратно вызывающему

| Исключение | Пакет | Когда |
|---|---|---|
| `InitializationTimeoutException` | `io.fom.api` | исчерпан общий бюджет `initTimeout` по всем повторам `init`, или в пределах этого бюджета `load` продолжал падать на свежеинициализированном состоянии; процесс теперь `Dead` (reinit с
`KEEP_OLD`, исчерпавший бюджет, не роняет ни одного вызывающего: старая версия
продолжает обслуживать, см. [Сбои reinit](#re-init-failures)). `init` или `load`, запросивший необъявленную зависимость, сюда **не** попадает — он сразу падает с `UndeclaredDependencyException` ([подробнее](#undeclared-dependency-in-init)) |
| `InitInProgressException` | `io.fom.api` | init/load цели отменён, пока его ждал запрос, — сообщение "Init of process '&lt;name&gt;' was cancelled" (запрос к ещё стартующему процессу вместо этого ждёт). Сам ждущий **запрос** получает не его, а `QueryRejectedException` с ним в качестве причины (cause): «&lt;name&gt; is Dead» после пользовательского [`cancelInit`](../concepts/process-lifecycle.md#cancelling-an-init), «&lt;name&gt; is shutting down», если старт отменили пауза или удаление. Напрямую его бросают `newGraph`, заблокированный на первом init узла, и `resume`/`resumeTenant`, заблокированный на возобновляемом узле, когда принятый `pause`/`remove` (или `pauseTenant`/`removeTenant`) этого узла [отменяет запуск](../concepts/graph-swap.md#конкурентность) |
| `QueryException` | `io.fom.api` | сбой маршрутизации — нет маршрута для типа («No route for type &lt;class&gt;»; то же самое даёт статический маршрут `.handles`, удалённый вместе со своим узлом), либо сообщение `Routable` / динамический резолвер `.route(...)` вернул пустое имя или имя, которого нет в графе |
| `UndeclaredDependencyException` | `io.fom.api` | наследник `QueryException`: `ctx.query(name, …)` к имени, которое вызывающий процесс **не** объявил своей зависимостью, — «No such dependency: &lt;name&gt;; declared: [&lt;имена&gt;]». Проверка идёт по *объявленным* зависимостям, поэтому имя, которого в графе вовсе нет, даёт то же сообщение. Бросается синхронно из `ctx.query`; в `init` или `load` — даже обёрнутый вашим кодом — он к тому же [сразу валит процесс](#undeclared-dependency-in-init), но только для спросившего процесса (`requester()`); пришедший к потребителю от зависимости — внутри её `QueryRejectedException` или как ответ её `compute` — повторяется, как любой сбой зависимости |
| `QueryRejectedException` | `io.fom.api` | `queryProcess(name)` / `queryProcess(ProcessRef)` для имени, которого нет в графе («Unknown process: '&lt;name&gt;'»); цель на паузе, `Dead` или действительно останавливается («&lt;name&gt; is shutting down» — пауза, удаление или `close()`, но **не** reinit; так же и для запроса, ждавшего старта, который отменили пауза или удаление, — с `InitInProgressException` в cause); её мейлбокс отклонил сообщение; узел удалили или поставили на паузу либо движок закрылся, пока запрос его ждал; или запрос ещё вычислялся, когда истёк бюджет очистки («&lt;name&gt; is shutting down: the query outlived the cleanup timeout» — это происходит при **любой** очистке, в том числе при reinit по триггеру и при замене графа, подменяющей узел, а не только при паузе, удалении и `close()`) |
| `TenantAccessDeniedException` | `io.fom.tenant` | [обёртка тенантов](../guides/multi-tenancy.md) отказала (не авторизован, или не-`Routable` `query`) |
| `TimeoutException` | `java.util.concurrent` | запрос достиг дедлайна (`queryTimeout` или таймаут вызова) — сообщение "Query to '&lt;name&gt;' did not complete within &lt;timeout&gt;" или "&lt;name&gt; did not start before the query deadline" для запроса, который ещё ждёт старта узла; вложенный `ctx.query(dep, …)`, исчерпавший дедлайн, унаследованный от вызвавшего запроса, сообщает "Query to '&lt;dep&gt;' did not complete within the deadline inherited from its calling query" — побеждает тот дедлайн, который сработал первым |
| `IllegalStateException` | `java.lang` | `engine.query`/`queryProcess` на закрытом движке, в том числе через `TenantAwareEngine`, — "Engine &lt;id&gt; is closed"; а также `remove` / `removeTenant` / `newGraph`, убирающий узлы, которые применили изменение графа, но не смогли списать состояние удалённого процесса, потому что **запись в лог упала** — «… could not retire the state of [...]: the log write failed», с исключением этой записи в cause. Если запись *отклонили*, будет наследник `LeadershipLostException`: разница (вернуть логу запись или заменить движок — само удаление повторить нельзя) — [ниже](#leadership-half-applied) |

`QueryRejectedException` и `InitInProgressException` бывают **временными**:
процесс на паузе или `Dead` отклоняет запросы, пока его не возобновят или не
перезапустят (запрос, гонящийся с паузой или удалением, получает «&lt;name&gt;
is shutting down», а не «is Dead»). Такие запросы стоит повторять с небольшим
backoff. «Unknown
process» для имени, которого в графе действительно нет, — окончательный отказ.
Несколько случаев, которые раньше приводили к отказу, теперь **ждут**, и
повторять их не нужно: запрос, попавший на reinit, сразу получает ответ от
старой версии (`KEEP_OLD`) или складывается в stash и отвечает уже из нового
состояния (`RELEASE_FIRST`); запрос к узлу, который заменяет замена графа,
передаётся новой FSM; запрос к ещё не стартовавшему узлу ждёт — как и запрос,
отправленный сразу после того, как
[`trigger`](../concepts/process-lifecycle.md#restarting-a-dead-process)
перезапустил `Dead`-процесс.

У `engine.query`/`queryProcess` они проявляются как сбой возвращённого
`CompletionStage` — в том числе `QueryException` от плохого маршрута и
`IllegalStateException` закрытого движка; синхронно бросается только ошибка
аргумента `null`. При ожидании stage через `.get()`
они приходят обёрнутыми в `ExecutionException` — разворачивайте через
`getCause()`:

```java
try {
    engine.query(msg).toCompletableFuture().get();
} catch (ExecutionException e) {
    if (e.getCause() instanceof QueryException qe) { /* плохой маршрут */ }
}
```

## Только для наблюдателей { #observer-only }

| Исключение | Пакет | Когда |
|---|---|---|
| `AttemptCancelledException` | `io.fom.api` | подкласс `java.util.concurrent.CancellationException`, передаваемый в `onInitFailed` / `onLoadFailed`, когда попытку init или load прервал **сам движок** — `cancelInit`, пауза, удаление, замена графа или `close()` («&lt;name&gt; init attempt N cancelled» / «… load attempt N cancelled»). По нему остановку оператором можно отличить от настоящего сбоя. Обычный `CancellationException`, брошенный *вашим* `init`/`load`, — **не** он: это обычная неудачная попытка, которая повторяется и расходует бюджет. См. [Отмену init](../concepts/process-lifecycle.md#cancelling-an-init) |

## Сбои reinit { #re-init-failures }

При `KEEP_OLD` (по умолчанию) reinit ничего не бросает вызывающим: на запросы
продолжает отвечать старая версия. Когда reinit сдаётся, причину получает
`EngineObserver.onReinitFailed(name, keptSid, cause)`, её называет `WARN`
«re-init gave up; keeps serving …», она лежит в `NodeReport.lastException`, а
`NodeReport.stale` равен `true`.

| `cause` | Когда | Повторяется сам |
|---|---|---|
| `InitializationTimeoutException` | `init` новой версии падал — любым исключением или ошибкой, включая `OutOfMemoryError`, — пока не кончился `initTimeout`, или её `load` в пределах этого бюджета продолжал падать на свежеинициализированном состоянии | да, через `reinitRetryBackoffMin`, с удвоением до `reinitRetryBackoffMax` (`Duration.ZERO` это выключает) |
| `UndeclaredDependencyException` | `init` или `load` новой версии запросил необъявленную зависимость | нет |
| `LeadershipLostException` | лог отклонил `LogInitialized` или `LogLoaded` новой версии, или бэкенд бросил это исключение | нет; сообщается один раз на обслуживающий Sid, следующие запросы reinit молча отбрасываются, узел остаётся stale (см. [ниже](#leadership)) |
| `IllegalArgumentException` | бэкенд вообще не принимает это событие (например, сверх своих лимитов на размер) | нет |
| `InitInProgressException` | reinit отменили через [`cancelInit`](../concepts/process-lifecycle.md#cancelling-an-init) | нет; запрос, стоявший за ним в очереди, запускается сразу. `cancelInit(name)` отменяет и повтор, который пока только запланирован, — узел остаётся stale |

Постоянный сбой ждёт следующего триггера, реактивного изменения или рестарта;
триггер к тому же сразу запускает новый reinit, даже если автоматический повтор
ещё не наступил. При `RELEASE_FIRST` старая версия выводится первой, поэтому
неудавшийся reinit заканчивается `Dead`, как неудачный первый старт, и
перечисленные исключения доходят до ждущих запросов внутри
`QueryRejectedException`. См.
[Если reinit не удался](../concepts/process-lifecycle.md#when-a-re-init-fails).

## Лидерство — `LeadershipLostException` { #leadership }

«Я больше не лидер своего лога» — это **один** тип исключения везде:
`io.fom.api.LeadershipLostException`, наследник `IllegalStateException`. Он
всегда бросается синхронно, и сама отклонённая запись в лог не попала —
достаточно поймать его один раз и обрабатывать все случаи одинаково:

| Откуда | Сообщение |
|---|---|
| `trigger(name, value)` / `trigger(Map)`, чью запись `LogTrigger` отклонили | «Engine &lt;id&gt; is no longer the leader of &lt;logId&gt;; the trigger for [...] was not recorded» |
| `pause` / `remove` / `newGraph` — и то же самое отдают `pauseTenant` / `removeTenant` | «Engine[&lt;id&gt;] lost leadership while persisting &lt;event&gt;» / «… while persisting graph» / «GraphMachine[&lt;id&gt;] lost leadership while persisting &lt;event&gt;» |
| `resume` / `resumeTenant`, чья запись `LogResumed` не удалась — узел снова останавливают, и он остаётся на паузе (см. ниже) | если бэкенд **отклонил** запись: «GraphMachine[&lt;id&gt;] lost leadership while persisting LogResumed for &lt;name&gt;; it stays paused» / «…; already resumed: [...]»; если он **бросил** исключение (отстранённый Postgres-лидер), исключение бэкенда пробрасывается без изменений — «PostgresLogBackend &lt;logId&gt; no longer holds the advisory lock …», без имени узла и без списка «already resumed» |
| `purgeArchives(keepHistory)` на отстранённом лидере — архивы принадлежат тому, кто ведёт лог сейчас | «Engine &lt;id&gt; is not the leader of &lt;logId&gt; (&lt;other&gt; is); no archive was removed» |
| `remove` / `removeTenant` (или `newGraph`, убирающий узлы), которые заменили граф, но не смогли списать состояние удалённых процессов, **потому что `LogDead` отклонили или бэкенд бросил `LeadershipLostException`** (по любой другой причине будет `IllegalStateException`) — см. [ниже](#leadership-half-applied) | «GraphMachine[&lt;id&gt;] applied the graph change but could not retire the state of [...]: no longer the leader of the log» — с исключением самого бэкенда в **cause**, если оно было |
| `pause` / `pauseTenant`, прерванные на середине — см. [ниже](#leadership-half-applied) | «… lost leadership while persisting LogPaused; nothing was paused» / «…; already paused: [...]» |
| любая запись на [отстранённом](../guides/multi-node.md#failover) Postgres-лидере | «… no longer holds the advisory lock … leader session was lost; another instance may own the log» |
| `LogBackend.compact`, а значит и `engine.snapshot()`, когда последний `LogLeader` в логе называет другой инстанс | «Cannot compact &lt;logId&gt;: instance &lt;id&gt; is no longer the leader (&lt;other&gt; is); nothing was written» |
| запись результата собственного `init`/`load` процесса | «Lost leadership for &lt;name&gt; during init» / «… during load» — но только если бэкенд **отклонил** запись; бэкенд, который сообщает о перехвате *броском* исключения (случай отстранённого Postgres выше), отдаёт своё собственное сообщение |

Повтором лидерство не вернуть: закройте движок и откройте новый, чтобы снова
побороться за лидерство. Чтение продолжает работать из состояния в памяти
(`engine.query` по-прежнему отвечает), но записать долговременное изменение
больше нельзя. [Watcher](../concepts/triggers-and-watchers.md),
триггер которого получил это исключение, останавливается сам с одним `WARN` —
в том числе когда наблюдаемый процесс `Dead`.

Где окажется пострадавший процесс, зависит от пути:

- узел, который не смог записать результат **своего** холодного или тёплого
  старта, уходит в `Dead`, и `NodeReport.lastException` прямо об этом сообщает, а
  не остаётся `null`. Текст бывает двух видов, поэтому сопоставляйте по **типу**,
  а не по одной строке: движок формирует
  `«io.fom.api.LeadershipLostException: Lost leadership for <name> during init»`
  (или `load`), когда бэкенд *отклонил* запись, а бэкенд, который сообщает о
  перехвате *броском* исключения, отдаёт своё сообщение — например
  `«io.fom.api.LeadershipLostException: PostgresLogBackend <logId> no longer
  holds the advisory lock for <table> — leader session was lost; another instance
  may own the log»`;
- узел, который **возобновляли**, **не** умирает. `resume` /
  `resumeTenant` бросают `LeadershipLostException` в одном из двух видов,
  поэтому сопоставляйте по **типу**, а не по одной строке. Если бэкенд
  *отклонил* запись, сообщение движка называет узел и, если более ранние узлы
  того же вызова успели возобновиться, перечисляет их («…; already resumed:
  [...]»). Если бэкенд сообщает о перехвате *броском* исключения —
  [отстранённый](../guides/multi-node.md#failover) `PostgresLogBackend` именно
  бросает, а не отклоняет, — это исключение пробрасывается как есть:
  `«PostgresLogBackend <logId> no longer holds the advisory lock for <table> —
  leader session was lost; another instance may own the log»`, без имени узла и
  без того, что уже возобновилось (какие узлы всё ещё `Paused`, покажет `engine.introspect()`). В обоих случаях
  сам упавший узел снова останавливают, и он
  остаётся `Paused` со своим последним Sid и `lastException == null`, по-прежнему
  отклоняя запросы `QueryRejectedException` («Process '&lt;name&gt;' is paused»).
  Возобновление нужно повторить: этим движком, если записи снова заработают, или
  тем, кто станет лидером.

    Сам `LogResumed` в лог не попал, но состояние узла в логе не тронуто лишь
    *обычно*. Возобновление, которое **холодно инициализировало** узел,
    выполняет его `init` до записи `LogResumed`, поэтому к моменту отказа в логе
    уже лежат свежие `LogInitialized`/`LogLoaded` для **нового Sid**, а списать
    их этот инстанс уже не может, потому что писать ему больше нельзя; при этом
    `NodeReport.sid` для узла на паузе по-прежнему показывает **старый** Sid.
    Это осиротевшее состояние никто не обслуживает — узел на паузе, а повторное
    возобновление снова холодно его инициализирует, — но не читайте лог так,
    будто возобновление не оставило следов;

    Любая запись, которая **бросила исключение**, а не была отклонена,
    обрабатывается так же — узел снова останавливают, и он остаётся на паузе, —
    и до вызывающего доходит собственное исключение бэкенда:
    `LeadershipLostException` у отстранённого Postgres-лидера или то, что бэкенд
    бросает при ошибке ввода-вывода или SQL;
- лидерство, потерянное *после* того как триггер был принят, не бросает ничего:
  процесс продолжает обслуживать то состояние, которое у него есть. При
  `KEEP_OLD` reinit сдаётся (`onReinitFailed` с `LeadershipLostException`, без
  повтора, сообщается один раз на обслуживающий Sid), а следующие запросы молча
  отбрасываются; при `RELEASE_FIRST`
  [reinit отбрасывается](../concepts/process-lifecycle.md#переинициализация) с
  `ERROR`.

### Две операции, которые сообщают, что применили наполовину { #leadership-half-applied }

Обе действуют по процессу за раз, поэтому исключение говорит, как далеко они
успели зайти, вместо того чтобы делать вид, что ничего не произошло:

- **`remove` / `removeTenant`** сначала меняют граф, а состояние
  удалённых процессов списывают после. Если изменение графа прошло, а состояние
  списать не удалось, вызов бросает исключение и **называет процессы, состояние
  которых не смог списать**. Изменение графа остаётся в силе — для этого движка
  процессов больше нет, — но их состояние в логе живо, поэтому следующий лидер
  тёпло загрузит его, если имя вернётся в граф.

    *Какое* именно исключение пришло, говорит, что с этим делать, — они не
    взаимозаменяемы:

    | Что произошло | Исключение и сообщение | Что делать |
    |---|---|---|
    | запись `LogDead` **отклонили** либо бэкенд сообщил о перехвате **броском** `LeadershipLostException` | `LeadershipLostException`: «GraphMachine[&lt;id&gt;] applied the graph change but could not retire the state of [...]: no longer the leader of the log», с исключением бэкенда в cause, если оно было | лидерство не возвращается: закройте этот движок и откройте новый |
    | запись **упала по другой причине** — ошибка ввода-вывода, обрыв соединения, — что вполне может быть временным | `IllegalStateException`: «GraphMachine[&lt;id&gt;] applied the graph change but could not retire the state of [...]: the log write failed», с этим исключением в cause | верните логу возможность записи; что делать с оставшимся состоянием — ниже |
    | FSM **так и не дошла до записи списания**: ожидание остановки истекло раньше, чем она добралась до своего `LogDead`, поэтому списали состояние или нет — просто неизвестно | сообщается так же, как упавшая запись, с cause «the shutdown of &lt;name&gt; did not finish within &lt;budget&gt;, and it had not recorded the state as retired» | считайте состояние **не** списанным; дальше — как выше |

    Остановка, которая просто **вышла за бюджет очистки**, здесь *не*
    сообщается вообще: `LogDead` пишется *до* начала `cleanUp`, поэтому
    медленный `cleanUp` ничем не рискует состоянием. Сообщается только та
    остановка, которая истекла раньше, чем FSM записала списание, — об этом
    прямо говорит сообщение.

    Если удалённых процессов несколько и падают они одновременно, побеждает
    потеря лидерства: одного отклонённого append'а достаточно, чтобы весь вызов
    стал `LeadershipLostException`, — всё равно ни одна следующая запись не
    пройдёт.

    **Повторять удаление не надо** — оно не откатит то, что применилось
    наполовину. Изменение графа уже применено, поэтому имени в текущем графе
    больше нет, и две точки входа ведут себя по-разному:

    - `Engine.remove(names)` работает с именами, которые вы передали,
      поэтому повтор с тем же набором будет отклонён с
      `IllegalArgumentException: Unknown process: '&lt;name&gt;'`, не успев ничего
      списать (удаление [не идемпотентно](#прочие-стандартные-исключения)).
    - `TenantAwareEngine.removeTenant(caller, tenant)` каждый раз заново выводит
      имена процессов тенанта из **текущего** графа, а их там уже нет — поэтому
      повтор ничего не находит, ничего не делает и просто возвращает **пустое
      множество**. Состояние, оставшееся после упавшего вызова, он при этом не
      списывает, так что тихий пустой результат — не признак починенного удаления.

    Что действительно можно сделать:

    - **Принять изменение графа.** Процесса в графе этого движка больше нет;
      запросы и триггеры по этому имени теперь падают как для неизвестного
      процесса.
    - **Устранить причину.** При потерянном лидерстве этот движок больше никогда
      не запишет ничего: закройте его и откройте новый. При упавшей записи
      верните логу возможность записи (место на диске, соединение, каталог файла).
    - **И только потом решить, что делать с состоянием в логе.** Оно там
      остаётся. Если имя вернётся в граф позже — это именно то, что нужно:
      следующий лидер лога тёпло загрузит его. Если не вернётся, уберите его
      **компакцией**: компакция сохраняет состояние только тех процессов, что
      есть в *последнем* графе, поэтому именно она убирает сироту —
      `Engine.snapshot()` на движке, который ведёт лог, когда запись снова
      работает, либо офлайн через `LogCompaction.compact(backend)` /
      [`fom-log compact`](../guides/cli.md), пока ни один движок не запущен.
      См. [Снапшоты](../concepts/snapshots.md).
- **`pause` / `pauseTenant`** пишут по одному `LogPaused` на процесс.
  Отказ на середине бросает исключение и **называет то, что уже поставлено на
  паузу** («already paused: [...]» либо «nothing was paused»); этот движок не
  может ни довести паузу до конца, ни отменить её.

## Сериализация и типизированные ячейки

| Исключение | Пакет | Когда |
|---|---|---|
| `SerDeException` | `io.fom.serde` | [`SerDe`](../guides/serialization.md) не смог (де)сериализовать — плохие байты, отклонение фильтром, неожиданный тип |
| `CodecException` | `io.fom` | [`Codec`](../guides/configuration.md#типизированные-ячейки-свойств) не смог закодировать/декодировать значение |
| `NoSuchPropertyException` | `io.fom` | `Properties.get(key)` для отсутствующего в ячейках ключа |

## Прочие стандартные исключения

- **`IllegalArgumentException`** — невалидный граф (цикл, отсутствующая
  зависимость, маршрут на неизвестный процесс), `remove`/`pause`/
  `resume` с именем процесса, которого нет в текущем графе — в том
  числе уже удалённого, так что удаление **не идемпотентно** («Unknown process:
  '&lt;name&gt;'»), то же сообщение **синхронно** бросают
  `trigger(name, value)`, `trigger(Map)` и `watch(watcher)` для имени, которого
  нет в графе (`trigger(Map)` проверяет все имена до того, как что-то запишет,
  поэтому ничего не записывается и ничего не применяется), `remove`/`Graph.without`,
  который убрал бы все процессы («Removing [...] would leave an empty graph» —
  вместо этого закройте движок), невалидный `EngineConfig`
  (неположительная длительность, `maxLoadRetries < 1`), или плохой SQL-идентификатор
  в `fom-jdbc` (в том числе зарезервированное ключевое слово SQL как имя таблицы).
- **`IllegalStateException`** — использование закрытого `Engine`/бэкенда (запрос
  получает его как упавший stage, см. выше), второй процесс пытается взять
  блокировку лидерства бэкенда, `pause` / `remove` (а значит, и
  `pauseTenant` / `removeTenant`), называющий процесс, `newGraph` / `updateGraph`,
  который его убирает или переопределяет, или `close()`, вызванные **на
  собственном диспетчере этого процесса** — то есть синхронно из колбэка
  `EngineObserver` о нём («… called on that process's own dispatcher (from an
  EngineObserver callback about it) …»; бросается сразу, ничего не меняется;
  `close()` там просто возвращается, если движок уже закрывается — см.
  [Остановка процесса из колбэка](../guides/observability.md#stopping-from-a-callback)), `Engine.close()`, пока `newGraph` (первая установка или
  последующая замена) ещё запускается («Engine closed while 'X' was starting»), DI-суплаер
  разрешает до регистрации контейнера, или изменение графа
  [не смогло списать состояние удалённого процесса](#leadership-half-applied),
  потому что запись в лог упала либо остановка процесса не завершилась в срок.
  Потеря лидерства — наследник `LeadershipLostException` [выше](#leadership).
- **обычный `java.lang.RuntimeException`** — узел, не дошедший до `Serving` за
  `initTimeout + loadTimeout`, роняет вызов, который его запустил
  (`newGraph`, замена графа, `resume`): «Node '&lt;name&gt;' did not
  reach Serving within PT1M (state=&lt;State&gt;)», плюс «; last failure: …»,
  если у узла уже была неудачная попытка. Это терпение *старта*, а не бюджет
  узла, и узел он не останавливает: `introspect()` сразу после этого вполне
  может показывать его в
  `Loading` с `lastException == null`. См.
  [бюджет старта](../concepts/process-lifecycle.md#startup-budget).
- **`IndexOutOfBoundsException`** — `LogBackend.get`/`getBetween` вне
  `[0, length())`.
- **`io.fom.log.LogCorruptedException`** — checked `IOException` из конструктора
  `FileLogBackend`: в файле повреждение, которое не может оставить крах
  (плохой фрейм, за которым идут ещё данные, слишком большой фрейм или целый
  фрейм, который не удаётся декодировать). Файл не изменяется; `offset()`,
  `readableEvents()` и `path()` указывают место. Восстановление — в
  [Бэкендах хранения](../guides/persistence-backends.md#filelogbackend).
- **`IllegalArgumentException`** из `new PostgresLogBackend(ds, logId)` — в
  `logId` есть символы, отличные от ASCII-букв, цифр и `_`.

!!! warning "`trigger()` сначала пишет в лог"
    `engine.trigger(...)` **сначала** записывает свой `LogTrigger`, поэтому всё,
    что бросит `append` бэкенда (сбой ввода-вывода или SQL, потеря лидерства),
    выходит из `trigger()` синхронно и **не** повторяется: в лог ничего не
    записано, reinit не начинается, а запросы продолжают обслуживаться из
    текущего состояния в памяти. Повторите `trigger` сами, когда бэкенд
    оживёт. То же верно и для `Dead`-процесса: его `LogTrigger` записывается
    до перезапуска, поэтому отклонённая запись ничего не перезапустит, — а
    `trigger(Map)` записывает **одну** запись, покрывающую все запрошенные
    имена, включая `Dead`, прежде чем что-либо применить.

> [English version](../../reference/exceptions.md)
