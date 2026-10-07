# Конфигурация

Движок конфигурируется неизменяемым record'ом `EngineConfig`. Начните с
`EngineConfig.defaults()` и [выведите](#программно) нужные поля (либо используйте
конструктор), распарсите из [HOCON](#hocon) и
[горячо перезагрузите](#hot-reload) во время выполнения.

## Поля

| Поле | По умолчанию | Смысл |
|---|---|---|
| `initTimeout` | 30 с | общий бюджет `init` узла на все повторы; также ограничивает каждую попытку |
| `loadTimeout` | 30 с | бюджет на одну попытку `load` |
| `cleanupTimeout` | 30 с | бюджет одного процесса на слив запросов «в полёте» и `cleanUp`; `close()` останавливает процессы одной глубины зависимостей параллельно, поэтому занимает не более примерно (уровни зависимостей) × этот бюджет |
| `queryTimeout` | 10 с | дедлайн `engine.query(...)` / `queryProcess(...)` по умолчанию |
| `dedupWindow` | 100 мс | [окно дебаунса reinit](../concepts/reactive-cascade.md#окно-дедупликации) |
| `backoffMin` | 50 мс | минимальный backoff повторов init/load |
| `backoffMax` | 5 мин | потолок базового backoff повторов init/load; джиттер ×[0.5, 1.5) применяется после потолка, поэтому задержка может достигать ~1.5 × `backoffMax` ([backoff](../concepts/process-lifecycle.md#backoff)) |
| `maxLoadRetries` | 1 | попыток `load` до отката к `init` |
| `snapshotPolicy` | `Disabled` | автоматическая [ротация лога](../concepts/snapshots.md) |
| `reinitRetryBackoffMin` | max(`initTimeout`, 30 с), но не больше явно заданного `reinitRetryBackoffMax` | первая задержка перед повтором неудавшегося reinit (старая версия тем временем продолжает обслуживать); удваивается с джиттером ×[0.5, 1.5) до максимума; `Duration.ZERO` отключает автоматические повторы; `null` = по умолчанию |
| `reinitRetryBackoffMax` | max(10 мин, `reinitRetryBackoffMin`) | потолок этой задержки; `null` = по умолчанию |
| `reinitStrategy` | `KEEP_OLD` | `KEEP_OLD`: старая версия обслуживает, пока новая не загружена, и продолжает обслуживать, если новая не удалась; `RELEASE_FIRST`: сначала старая версия выводится (запросы ждут, неудавшийся reinit оставляет узел `Dead`, двух версий в памяти не бывает). Узел может переопределить её через `GraphBuilder.reinitStrategy(...)`; `null` = по умолчанию |

Порядок полей выше — это в точности порядок канонического конструктора
record'а (все двенадцать компонентов); вторичный конструктор из девяти
аргументов (без последних трёх) сохранён и берёт для них значения по умолчанию.
Конструктор валидирует значения и иначе бросает `IllegalArgumentException`:

- первые семь длительностей должны быть **строго положительными** (включая
  `dedupWindow`), `backoffMax >= backoffMin`, `maxLoadRetries >= 1`;
- каждая из границ повтора reinit может быть `null` (= выводится, см. ниже);
- `reinitRetryBackoffMin` может быть `Duration.ZERO` (автоматические повторы
  reinit отключены), но не отрицательной;
- явно заданный `reinitRetryBackoffMax` должен быть **строго положительным** и
  `>= reinitRetryBackoffMin`.

Последние три поля выводятся через `withReinitRetryBackoff(min, max)` и
`withReinitStrategy(strategy)`.

**Выводимые границы повтора reinit следуют за бюджетом init.** Граница повтора,
которую вы не задали (`null`), не фиксируется при создании конфига: она
вычисляется при каждом чтении из текущего `initTimeout`, поэтому
`withInitTimeout(...)` её сдвигает. Заданная вами граница остаётся как есть —
даже если она равна той, что была бы выведена:

```java
var d = EngineConfig.defaults();
d.withInitTimeout(Duration.ofMinutes(20)).reinitRetryBackoffMin();    // 20 мин (выведено)
d.withInitTimeout(Duration.ofMinutes(20)).reinitRetryBackoffMax();    // 20 мин (выведено)
d.withReinitRetryBackoff(Duration.ofSeconds(5), Duration.ofSeconds(50))
 .withInitTimeout(Duration.ofMinutes(2)).reinitRetryBackoffMin();     // 5 с (задано явно)
```

Выведенный min никогда не превышает явно заданный max: если задан только max
(скажем, 1 мин — через `withReinitRetryBackoff(null, Duration.ofMinutes(1))`), а
`initTimeout` равен 5 мин, min читается как 1 мин. Поскольку record хранит
`null` для выводимой границы, конфиг с выводимой границей **не** равен
(`equals`) конфигу, где то же значение задано явно, а `toString()` показывает
для неё `null` (`reinitRetryBackoffMin=null`); действующие значения читайте
через аксессоры.

Когда ответ на запрос завершается ошибкой — истёк его `queryTimeout` (или таймаут
вызова), запрос отменён или запрос к зависимости упёрся в унаследованный
дедлайн, — движок отменяет `CompletionStage` вычисления и сразу освобождает его
слот «в полёте» — даже если вычисление нельзя остановить (оно всё ещё
блокировано внутри `compute()` или его stage игнорирует `cancel()`, как
`minimalCompletionStage()`). Такое вычисление брошено: оно продолжает работать в
фоне, но не держит reinit, паузу или закрытие на `cleanupTimeout`, не
считается неудачной очисткой и не сообщает `onComputeDuration`, даже если позже
завершится — поэтому `cleanUp` может выполняться, пока оно ещё
работает. См.
[Жизненный цикл процесса](../concepts/process-lifecycle.md#повторы-backoff-таймауты).

Аналогично, когда попытка `init` или `load` выходит за `initTimeout` /
`loadTimeout`, движок отменяет `CompletionStage`, который вернул `init` /
`load` (корутина Kotlin отменяется; работа обычного `CompletableFuture` не
прерывается). Load, ставший ненужным по *любой другой* причине
(отменённый старт, пауза, удаление, замена графа, `close()`), отменяется так
же; см.
[контракт отмены load](../concepts/process-lifecycle.md#load-cancellation).

!!! note "Три бюджета измеряются и сообщаются, а не принудительно применяются"
    `initTimeout`, `loadTimeout` и `cleanupTimeout`
    измеряются по **всей попытке**, а не только по ожиданию на
    возвращённом ею `CompletionStage`: работа, сделанная до того как stage отдан
    наружу (синхронный `init`, возвращающий `completedFuture(...)`), тоже
    измеряется, и попытка, исчерпавшая бюджет, считается таймаутом, даже
    если её stage уже завершён. init или load, блокирующийся **до возврата
    своего stage**, тоже завершается по бюджету (сторож проваливает попытку с
    «… had not even returned its stage»), поэтому синхронный `init`,
    крутящийся 4 с при бюджете 1 с, падает примерно через 1 с, и узел
    повторяет попытку или уходит в `Dead`, как при любом таймауте. Попытка
    завершается, но работа не **останавливается**: истёкший бюджет отменяет
    stage (что ничего не даёт, если stage уже завершён) и никогда не прерывает
    поток, на котором идёт ваш код, так что этот `init` считает все 4 с, а его
    результат отбрасывается. Поток прерывает
    только явная остановка — `cancelInit`, пауза, удаление, заменяющее изменение
    графа, `close()`; см.
    [Жизненный цикл процесса](../concepts/process-lifecycle.md#cancelling-an-init).
    `cleanupTimeout` — один бюджет, *разделяемый* между сливом
    запросов «в полёте» и `cleanUp`, в этом порядке; по его истечении узел идёт
    дальше, но тело `cleanUp` не прерывается никогда и может продолжать работать в
    фоне.

## Программно

```java
import io.fom.*;
import java.time.Duration;

// `EngineConfig.defaults()` — точка отсчёта; меняйте только нужное:
EngineConfig cfg = EngineConfig.defaults()
    .withInitTimeout(Duration.ofMinutes(2))
    .withQueryTimeout(Duration.ofSeconds(2))
    .withBackoff(Duration.ofMillis(100), Duration.ofSeconds(30))
    .withSnapshotPolicy(new SnapshotPolicy.FixedInterval(Duration.ofHours(6), 7));
    // FixedInterval(Duration.ofHours(6)) без второго аргумента хранил бы все архивы
```

Каждый такой метод возвращает **новый** `EngineConfig` (record неизменяем) и
выполняет ту же валидацию, что и конструктор, поэтому недопустимое значение
отвергается сразу:

| Метод | Какое поле заменяет |
|---|---|
| `withInitTimeout(Duration)` | `initTimeout` (границы повтора reinit, не заданные явно, следуют за ним) |
| `withLoadTimeout(Duration)` | `loadTimeout` |
| `withCleanupTimeout(Duration)` | `cleanupTimeout` |
| `withQueryTimeout(Duration)` | `queryTimeout` |
| `withDedupWindow(Duration)` | `dedupWindow` |
| `withBackoff(Duration min, Duration max)` | `backoffMin` **и** `backoffMax` (отвергается, если `max < min`) |
| `withMaxLoadRetries(int)` | `maxLoadRetries` (должно быть `>= 1`) |
| `withSnapshotPolicy(SnapshotPolicy)` | `snapshotPolicy` |
| `withReinitRetryBackoff(Duration min, Duration max)` | `reinitRetryBackoffMin` **и** `reinitRetryBackoffMax`; любая может быть `null` (= выводится); `min` может быть `ZERO` (повторы отключены); явный `max` должен быть `> 0` и `>= min` |
| `withReinitStrategy(ReinitStrategy)` | `reinitStrategy` (не `null`) |

Если хочется указать компоненты сразу, вторичный конструктор из девяти
аргументов принимает первые девять в порядке таблицы из раздела [Поля](#поля) и
оставляет поля reinit по умолчанию (канонический конструктор принимает все
двенадцать; `null` в поле reinit оставляет его значение по умолчанию):

```java
EngineConfig explicit = new EngineConfig(
    Duration.ofSeconds(30),   // initTimeout
    Duration.ofSeconds(30),   // loadTimeout
    Duration.ofSeconds(30),   // cleanupTimeout
    Duration.ofSeconds(10),   // queryTimeout
    Duration.ofMillis(100),   // dedupWindow
    Duration.ofMillis(50),    // backoffMin
    Duration.ofMinutes(5),    // backoffMax
    1,                        // maxLoadRetries
    SnapshotPolicy.Disabled.INSTANCE);
```

## HOCON { #hocon }

Модуль `fom-config-hocon` парсит `EngineConfig` из Typesafe `Config`:

```java
import io.fom.config.EngineConfigHocon;

EngineConfig cfg = EngineConfigHocon.parse();              // ConfigFactory.load()
EngineConfig cfg2 = EngineConfigHocon.parse(myConfig);     // из собственного Config
```

Он читает `engine.graph.default.*` и `engine.system.*`. Базовый набор (любой путь
можно переопределить):

```hocon
engine {
  graph.default {
    init    { timeout = 30s, min-backoff = 50ms, max-backoff = 5m }
    load    { timeout = 30s, max-retries = 1 }
    cleanup.timeout = 30s
    reinit {
      strategy = keep-old          # или release-first
      # необязательно; нет = max(init.timeout, 30s) и max(10m, min-backoff); min-backoff = 0 отключает повторы
      retry { min-backoff = 30s, max-backoff = 10m }
    }
  }
  system {
    query-timeout = 10s
    dedup-window = 100ms
    # опциональная ротация:
    log.rotate { cron = "0 0 */6 * * ?", keep-history = 7 }   # cron Quartz из 6 полей
                                                               # keep-history необязателен: нет = хранить все
  }
}
```

`log.rotate.cron` — выражение **Quartz из 6 полей**
(`sec min hour day-of-month month day-of-week`). Оно превращается в
`io.fom.config.CronSnapshotPolicy`: каждый снапшот планируется на ближайшее
время срабатывания cron (часовой пояс JVM по умолчанию), а следующее
пересчитывается после каждого срабатывания. Поток планировщика движка никогда
не блокируется; слот, наступивший, пока предыдущий снапшот ещё идёт,
пропускается. Значение `never` (или пусто) оставляет ротацию `Disabled`.
`log.rotate` должен быть объектом с ключом `cron` — ротацию выключает только явное
`cron = never` (или пустое значение), а если `log.rotate` не задан вовсе, она
тоже `Disabled`. Скаляр (`log.rotate = "0 0 3 * * ?"`), объект без `cron` или
неизвестный ключ вроде опечатки `corn` приводят к `ConfigException` с полным
ключом (`engine.system.log.rotate: must be an object ...`,
`engine.system.log.rotate.cron: missing ...`,
`engine.system.log.rotate.corn: unknown key 'corn' ...`), а не к молчаливому
отключению ротации.

**Хранение архивов.** `log.rotate.keep-history` необязателен. Если его нет (или
он равен `all`), **хранятся** все архивы, которые оставляют снапшоты: политика
получает `keepHistory = SnapshotPolicy.KEEP_ALL` и вообще не запускает чистку.
Число `>= 1` удаляет после каждого снапшота все архивы, кроме стольких новейших.
Хранимые архивы растят занятое место без ограничений (каждый — примерно размером
с лог, который он заменил), поэтому либо задайте `keep-history`, либо удаляйте
архивы сами через `engine.purgeArchives(n)`; см.
[Снапшоты — Хранение архивов](../concepts/snapshots.md#archive-retention).

**Часовой пояс и летнее время.** Политика, построенная из HOCON, создаётся
конструктором с двумя аргументами `CronSnapshotPolicy(String expression, int keepHistory)`
(с `KEEP_ALL`, если `keep-history` не задан), который вычисляет cron в `ZoneId.systemDefault()`. Ключа для часового пояса в
HOCON нет — `log.rotate` принимает только `cron` и `keep-history`, поэтому ключ
`zone = UTC` отвергается как неизвестный. Чтобы зафиксировать пояс, либо
запускайте JVM с `-Duser.timezone=UTC`, либо замените разобранную политику
конструктором с тремя аргументами:

```java
EngineConfig cfg = EngineConfigHocon.parse()
        .withSnapshotPolicy(new CronSnapshotPolicy("0 0 3 * * ?", 7, ZoneOffset.UTC));
// или хранить все архивы: new CronSnapshotPolicy("0 0 3 * * ?", ZoneOffset.UTC)
```

В поясе с переходом на летнее время следующий слот берётся из `nextExecution`
библиотеки `cron-utils`: слот, попавший в «пропущенный» час весеннего перехода
(например, `02:30` в ночь, когда часы прыгают с 02:00 на 03:00), в этот день
**пропускается**, а слот с фиксированным временем в повторяющемся часе осеннего
перехода срабатывает **один раз** (ежечасный cron, однако, срабатывает в обеих
копиях повторяющегося часа). Рекомендуем вычислять cron в **UTC** — там нет ни
того, ни другого.

Оба пространства имён **строгие**: каждый ключ внутри `engine.graph.default` и
`engine.system` должен быть одним из перечисленных выше. Опечатка в любом месте —
в родителе уровнем выше (`engine.system.logs.rotate { ... }`) или в листе
(`engine.system.query-timout = 1s`) — приводит к `ConfigException` с полным
неизвестным ключом и списком допустимых, например
`engine.system.query-timout: unknown key 'query-timout' in engine.system (allowed: dedup-window, log, query-timeout)`,
а не игнорируется с молчаливым применением значения по умолчанию. Скаляр на месте
одного из этих объектов (или объекта на пути к нему) сообщается по самому этому
ключу: `engine.system = "fast"` падает с
`engine.system: must be an object, was string "fast" (...)`. Ключи вне этих
двух объектов — собственные настройки приложения и даже другие ключи `engine.*` —
не проверяются.

Выражение, которое не разбирается, сразу падает с `IllegalArgumentException`, где
названа сама строка, — вместо собственного «Failed to parse cron expression» от
`cron-utils`: `Invalid Quartz cron expression '0 */6 * * *' — this looks like a
5-field Unix cron; Quartz starts with seconds, so prepend a seconds field and use
'?' for one day field (e.g. '0 0 */6 * * ?')`. Предложенная
форма показывается, только если она действительно разбирается: Quartz не допускает `*`
сразу в обоих полях дня, поэтому простой префикс секунд (`'0 0 */6 * * *'`) не
подошёл бы; если простого исправления нет, подсказка лишь советует поставить `?` в
day-of-month или в day-of-week. Для **числового** дня недели конкретной формы
тоже не предлагается: Unix считает с воскресенья = 0 (понедельник = 1), Quartz — с
воскресенья = 1 (понедельник = 2), поэтому перенесённый как есть `'30 2 * * 1'`
сработал бы в воскресенье, — подсказка даёт общий совет и отмечает, что имена
вроде `MON-FRI` в обоих диалектах значат одно и то же. Любое другое число полей сообщается так же — с перечнем шести
полей Quartz и тем, сколько их передали. `keep-history` должен быть `>= 1` или
`all` (либо отсутствовать — то же, что `all`).

Каждая ошибка `EngineConfigHocon.parse` начинается с **полного HOCON-ключа**, к
которому относится, а исходное исключение сохраняется как причина — например,
`engine.graph.default.init.min-backoff: must be > 0, was PT0S`,
`engine.graph.default.init.max-backoff: must be >= engine.graph.default.init.min-backoff (PT10S), was PT1S`,
`engine.system.log.rotate.cron: Invalid Quartz cron expression ...`. Неразбираемое
значение или значение не того типа (`init.timeout = soon`) даёт
`com.typesafe.config.ConfigException`, корректное по форме, но недопустимое —
`IllegalArgumentException`.

!!! warning "cron из недоверенного ввода"
    Cron-строка парсится `cron-utils`. Не подавайте cron из недоверенных
    источников без валидации. См. [Безопасность](../security.md).

## Горячая перезагрузка { #hot-reload }

Сменить живой конфиг во время выполнения:

```java
engine.updateConfig(newConfig);
```

Операции «в полёте» сохраняют захваченные значения; последующие читают новый
конфиг. Если изменился `snapshotPolicy`, запланированная ротация отменяется и
переустанавливается под новую политику. `updateConfig` сериализуется относительно
`newGraph` control-lock'ом движка.

## Типизированные ячейки свойств

Не связано с `EngineConfig`, но полезно: внутри `init`/`load` можно типобезопасно
читать и писать сохраняемые ячейки через `Properties`, `TypedKey` и `Codec`:

```java
import io.fom.*;
import io.fom.Properties; // не java.util.Properties
import java.util.*;

static final TypedKey<String> NAME = new TypedKey<>("name", Codecs.stringCodec());
static final TypedKey<Long>   VER  = new TypedKey<>("ver",  Codecs.longCodec());

// в init (Properties неизменяем: каждый put возвращает новый экземпляр):
Map<String, byte[]> cells = Properties.empty()
    .put(NAME, "stations")
    .put(VER, 42L)
    .asRaw();

// в load (properties — Map<String, byte[]>, переданная в load):
var props = Properties.of(properties);
String name = props.get(NAME);
long ver    = props.get(VER);    // бросает NoSuchPropertyException, если отсутствует
Optional<Long> maybe = props.find(VER);
```

`Codecs` предоставляет `stringCodec()`, `longCodec()`, `intCodec()` и `uri()`;
для своих типов реализуйте `Codec<T>`. Сырые ячейки по-прежнему доступны через
`putRaw`/`getRaw`.

> [English version](../../guides/configuration.md)
