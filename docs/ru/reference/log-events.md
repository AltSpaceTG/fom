# Справочник событий лога

Каждое долговечное изменение — это `LogEvent` (sealed-интерфейс,
`extends Serializable`). Все несут `long clock()`, `long timestamp()` (epoch ms) и
`short formatVersion()`. См. [Лог](../concepts/the-log.md) о том, как они
сочетаются.

## Общая форма

```java
public sealed interface LogEvent extends Serializable
        permits LogLeader, LogChangeGraph, LogInitialized, LogLoaded,
                LogTrigger, LogDependencyChanged, LogDead, LogCleanedUp, LogSnapshot,
                LogPaused, LogResumed {
    long clock();
    long timestamp();
    short formatVersion();
}
```

!!! warning "Упорядочивайте по `clock`, а не по `timestamp`"
    `clock` назначается бэкендом в момент добавления и строго возрастает.
    `timestamp` — лишь контекст по стенным часам: он берётся через
    `System.currentTimeMillis()` там, где событие *создаётся*, до того как оно
    встанет в очередь на добавление. Поэтому при параллельных добавлениях
    колонка timestamp может идти назад, пока `clock` продолжает расти (замер на
    Postgres: 490 инверсий на 6472 события, худшая — 154 мс). Никогда не
    сортируйте, не сравнивайте и не дедуплицируйте события лога по `timestamp`.

## Базовые события

Стабильны навсегда в пределах мажорной версии.

### LogLeader
`(clock, timestamp, formatVersion, String instanceId)` — экземпляр JVM заявил
лидерство. Последнее называет текущего лидера и гейтит все append'ы.

### LogChangeGraph
`(clock, timestamp, formatVersion, List<Node> nodes)` — граф установлен или
заменён. Каждый `Node(String name, List<String> reactiveDependencies,
List<String> stableDependencies, byte[] param)` описывает определение одного
узла; `param` производится `SerDe.serializeParam` (или `null`). С этим сравнивает
рестарт, решая, подходит ли сохранённое состояние узлу. Фабрики и маршруты в лог
**не** пишутся — они живут в коде. `newGraph`, не меняющий ни узлов, ни
маршрутов, это событие не пишет. Сериализованный `param` хранится как есть и
доступен любому, кто может читать лог, — не кладите секреты в параметры
([Безопасность](../security.md#data-retention)).

### LogInitialized
`(clock, timestamp, formatVersion, String processName, Map<String, byte[]> properties, Sid replaces)`
— процесс завершил `init`. Определяет [Sid](../concepts/sid-and-clock.md):
`sid() == new Sid(processName, clock)`. `properties` — сохранённые ячейки (глубоко
копируются при конструировании).

`replaces` — всё ещё обслуживающая версия, которую этот init должен заменить
([reinit при `KEEP_OLD`](../concepts/process-lifecycle.md#переинициализация)), или
`null` для холодного init и для любого reinit при `RELEASE_FIRST`. Такой init —
**кандидат**: заменяемый Sid остаётся живым, пока его не продвинет `LogLoaded`
кандидата. Рестарт, нашедший кандидата без его `LogLoaded`, обслуживает
заменяемую версию и загружает кандидата в фоне, без нового init, — если только
узел теперь не работает с `RELEASE_FIRST`: тогда кандидат снимается, а узел
инициализируется холодно (см.
[Идемпотентный рестарт](../concepts/idempotent-restart.md#restart-mid-reinit)). `replaces`
должен называть тот же процесс. Конструкторы без `replaces` сохранены и передают
`null`.

### LogLoaded
`(clock, timestamp, formatVersion, Sid sid)` — процесс завершил `load` и теперь в
Serving. Для кандидата это и есть надёжное переключение: с этого момента живой —
его Sid, даже если `LogDead` старого так и не запишется.

### LogTrigger
`(clock, timestamp, formatVersion, List<String> processNames)` — запрошена
переинициализация этих процессов (`engine.trigger(...)` или watcher); записывается
атомарно до её выполнения. При рестарте процесс, у которого последний
`LogTrigger` (или `LogDependencyChanged`) новее живого `LogInitialized` (и его
кандидата, если он есть), после тёплой загрузки переинициализируется — запрос повторяется. Триггер для процесса в
`Dead` записывает свой `LogTrigger` так же, до перезапуска, а `trigger(Map)`
пишет **одну** запись со всеми запрошенными именами, включая `Dead`. Значения
триггеров не записываются.

Во время reinit (при любой стратегии) `LogTrigger` с именем процесса может также
**идти после** `LogInitialized` новой версии. Его пишет сам движок, а не вызов
`trigger()`: у запроса (триггера или изменения зависимости), пришедшего, пока
строилась новая версия, собственная запись стоит *до* этого `LogInitialized`, и
рестарт счёл бы его выполненным. Повторная запись после нового состояния
заставляет рестарт его воспроизвести. На один reinit пишется одна такая запись,
сколько бы запросов ни накопилось.

### LogDead
`(clock, timestamp, formatVersion, Sid sid)` — данный Sid отозван (reinit, replace
или кандидат, который уже не будет обслуживать). Потребители должны
переключиться на новый живой Sid. После reinit при `KEEP_OLD` он идёт за
`LogLoaded` новой версии и лишь наводит порядок в логе: промоушен уже случился.

## Расширенные события

Могут пропускаться старым читателем с предупреждением.

### LogDependencyChanged
`(clock, timestamp, formatVersion, Sid sid, String depName, long oldDepClock, long newDepClock)`
— реактивная зависимость `depName` у `sid` сменила clock Sid с `oldDepClock` на
`newDepClock`. Записывается для трассировки и дедупа.

### LogCleanedUp
`(clock, timestamp, formatVersion, Sid sid, boolean ok)` — `cleanUp` завершился
(`ok=true`) или бросил/упал по таймауту (`ok=false`) для `sid`.

### LogSnapshot
`(clock, timestamp, formatVersion, long checkpointClock)` — маркер границы ротации,
**последнее** событие, которое пишет [снапшот](../concepts/snapshots.md);
`checkpointClock` — последний clock лога, из которого построен снапшот.

## События паузы

Последнее `LogPaused` / `LogResumed` для процесса решает, поднимется ли он на
паузе после рестарта. См. [паузу процессов](../concepts/graph-swap.md).

### LogPaused
`(clock, timestamp, formatVersion, String processName, boolean stale, boolean forDependency)`
— процесс поставлен на паузу: остановлен, сохранённое состояние оставлено.
Пишется ещё раз со `stale=true`, когда во время паузы приходит триггер или
изменение реактивной зависимости: при возобновлении процесс должен
переинициализироваться. `forDependency=true` значит, что паузу никто не
запрашивал: движок поставил процесс на паузу при старте только потому, что на
паузе была его зависимость, и после рестарта процесс стартует сам, как только ни
одна зависимость не на паузе. Снапшот заново пишет по одному на каждый процесс
на паузе, сохраняя оба флага.

### LogResumed
`(clock, timestamp, formatVersion, String processName)` — пауза закончилась:
процесс возобновлён или удалён из графа, пока был на паузе.

## Переписывание clock

При добавлении `clock` события рекомендательный; бэкенд перезаписывает его
значением `LogClocks.nextClock(lastEvent)` — clock последнего события + 1
или `0` в пустом логе. `LogClocks.withClock(event, clock)` пересобирает
запись с назначенным clock'ом. Все бэкенды используют их, поэтому clock'и строго
возрастают и никогда не повторяются; `compact` сохраняет переданные ему clock'и
(см. [Sid и clock](../concepts/sid-and-clock.md#sid-и-компактизация)). Пример
ниже — лог без компакции, где clock и позиция совпадают.

## Типичный жизненный цикл в логе

```text
0  LogLeader        (экземпляр заявляет лидерство)
1  LogChangeGraph   (граф установлен)
2  LogInitialized   A  → Sid(A,2)
3  LogLoaded        A
4  LogInitialized   B  → Sid(B,4)
5  LogLoaded        B
6  LogTrigger       [A]
7  LogInitialized   A  → Sid(A,7) replaces=Sid(A,2)   (Sid(A,2) ещё обслуживает)
8  LogLoaded        Sid(A,7)            (переключение)
9  LogDead          Sid(A,2)
10 LogDependencyChanged  Sid(B,4) depName=A 2→7   (B реактивен)
11 LogCleanedUp     Sid(A,2) ok         (после того как его вычисления закончились)
12 LogInitialized   B  → Sid(B,12) replaces=Sid(B,4)
13 LogLoaded        Sid(B,12)
14 LogDead          Sid(B,4)
15 LogCleanedUp     Sid(B,4) ok
```

События 9–11 могут прийти в другом порядке: каскад идёт в своём потоке, а
очистка старой версии ждёт её вычислений. При `RELEASE_FIRST` reinit пишет
`LogDead(old)` → `LogCleanedUp(old)` → `LogInitialized(new)` (без `replaces`) →
`LogLoaded(new)`. Запрос, пришедший, пока строится новая версия, добавляет
`LogTrigger [A]` сразу после её `LogInitialized` (см. [LogTrigger](#logtrigger)).

> [English version](../../reference/log-events.md)
