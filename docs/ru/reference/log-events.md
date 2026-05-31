# Справочник событий лога

Каждое долговечное изменение — это `LogEvent` (sealed-интерфейс,
`extends Serializable`). Все несут `int clock()`, `long timestamp()` (epoch ms) и
`short formatVersion()`. См. [Лог](../concepts/the-log.md) о том, как они
сочетаются.

## Общая форма

```java
public sealed interface LogEvent extends Serializable
        permits LogLeader, LogChangeGraph, LogInitialized, LogLoaded,
                LogTrigger, LogDependencyChanged, LogDead, LogCleanedUp, LogSnapshot {
    int clock();
    long timestamp();
    short formatVersion();
}
```

## Базовые события

Стабильны навсегда в пределах мажорной версии.

### LogLeader
`(clock, timestamp, formatVersion, String instanceId)` — экземпляр JVM заявил
лидерство. Последнее называет текущего лидера и гейтит все append'ы.

### LogChangeGraph
`(clock, timestamp, formatVersion, byte[] serializedGraph)` — граф установлен или
заменён. `serializedGraph` производится `SerDe.serializeProcessGraph`.

### LogInitialized
`(clock, timestamp, formatVersion, String processName, Map<String, byte[]> properties)`
— процесс завершил `init`. Определяет [Sid](../concepts/sid-and-clock.md):
`sid() == new Sid(processName, clock)`. `properties` — сохранённые ячейки (глубоко
копируются при конструировании).

### LogLoaded
`(clock, timestamp, formatVersion, Sid sid)` — процесс завершил `load` и теперь в
Serving.

### LogTrigger
`(clock, timestamp, formatVersion, Map<String, byte[]> values)` — сработал один
или несколько триггеров (`processName → сериализованное значение триггера`),
записаны атомарно.

### LogDead
`(clock, timestamp, formatVersion, Sid sid)` — данный Sid отозван (reinit или
replace). Потребители должны переключиться на новый живой Sid.

## Расширенные события

Могут пропускаться старым читателем с предупреждением.

### LogDependencyChanged
`(clock, timestamp, formatVersion, Sid sid, String depName, int oldDepClock, int newDepClock)`
— реактивная зависимость `depName` у `sid` сменила clock Sid с `oldDepClock` на
`newDepClock`. Записывается для трассировки и дедупа.

### LogCleanedUp
`(clock, timestamp, formatVersion, Sid sid, boolean ok)` — `cleanUp` завершился
(`ok=true`) или бросил/упал по таймауту (`ok=false`) для `sid`.

### LogSnapshot
`(clock, timestamp, formatVersion, int checkpointClock)` — маркер границы ротации,
эмитится в начале [снапшота](../concepts/snapshots.md); `checkpointClock` — это
последний clock, вошедший в исходное состояние снапшота.

## Переписывание clock

При добавлении `clock` события рекомендательный; бэкенд перезаписывает его
следующей позицией лога. `ClockRewriter.withClock(event, clock)` пересобирает
запись с назначенным clock'ом — все бэкенды используют его, поэтому **первое
событие в пустом логе всегда clock 0**.

## Типичный жизненный цикл в логе

```text
0  LogLeader        (экземпляр заявляет лидерство)
1  LogChangeGraph   (граф установлен)
2  LogInitialized   A  → Sid(A,2)
3  LogLoaded        A
6  LogTrigger       {A: refresh}
7  LogDead          Sid(A,2)            (A переинициализируется)
8  LogInitialized   A  → Sid(A,8)
9  LogLoaded        A
10 LogDependencyChanged  Sid(B,4) depName=A 2→8   (B реактивен)
```

> [English version](../../reference/log-events.md)
