# Бэкенды хранения

`LogBackend` — это место, где живёт [append-only лог](../concepts/the-log.md). Три
поставляются в комплекте; можно написать свой против SPI.

| Бэкенд | Модуль | Хранит? | Лидерство | Для чего |
|---|---|---|---|---|
| `InMemoryLogBackend` | `fom-core` | нет | внутри процесса | тесты, быстрый старт, эфемерное состояние |
| `LocalFileLogBackend` | `fom-core` | да (файл) | ОС-блокировка файла | одноузловое долговечное состояние |
| `PostgresLogBackend` | `fom-jdbc` | да (таблица) | `pg_advisory_lock` | координация лидерства между узлами |

Все три соблюдают один [контракт `LogBackend`](../concepts/the-log.md#spi-logbackend)
(атомарный append, один лидер, параллельные чтения, плотные clock'и с нуля) и
проверяются общим `LogBackendContractTest`.

## InMemoryLogBackend

```java
try (var backend = new InMemoryLogBackend()) { … }
try (var backend = new InMemoryLogBackend("my-log-id")) { … }
```

На основе `CopyOnWriteArrayList`. Ничего не переживает JVM — каждый старт
холодный. Идеален для тестов.

## LocalFileLogBackend

```java
try (var backend = new LocalFileLogBackend(Path.of("/var/lib/fom/log.bin"))) { … }
```

- Каждое событие обрамлено `[int length][int CRC32][payload]`; файл начинается с
  4-байтовой магии (`FOM\1`), чтобы неверный файл падал явно.
- При открытии весь файл сканируется; первый невалидный/несовпавший по CRC фрейм
  обрезает хвост (восстановление после частичной записи при крахе).
- **Лидерство** — эксклюзивная ОС-блокировка соседнего файла `<name>.lock`,
  берётся при конструировании. Второй процесс, открывающий тот же путь, падает с
  `IllegalStateException`.
- **Компактизация** пишет новый файл, копирует старый в
  `<name>.archived.<timestamp>`, атомарно заменяет живой файл и делает fsync
  каталога, чтобы переименование было долговечным при потере питания.

Этот бэкенд владеет своими файлами; путь — доверенный ввод от приложения.

## PostgresLogBackend

`fom-jdbc`. Хранит события в одной таблице и координирует лидерство advisory-локом
Postgres — основа для [нескольких узлов](multi-node.md).

```java
import io.fom.jdbc.PostgresLogBackend;

var backend = new PostgresLogBackend(dataSource, "orders");
// или явно указать имя таблицы:
var backend2 = new PostgresLogBackend(dataSource, "orders", "fom_log_orders");
```

Схема (создаётся при отсутствии):

```sql
CREATE TABLE IF NOT EXISTS <table> (
  clock     SERIAL PRIMARY KEY,
  type      TEXT   NOT NULL,
  payload   BYTEA  NOT NULL,
  ts_millis BIGINT NOT NULL
);
```

- Конструктор берёт `pg_try_advisory_lock(hash64(tableName))` на выделенном
  соединении, удерживаемом до `close()`. Второй экземпляр, открывающий ту же
  таблицу, быстро падает с `IllegalStateException` («advisory lock»). Соединение
  (и блокировка) освобождается, даже если конструирование падает на полпути.
- Внутренний столбец `clock` — `SERIAL` с единицы, но видимый движку clock
  соблюдает контракт SPI **с нуля**.
- Чтения адресуют строки по **позиции** (`ORDER BY clock OFFSET ?`), поэтому
  позиционная адресация переживает компактизацию (которая пересоздаёт таблицу).
- `getBetween` выполняет проверку границ и чтение диапазона в одной транзакции
  `REPEATABLE READ`, поэтому конкурентный append/compact не может укоротить
  результат.
- `DataSource` внедряете вы (используйте пул). Идентификаторы валидируются и
  ограничены по длине (63-байтовый `NAMEDATALEN` Postgres).

!!! note "Testcontainers"
    Интеграционные тесты `fom-jdbc` работают против реального Postgres через
    Testcontainers и потому требуют демона Docker. `PostgresLogBackendTest`
    прогоняет общий контракт бэкенда плюс тесты advisory-lock/reopen.

## Свой бэкенд

Реализуйте `io.fom.log.LogBackend` и соблюдайте
[контракт](../concepts/the-log.md#spi-logbackend). Чтобы получить тесты
инвариантов бесплатно, унаследуйте общий `LogBackendContractTest` (выставлен из
test-fixtures `fom-core`) — см. [Тестирование](testing.md).

> [English version](../../guides/persistence-backends.md)
