# Установка

FOM — многомодульная библиотека. Всегда подключайте **`fom-core`**; опциональные
модули добавляйте по необходимости.

!!! warning "Пока не в Maven Central"
    Координаты ниже используют group id `io.fom` и текущую версию разработки
    `0.1.0-SNAPSHOT`. Пока артефакты не опубликованы, подключайте модули из
    локальной сборки (`./gradlew publishToMavenLocal`) или через composite/included
    build.

## Требования

- **JDK 21+** во время выполнения (ядро использует виртуальные потоки и
  pattern matching).
- Сборка Gradle нацелена на **toolchain JDK 21** и проверяется на более новых JDK
  через foojay toolchain resolver.
- Интеграционные тесты `fom-jdbc` требуют рабочего демона **Docker**
  (Testcontainers).

## Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("io.fom:fom-core:0.1.0-SNAPSHOT")

    // Рекомендуемый сериализатор для продакшена:
    implementation("io.fom:fom-fury:0.1.0-SNAPSHOT")

    // Любые опциональные модули:
    implementation("io.fom:fom-kotlin:0.1.0-SNAPSHOT")        // Kotlin DSL + корутины
    implementation("io.fom:fom-config-hocon:0.1.0-SNAPSHOT")  // парсинг HOCON-конфига
    implementation("io.fom:fom-micrometer:0.1.0-SNAPSHOT")    // метрики
    implementation("io.fom:fom-otel:0.1.0-SNAPSHOT")          // трассировка
    implementation("io.fom:fom-jdbc:0.1.0-SNAPSHOT")          // бэкенд Postgres
}
```

## Maven

```xml
<dependency>
  <groupId>io.fom</groupId>
  <artifactId>fom-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>io.fom</groupId>
  <artifactId>fom-fury</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Карта модулей

| Модуль | Добавляйте, когда нужно… |
|---|---|
| `fom-core` | всегда — рантайм, FSM, граф, SPI лога, in-memory + файловый бэкенды, Java SerDe |
| `fom-fury` | компактный, быстрый, дружественный к эволюции схемы бинарный сериализатор (рекомендуется для прода) |
| `fom-config-hocon` | строить `EngineConfig` из HOCON-файла |
| `fom-kotlin` | DSL `graph { … }` и `suspend`-мосты |
| `fom-guice` / `fom-spring` | разрешать фабрики процессов из DI-контейнера |
| `fom-micrometer` / `fom-otel` | метрики / распределённая трассировка |
| `fom-jdbc` | лог на Postgres для координации лидерства между узлами |
| `fom-tenant` | обёртка авторизации по тенантам поверх движка |
| `fom-log` | автономный CLI для инспекции/диагностики файлов лога |
| `fom-log-maintenance` | политики снапшотов по размеру и составные |
| `fom-test` | переиспользуемые тест-контракты |

Полный каталог — в [справочнике модулей](../reference/modules.md).

## JPMS

Каждый модуль поставляет `module-info.java`. `fom-core` — это `module io.fom.core`
и экспортирует `io.fom`, `io.fom.api`, `io.fom.log`, `io.fom.serde`. Если ваше
приложение модульное — `requires io.fom.core;` (и нужные интеграционные модули).

> [English version](../../getting-started/installation.md)
