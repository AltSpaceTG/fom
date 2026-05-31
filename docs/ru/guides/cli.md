# CLI `fom-log`

`fom-log` — автономный инструмент командной строки для инспекции, диагностики и
(в будущем) миграции файлов `LocalFileLogBackend`. Удобен для офлайн-отладки
продакшен-лога.

## Команды

```text
fom-log inspect  <path>                         # сводка + счётчики событий по типам
fom-log diagnose <path>                         # проверка CRC по фреймам; отчёт об обрезке
fom-log migrate  --from <v> --to <v> <in> <out> # межмажорная миграция (заглушка)
```

### inspect

Печатает id лога, длину, текущего лидера, время последнего события и счётчик
событий по типам:

```text
$ fom-log inspect /var/lib/fom/orders.bin
Log: /var/lib/fom/orders.bin
Length: 128 event(s)
Current leader: fom-7f3c…
Last event timestamp: 1737045123456
Counts by type:
  LogLeader                 3
  LogInitialized           40
  …
```

### diagnose

Открывает файл (при открытии обрезается любой повреждённый хвост), затем обходит
каждое событие, проверяя round-trip. Сообщает точку обрезки и число дефектных
событий; завершается с ненулевым кодом, если такие найдены.

```text
$ fom-log diagnose /var/lib/fom/orders.bin
Opened: /var/lib/fom/orders.bin
Length after open (any corrupt tail was truncated): 128
Diagnose OK: all 128 events round-tripped.
```

### migrate

Заглушка для межмажорной миграции формата. Текущий формат — единственный
выпущенный мажор, цели миграции пока нет; команда завершается с ненулевым кодом и
объясняет это.

## Запуск

`fom-log` собирает исполняемый дистрибутив (через Gradle-плагин `application`):

```bash
./gradlew :fom-log:installDist
./fom-log/build/install/fom-log/bin/fom-log inspect /path/to/log.bin
```

!!! danger "Считайте входные файлы доверенными"
    `inspect`/`diagnose` десериализуют полезные нагрузки лога. Файловый бэкенд
    применяет allowlist-фильтр десериализации (только `io.fom.*` + JDK-типы), а
    `diagnose` в основном проверяет CRC — но запускайте CLI только на файлах,
    которым доверяете. См. [Безопасность](../security.md).

> [English version](../../guides/cli.md)
