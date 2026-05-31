# Публикация в Maven Central

Проект настроен на публикацию всех 13 модулей в **Maven Central** через
**Central Portal** (поток `central.sonatype.com`, заменивший OSSRH). Сборка Gradle
уже производит для каждого модуля `jar` + `sources` + `javadoc` + `.pom` (и
`.asc`-подписи, когда настроен ключ) и собирает их в один бандл для загрузки — вам
нужно лишь предоставить аккаунт, подтверждённый namespace и ключ подписи.

## Namespace

Сборка публикует под **`io.github.altspacetg`** — group id в корневом
`build.gradle.kts`, выведенный из GitHub-аккаунта `AltSpaceTG`. Форма
`io.github.<user>` подтверждается бесплатно доказательством владения аккаунтом;
домен не нужен. Публикуете под другим аккаунтом/организацией? Измените `group` в
корневом build.

## Разовая настройка

### 1. Аккаунт Central Portal + namespace

1. Войдите на [central.sonatype.com](https://central.sonatype.com/) через GitHub.
2. **Add namespace** → `io.github.altspacetg`. Для `io.github.*` Портал попросит
   создать публичный GitHub-репозиторий с именем, равным показанному коду
   подтверждения. Создайте его, нажмите **Verify**.
3. **Account → Generate User Token** → пара username/password. Это и есть
   `mavenCentralUsername` / `mavenCentralPassword`.

### 2. PGP-ключ подписи

Central требует подписи каждого артефакта.

```bash
gpg --full-generate-key                                   # RSA 4096, без срока — нормально
gpg --list-secret-keys --keyid-format=long                # узнать id ключа
gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID> # опубликовать публичную половину
gpg --armor --export-secret-keys <KEY_ID> > fom-signing-key.asc   # экспортировать секретную
```

### 3. Положите учётные данные туда, где их найдёт Gradle

Никогда не коммитьте секреты. Используйте **пользовательские** свойства Gradle
(`~/.gradle/gradle.properties`) — вне репозитория:

```properties
mavenCentralUsername=<portal token username>
mavenCentralPassword=<portal token password>

# вставьте armored-ключ с литеральными \n вместо переводов строк:
signingInMemoryKey=-----BEGIN PGP PRIVATE KEY BLOCK-----\n...\n-----END...
signingInMemoryKeyPassword=<пароль ключа>
```

`gradle.properties.example` в репозитории перечисляет те же ключи как
напоминание. Корневой `gradle.properties`, а также `*.asc`/`*.gpg`/`secrets.properties`
— в `.gitignore`, чтобы настоящий ключ нельзя было случайно закоммитить. В CI
предпочитайте переменные окружения: `ORG_GRADLE_PROJECT_mavenCentralUsername`,
`ORG_GRADLE_PROJECT_signingInMemoryKey` и т. д.

## Задайте релизную версию

Central **отклоняет версии `-SNAPSHOT`** и не примет одну версию дважды. Задайте
реальную версию в корневом `build.gradle.kts`:

```kotlin
version = "0.1.0"   // было 0.1.0-SNAPSHOT
```

## Соберите бандл для загрузки

```bash
./gradlew clean build            # сначала зелёная сборка (тесты, -Werror, все модули)
./gradlew centralBundle          # стейджит + подписывает каждый модуль, зипует один бандл
```

Получится **`build/central-bundle.zip`** в требуемой Maven-раскладке —
`io/github/altspacetg/<module>/<version>/…` с `.jar`, `-sources.jar`,
`-javadoc.jar`, `.pom`, контрольными суммами `.md5/.sha*` и `.asc`-подписями для
каждого (подписи появляются только когда настроен ключ). Тестовый артефакт
test-fixtures модуля `fom-core` намеренно исключён.

!!! note "Что делает `centralBundle`"
    Каждый библиотечный модуль публикуется в общий каталог `build/staging-deploy`
    через встроенный `maven-publish` Gradle; `signing` прикрепляет `.asc`-файлы,
    когда ключ настроен; корневая задача `centralBundle` зипует каталог. Ни одного
    стороннего Gradle-плагина — совместимо с Gradle 9 проекта.

## Загрузка и релиз

1. В Портале: **Publish → Upload a Deployment**, выберите
   `build/central-bundle.zip`.
2. Портал валидирует бандл (полнота POM, подписи, наличие sources + javadoc).
   Исправьте указанное и загрузите заново.
3. Когда валидация пройдёт, нажмите **Publish**. Первые релизы под новым
   namespace могут какое-то время доходить до `repo1.maven.org` и поиска.

После релиза потребители подключают зависимость как в
[установке](../getting-started/installation.md), с вашей релизной версией.

## Локальный тест без Central

`./gradlew publishToMavenLocal` ставит каждый модуль в `~/.m2`, так что другой
локальный проект может разрешить `io.github.altspacetg:fom-core:<version>` ещё до
обращения к Порталу.

## Чек-лист

- [ ] Namespace `io.github.altspacetg` подтверждён в Central Portal.
- [ ] Токен пользователя Портала в `~/.gradle/gradle.properties`.
- [ ] PGP-ключ сгенерирован, публичная половина на keyserver, секретная настроена.
- [ ] `version` задана без `-SNAPSHOT` (инкремент на каждый релиз).
- [ ] `./gradlew clean build` зелёная.
- [ ] `./gradlew centralBundle` → создан `build/central-bundle.zip` с подписями `.asc`.
- [ ] Бандл загружен и опубликован в Портале.

> [English version](../../guides/publishing.md)
