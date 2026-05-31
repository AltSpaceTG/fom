# Мультитенантность

Частый паттерн — один процесс **на тенант** (например, `Inventory_PUB1`,
`Inventory_PUB2`), маршрутизируемый [`Routable`](../concepts/graph-and-routing.md)-сообщением
или динамическим маршрутом. Модуль `fom-tenant` добавляет **обёртку авторизации
defence-in-depth**, чтобы вызывающий мог трогать только разрешённые ему тенанты.

!!! info "Это обёртка, а не замена"
    `TenantAwareEngine` делегирует обычному `Engine`. Движок остаётся источником
    истины; обёртка добавляет проверку authz поверх `query` / `queryProcess` /
    `trigger` / `shutdownTenant`.

## Построение обёртки

```java
import io.fom.tenant.*;

var aware = TenantAwareEngine.builder(engine)
    .tenantResolver(TenantResolver.suffixAfter("_"))                  // "Inventory_PUB1" → "PUB1"
    .authzPolicy((caller, tenant) -> caller.tenants().contains(tenant))
    .build();
```

- **`TenantResolver`** отображает имя процесса в `TenantId`.
  `TenantResolver.suffixAfter("_")` берёт часть после последнего `_`; реализуйте
  интерфейс для любого другого соглашения (есть и `regex(...)`, `of(...)`). Имя,
  не разрешающееся ни в какой тенант (например, глобальный процесс), обходит
  проверку.
- **`authzPolicy`** — `BiPredicate<TenantCaller, TenantId>`, решающий, может ли
  вызывающий действовать с тенантом.

!!! danger "По умолчанию — запрет всего"
    Если не задать `authzPolicy`, обёртка **запрещает всё** — fail-closed. Доступ
    нужно разрешать явно.

## Вызывающие и тенанты

```java
TenantId pub1 = TenantId.of("PUB1");
TenantCaller alice = TenantCaller.of("alice", pub1);     // может действовать с PUB1
TenantCaller anon  = TenantCaller.anonymous();           // нет тенантов
```

## Авторизованные операции

```java
// query: принимаются только Routable-сообщения (цель известна заранее)
aware.query(alice, new GetInventory("PUB1"));            // ok, если у alice есть PUB1

// явная адресация
aware.queryProcess(alice, "Inventory_PUB1", msg);

// триггер
aware.trigger(alice, "Inventory_PUB1", new RefreshSignal("x"));

// жизненный цикл по тенанту
aware.shutdownTenant(alice, pub1, Duration.ofSeconds(30));
```

### Почему `query` отклоняет не-`Routable` сообщения

`aware.query(caller, msg)` должен знать целевой тенант *до* диспетчеризации, чтобы
авторизовать. `Routable`-сообщение несёт `targetProcess()`, поэтому тенант
известен. Не-`Routable` сообщение было бы маршрутизировано по типу *внутри* движка
в процесс, который обёртка не проверяла — дыра fail-open. Поэтому `query`
отклоняет не-`Routable` сообщения с `TenantAccessDeniedException`; для явной
адресации используйте `queryProcess(caller, name, msg)`.

## Отказ в доступе

Отклонённый вызов завершает возвращённый stage с `TenantAccessDeniedException`
(для `query`/`queryProcess`/`shutdownTenant`) или бросает его напрямую (для
`trigger`):

```java
aware.query(anon, new GetInventory("PUB1"))
    .whenComplete((r, err) -> {
        if (err instanceof TenantAccessDeniedException) { /* 403 */ }
    });
```

## Сводка дефолтов

| Настройка | По умолчанию | Следствие |
|---|---|---|
| `tenantResolver` | `suffixAfter("_")` | имя `X_TENANT` → тенант `TENANT` |
| `authzPolicy` | `(c, t) -> false` | **запрет всего**, пока не настроено |
| не-`Routable` `query` | отклоняется | используйте `queryProcess` |

См. [Безопасность](../security.md) о более широкой модели угроз.

> [English version](../../guides/multi-tenancy.md)
