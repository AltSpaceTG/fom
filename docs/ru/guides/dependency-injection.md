# Внедрение зависимостей

Фабрики процессов — это `SerializableSupplier`'ы; они сериализуются в граф в логе,
поэтому не могут напрямую захватывать живые объекты вроде бина Spring или сервиса
Guice. Модули `fom-guice` и `fom-spring` решают это: каждый даёт **сериализуемый**
суплаер, который замыкает только целевой `Class` и разрешает экземпляр из
**статически зарегистрированного контейнера** во время вызова.

## Guice

```java
import io.fom.guice.GuiceFactories;

// 1. Зарегистрируйте инжектор один раз при старте, до установки графа:
GuiceFactories.setInjector(injector);

// 2. Используйте сериализуемые суплаеры, разрешающие из него:
new GraphBuilder()
    .add("Orders",
         GuiceFactories.bound(OrdersInit.class),   // фабрика init
         GuiceFactories.bound(OrdersInit.class))   // фабрика load
    .build();
```

| Метод | Назначение |
|---|---|
| `setInjector(Injector)` | зарегистрировать инжектор (один раз при старте) |
| `requireInjector()` | зарегистрированный инжектор или `IllegalStateException`, если не задан |
| `bound(Class<T>)` | `SerializableSupplier<T>`, разрешающий `T` из инжектора в момент `get()` |

`bound(...)` вызывает `injector.getInstance(type)` на каждый `get()`, поэтому
`@Singleton`-привязки дают тот же экземпляр, а unscoped — свежие. Если суплаер
выполняется до `setInjector(...)`, он бросает понятный `IllegalStateException`.

## Spring

Симметричный API в `fom-spring`:

```java
import io.fom.spring.SpringFactories;

SpringFactories.setContext(applicationContext);

new GraphBuilder()
    .add("Orders",
         SpringFactories.bean(OrdersInit.class),
         SpringFactories.bean(OrdersInit.class))
    .build();
```

| Метод | Назначение |
|---|---|
| `setContext(ApplicationContext)` | зарегистрировать контекст (один раз при старте) |
| `requireContext()` | зарегистрированный контекст или `IllegalStateException`, если не задан |
| `bean(Class<T>)` | `SerializableSupplier<T>`, разрешающий бин типа `T` через `getBean(type)` |

## Порядок жизненного цикла

Поскольку суплаер разрешает лениво, контейнер **должен быть зарегистрирован до
выполнения суплаера** — то есть до `engine.newGraph(graph)` при первом старте *и*
до восстановления при каждом рестарте JVM. Делайте вызов `setInjector(...)` /
`setContext(...)` частью бутстрапа, до конструирования и запуска движка.

!!! note "Один контейнер на JVM"
    Обе регистрации хранят единственный контейнер в статической ссылке. Если нужно
    несколько — разрешайте через один корневой контейнер (родительский контекст /
    один инжектор с дочерними модулями), а не жонглируйте несколькими
    регистрациями.

> [English version](../../guides/dependency-injection.md)
