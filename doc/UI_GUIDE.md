# UI интерфейс для ETL Engine

## Обзор

Веб-интерфейс для управления ETL job'ами, созданный на Vaadin 24.5.5 с поддержкой WebSocket Push для real-time обновлений.

## Технологический стек

- **Vaadin 24.5.5** - UI фреймворк на Java
- **Spring Boot 3.5.0** - Backend framework
- **WebSocket Push** - Real-time обновления метрик

## Архитектура

```
ru.pospelov.etl.ui/
├── MainLayout.java                    # Главный layout с навигацией
├── views/
│   ├── JobListView.java              # Список всех джобов (Grid)
│   ├── JobFormView.java              # Форма создания/редактирования
│   └── JobDetailsView.java           # Детали джоба с метриками
└── services/
    └── MetricsBroadcaster.java       # WebSocket broadcaster для метрик
```

## REST API для UI

### MetricsController

Новый контроллер для получения метрик:

```
GET  /api/metrics              - Получить метрики по всем job'ам
GET  /api/metrics/{jobId}      - Получить метрики по конкретному job'у
GET  /api/metrics/{jobId}/status - Получить только статус job'а
```

### SchemaController

Новый контроллер для работы со схемами из Schema Registry:

```
GET  /api/schemas              - Получить список всех subject'ов из Schema Registry
GET  /api/schemas/health       - Проверить доступность Schema Registry
```

## UI компоненты

### 1. JobListView (/)

Главная страница со списком всех job'ов.

**Функции:**
- Отображение всех job'ов в Grid
- Колонки: Job ID, Source Query, Target, Status
- Кнопки действий для каждого job'а:
  - **View** - просмотр деталей и метрик
  - **Edit** - редактирование job'а
  - **Run** - запуск job'а
  - **Delete** - удаление job'а
- Кнопка **Create New Job** для создания нового job'а

**URL:** `http://localhost:8080/`

### 2. JobFormView (/jobs/edit)

Форма для создания нового job'а или редактирования существующего.

**Поля формы:**
- **Job ID** - уникальный идентификатор (read-only при редактировании)
- **Source Query** - SQL запрос для извлечения данных
- **Target Table** - целевая таблица для загрузки
- **Extractor Type** - тип экстрактора (sql, kafka)
- **Transformer Type** - тип трансформера (noop, avro)
- **Loader Type** - тип лоадера (kafka, sql, fast-sql)
- **Threads** - количество потоков (по умолчанию: 4)
- **Stream Batch Size** - размер батча (по умолчанию: 50000)

**Kafka параметры** (опционально):
- **Topic** - Kafka topic для отправки данных
- **Format** - формат сообщений (avro, json)
- **Avro Schema (Subject)** - выпадающий список со схемами из Schema Registry. Выберите subject, и система автоматически получит актуальную версию схемы
- **Key Column** - колонка для ключа сообщения
- **Partition Column** - колонка для партиционирования
- **Partitions** - количество партиций

**Важно:** Если нужной схемы нет в списке, сначала зарегистрируйте её в Schema Registry.

**URL:**
- Создание: `http://localhost:8080/jobs/edit`
- Редактирование: `http://localhost:8080/jobs/edit/{jobId}`

### 3. JobDetailsView (/jobs/details/{jobId})

Детальная информация о job'е с метриками и статусом.

**Отображаемая информация:**

**Job Information:**
- Source Query
- Target
- Extractor, Transformer, Loader типы

**Метрики (карточки):**
- **Extracted** - количество извлеченных записей
- **Processed** - количество обработанных записей
- **Transformed** - количество трансформированных записей
- **Loaded** - количество загруженных записей
- **Errors** - количество ошибок

**Performance:**
- **Throughput** - производительность (записей/сек)
- **Total Duration** - общая длительность
- **Extract/Transform/Load Duration** - длительность каждого этапа

**Статус:**
- Отображается в виде badge:
  - PENDING, RUNNING, EXTRACTING, TRANSFORMING, LOADING
  - COMPLETED (зеленый)
  - FAILED (красный)

**Прогресс бар:**
- Показывается во время выполнения job'а

**Real-time обновления:**
- Автоматически обновляется через WebSocket Push при изменении статуса
- Не требует ручного обновления страницы

**URL:** `http://localhost:8080/jobs/details/{jobId}`

## WebSocket Push

### MetricsBroadcaster

Spring сервис который:
1. Подписывается на события от `EtlMetricsCollector`
2. Транслирует обновления в UI через Vaadin Push
3. Управляет подписками UI компонентов

**Принцип работы:**
- При изменении статуса job'а, `EtlMetricsCollector` отправляет событие
- `MetricsBroadcaster` получает событие и уведомляет подписанные UI
- Vaadin Push автоматически отправляет обновление в браузер через WebSocket
- UI обновляется без перезагрузки страницы

## Запуск приложения

1. Соберите проект:
```bash
mvn clean install
```

2. Запустите приложение:
```bash
mvn spring-boot:run
```

3. Откройте браузер:
```
http://localhost:8080
```

При первом запуске Vaadin скачает npm зависимости и соберет frontend - это может занять несколько минут.

## Примеры использования

### Создание нового job'а

1. Перейдите на главную страницу
2. Нажмите **Create New Job**
3. Заполните форму:
   - Job ID: `my-first-job`
   - Source Query: `SELECT * FROM orders`
   - Target: `dbo.orders_target`
   - Extractor Type: `sql`
   - Transformer Type: `noop`
   - Loader Type: `fast-sql`
   - Threads: `8`
4. Нажмите **Save**

### Запуск job'а

1. В списке job'ов найдите нужный job
2. Нажмите **Run**
3. Нажмите **View** для просмотра прогресса
4. Страница будет автоматически обновляться в real-time

### Мониторинг выполнения

1. Откройте страницу деталей job'а
2. Наблюдайте за изменением статуса:
   - PENDING → RUNNING → EXTRACTING → TRANSFORMING → LOADING → COMPLETED
3. Смотрите обновление метрик в реальном времени
4. Отслеживайте производительность (throughput)

## Особенности реализации

### Не изменяли существующий код

Все изменения сделаны через создание новых компонентов:
- Новый пакет `ru.pospelov.etl.ui`
- Новый контроллер `MetricsController`
- Новые Vaadin view

### Используем существующую инфраструктуру

- REST API из `JobController`
- Метрики из `EtlMetricsCollector`
- Сервисы из `JobService`

### WebSocket через Vaadin Push

- Не требует отдельной настройки WebSocket
- Vaadin автоматически управляет соединениями
- Поддержка fallback на long-polling

## Расширение функционала

### Добавление новых view

1. Создайте новый класс в `ru.pospelov.etl.ui.views`
2. Добавьте аннотации `@Route` и `@PageTitle`
3. Добавьте ссылку в `MainLayout`

### Добавление новых метрик

1. Метрики уже собираются через `EtlMetricsCollector`
2. Просто отобразите их в `JobDetailsView`
3. Обновления будут автоматически через WebSocket

### Кастомизация дизайна

Vaadin использует Lumo theme. Можно кастомизировать через:
- CSS переменные
- Lumo utility classes
- Custom CSS файлы в `frontend/styles/`

## Troubleshooting

### Не загружается UI

1. Проверьте что приложение запущено: `http://localhost:8080`
2. Проверьте логи - Vaadin может компилировать frontend
3. При первом запуске подождите 2-3 минуты

### Метрики не обновляются

1. Проверьте что job запущен
2. Откройте консоль браузера - проверьте WebSocket соединение
3. Убедитесь что `MetricsBroadcaster` инициализирован в логах

### Ошибки компиляции

1. Убедитесь что установлен Node.js (для Vaadin frontend)
2. Запустите `mvn clean install -Pproduction`
3. Проверьте версию Java (требуется Java 21)

## Полезные ссылки

- [Vaadin Documentation](https://vaadin.com/docs/latest)
- [Vaadin Push Guide](https://vaadin.com/docs/latest/advanced/server-push)
- [Spring Boot + Vaadin](https://vaadin.com/docs/latest/integrations/spring)
