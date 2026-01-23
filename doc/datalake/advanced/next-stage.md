# Следующая стадия (после MVP)

## Когда

MVP закрывает быстрый запуск и работу с 2+ TB. Дальше поэтапно:
- **Шаг 2**: авторизация пользователей и доступы
- **Шаг 3-4**: High Availability, балансировщики, координаторы, мониторинг, backup/DR

---

## Что добавляем

### High Availability и масштабирование
- MinIO distributed set (4+ узла) + LB
- Kafka Connect cluster (2-3 узла)
- Hive Metastore HA (2-3 узла + LB)
- Trino coordinator + workers
- Spark cluster (5-10 workers для 2+ TB данных)
- PostgreSQL standby/replication + failover

### Мониторинг
- Prometheus + Grafana + Alerting
- JMX exporters (Kafka Connect, Hive Metastore, Trino)
- Spark Metrics (Prometheus sink)
- Spark History Server
- Метрики MinIO/DB/Connectors

### Backup и Disaster Recovery
- Регулярные backup jobs для PostgreSQL
- Репликация/backup MinIO (mirror/replication)
- DR план и RTO/RPO

### Авторизация пользователей
- Jupyter (OAuth2/LDAP)
- Trino (LDAP/OPA/File-based ACL)
- MinIO политики и ограничение доступа

### Security hardening
- TLS везде
- ограничение сетевых доступов
- secrets management

---

## Где смотреть детали

- [MinIO Production Guide](minio.md)
- [PostgreSQL Metastore Production Guide](postgres-metastore.md)
- [Hive Metastore Production Guide](hive-metastore.md)
- [Kafka Connect Production Guide](kafka-connect.md)
- [Spark Production Guide](../spark.md)
- [Trino Production Guide](trino.md)
- [Jupyter Production Guide](jupyter.md)

---

## Важно

Эти изменения не входят в MVP. Сначала фиксируем стабильный MVP, затем включаем блоки по очереди.
