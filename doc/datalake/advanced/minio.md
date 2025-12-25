# MinIO Production Deployment Guide

## Содержание
1. [Обзор](#обзор)
2. [Архитектура](#архитектура)
3. [Требования](#требования)
4. [Подготовка инфраструктуры](#подготовка-инфраструктуры)
5. [Production развертывание](#production-развертывание)
6. [Конфигурация](#конфигурация)
7. [Высокая доступность](#высокая-доступность)
8. [Мониторинг](#мониторинг)
9. [Backup и восстановление](#backup-и-восстановление)
10. [Операции](#операции)
11. [Troubleshooting](#troubleshooting)

---

## Обзор

MinIO - это высокопроизводительное S3-совместимое объектное хранилище для хранения Parquet файлов Data Lake.

### Роль в архитектуре
- Хранение данных в формате Parquet
- S3 API для записи (Kafka Connect) и чтения (Trino, Jupyter)
- Распределенное хранилище с репликацией

### Зависимости
- **Входящие**: Нет (первый компонент для развертывания)
- **Исходящие от MinIO**: Другие MinIO узлы (в distributed режиме)

---

## Архитектура

### Single-Node (Development/Testing)
```
┌──────────────────┐
│   MinIO Server   │
│   Port: 9000     │ ← S3 API
│   Port: 9001     │ ← Web Console
└────────┬─────────┘
         │
    ┌────▼────┐
    │ Volume  │
    │  /data  │
    └─────────┘
```

### Distributed Mode (Production)
```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  MinIO-1    │  │  MinIO-2    │  │  MinIO-3    │  │  MinIO-4    │
│  /data1     │  │  /data1     │  │  /data1     │  │  /data1     │
│  /data2     │  │  /data2     │  │  /data2     │  │  /data2     │
└─────┬───────┘  └─────┬───────┘  └─────┬───────┘  └─────┬───────┘
      │                │                │                │
      └────────────────┴────────────────┴────────────────┘
                    Distributed Set
            (Erasure Coding: N/2 data parity)
```

**Erasure Coding**:
- Минимум 4 узла для distributed режима
- Защита от отказа до N/2 дисков/узлов
- Автоматическое восстановление данных (self-healing)

---

## Требования

### Аппаратные требования (на узел)

#### Minimum (Testing)
- **CPU**: 4 cores
- **RAM**: 8GB
- **Storage**: 500GB SSD
- **Network**: 1 Gbit/s

#### Recommended (Production)
- **CPU**: 8-16 cores
- **RAM**: 32GB
- **Storage**: 2-10TB NVMe SSD (RAID не требуется)
- **Network**: 10+ Gbit/s

### Требования к хранилищу
- **File system**: XFS (recommended) или ext4
- **RAID**: НЕ использовать RAID (MinIO сам обеспечивает redundancy)
- **Multiple drives**: Для best performance использовать JBOD (Just a Bunch Of Disks)

### Сетевые требования
- **Latency**: < 1ms между узлами
- **Bandwidth**: 10 Gbit/s minimum для production
- **DNS**: Корректное разрешение всех узлов

---

## Подготовка инфраструктуры

### 1. Подготовка серверов

На каждом узле:

```bash
# Обновление системы
apt-get update && apt-get upgrade -y

# Установка необходимых пакетов
apt-get install -y \
    curl \
    ca-certificates \
    gnupg \
    lsb-release \
    xfsprogs

# Установка Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io
```

### 2. Подготовка дисков

```bash
# Для каждого диска создать XFS файловую систему
mkfs.xfs -f /dev/sdb
mkfs.xfs -f /dev/sdc

# Создать mount points
mkdir -p /mnt/data1
mkdir -p /mnt/data2

# Получить UUID дисков
blkid /dev/sdb
blkid /dev/sdc

# Добавить в /etc/fstab для автоматического монтирования
echo "UUID=<uuid-sdb> /mnt/data1 xfs defaults,noatime 0 2" >> /etc/fstab
echo "UUID=<uuid-sdc> /mnt/data2 xfs defaults,noatime 0 2" >> /etc/fstab

# Смонтировать
mount -a

# Проверить
df -h
```

### 3. Настройка сети

```bash
# Настроить hostnames в /etc/hosts на всех узлах
cat <<EOF >> /etc/hosts
10.0.1.11 minio-1
10.0.1.12 minio-2
10.0.1.13 minio-3
10.0.1.14 minio-4
EOF

# Проверить доступность
ping -c 3 minio-1
ping -c 3 minio-2
ping -c 3 minio-3
ping -c 3 minio-4
```

### 4. Firewall правила

```bash
# Открыть порты MinIO
ufw allow 9000/tcp comment 'MinIO S3 API'
ufw allow 9001/tcp comment 'MinIO Console'

# Для distributed режима - открыть порты между узлами
ufw allow from 10.0.1.0/24 to any port 9000 proto tcp
ufw allow from 10.0.1.0/24 to any port 9001 proto tcp
```

---

## Production развертывание

### Distributed Mode (4+ узлов)

#### 1. Создать secrets

```bash
# Сгенерировать сильные credentials
export MINIO_ROOT_USER=$(openssl rand -hex 16)
export MINIO_ROOT_PASSWORD=$(openssl rand -hex 32)

# Сохранить в secrets manager или .env файл
cat > /opt/minio/.env <<EOF
MINIO_ROOT_USER=${MINIO_ROOT_USER}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}
EOF

chmod 600 /opt/minio/.env
```

#### 2. Docker запуск на КАЖДОМ узле

Создать `/opt/minio/docker-run.sh` на каждом узле:

```bash
#!/bin/bash

# Загрузить переменные окружения
source /opt/minio/.env

# Запустить MinIO в distributed режиме
docker run -d \
  --name minio \
  --restart unless-stopped \
  --network host \
  -e "MINIO_ROOT_USER=${MINIO_ROOT_USER}" \
  -e "MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}" \
  -e "MINIO_DOMAIN=minio.company.com" \
  -e "MINIO_SERVER_URL=https://minio.company.com" \
  -e "MINIO_BROWSER_REDIRECT_URL=https://console.minio.company.com" \
  -e "MINIO_REGION_NAME=us-east-1" \
  -e "MINIO_PROMETHEUS_AUTH_TYPE=public" \
  -v /mnt/data1:/data1 \
  -v /mnt/data2:/data2 \
  minio/minio:RELEASE.2024-12-13T22-19-12Z \
  server \
  http://minio-{1...4}/data{1...2} \
  --console-address ":9001"
```

**Параметры команды server**:
- `http://minio-{1...4}/data{1...2}` - Distributed set: 4 сервера × 2 диска = 8 drives
  - MinIO автоматически распределит данные с erasure coding
  - Можно потерять до 4 дисков без потери данных (N/2 parity)

#### 3. Запустить на всех узлах

```bash
# На каждом узле (minio-1, minio-2, minio-3, minio-4)
chmod +x /opt/minio/docker-run.sh
/opt/minio/docker-run.sh

# Проверить логи
docker logs -f minio
```

#### 4. Проверить cluster

```bash
# Установить MinIO Client
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
mv mc /usr/local/bin/

# Настроить alias
mc alias set prod https://minio.company.com ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# Проверить статус
mc admin info prod

# Вывод должен показать:
# - 4 сервера онлайн
# - 8 drives
# - Erasure coding: EC:4
```

### Systemd Service (Рекомендуется)

Вместо docker run создать `/etc/systemd/system/minio.service`:

```ini
[Unit]
Description=MinIO Object Storage
Documentation=https://min.io/docs/minio/linux/index.html
After=docker.service
Requires=docker.service

[Service]
Type=simple
User=root
EnvironmentFile=/opt/minio/.env
ExecStartPre=-/usr/bin/docker stop minio
ExecStartPre=-/usr/bin/docker rm minio
ExecStart=/usr/bin/docker run --rm \
  --name minio \
  --network host \
  -e "MINIO_ROOT_USER=${MINIO_ROOT_USER}" \
  -e "MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}" \
  -e "MINIO_DOMAIN=minio.company.com" \
  -e "MINIO_SERVER_URL=https://minio.company.com" \
  -e "MINIO_BROWSER_REDIRECT_URL=https://console.minio.company.com" \
  -e "MINIO_REGION_NAME=us-east-1" \
  -e "MINIO_PROMETHEUS_AUTH_TYPE=public" \
  -v /mnt/data1:/data1 \
  -v /mnt/data2:/data2 \
  minio/minio:RELEASE.2024-12-13T22-19-12Z \
  server \
  http://minio-{1...4}/data{1...2} \
  --console-address ":9001"
ExecStop=/usr/bin/docker stop minio
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Запуск:
```bash
systemctl daemon-reload
systemctl enable minio
systemctl start minio
systemctl status minio
```

---

## Конфигурация

### Переменные окружения

| Переменная | Описание | Пример | Обязательно |
|------------|----------|--------|-------------|
| `MINIO_ROOT_USER` | Access Key (admin) | `admin-2024-prod` | Да |
| `MINIO_ROOT_PASSWORD` | Secret Key (admin) | `<32+ chars>` | Да |
| `MINIO_DOMAIN` | Domain для bucket virtual hosting | `minio.company.com` | Production: Да |
| `MINIO_SERVER_URL` | Public URL для S3 API | `https://minio.company.com` | Production: Да |
| `MINIO_BROWSER_REDIRECT_URL` | Console URL | `https://console.minio.company.com` | Опционально |
| `MINIO_REGION_NAME` | AWS region для совместимости | `us-east-1` | Рекомендуется |
| `MINIO_PROMETHEUS_AUTH_TYPE` | Тип авторизации для метрик | `public` | Для мониторинга |

### SSL/TLS Configuration

```bash
# Создать директорию для сертификатов
mkdir -p /opt/minio/certs

# Скопировать сертификаты
cp your-domain.crt /opt/minio/certs/public.crt
cp your-domain.key /opt/minio/certs/private.key

# Для CA certificate (если нужен)
cp ca.crt /opt/minio/certs/CAs/

# Добавить volume в docker run
-v /opt/minio/certs:/root/.minio/certs

# Изменить схему в MINIO_SERVER_URL
-e "MINIO_SERVER_URL=https://minio.company.com"
```

---

## Высокая доступность

### Load Balancer конфигурация

#### Nginx Load Balancer

```nginx
upstream minio_s3 {
    least_conn;
    server minio-1:9000;
    server minio-2:9000;
    server minio-3:9000;
    server minio-4:9000;
}

upstream minio_console {
    least_conn;
    server minio-1:9001;
    server minio-2:9001;
    server minio-3:9001;
    server minio-4:9001;
}

# S3 API
server {
    listen 443 ssl http2;
    server_name minio.company.com;

    ssl_certificate /etc/nginx/ssl/minio.crt;
    ssl_certificate_key /etc/nginx/ssl/minio.key;

    # Increase upload size for large files
    client_max_body_size 10G;
    client_body_timeout 300s;

    # Disable buffering for performance
    proxy_buffering off;
    proxy_request_buffering off;

    location / {
        proxy_pass http://minio_s3;
        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Timeouts
        proxy_connect_timeout 300;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        chunked_transfer_encoding off;
    }
}

# Console
server {
    listen 443 ssl http2;
    server_name console.minio.company.com;

    ssl_certificate /etc/nginx/ssl/console.crt;
    ssl_certificate_key /etc/nginx/ssl/console.key;

    location / {
        proxy_pass http://minio_console;
        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support для Console
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

#### HAProxy

```haproxy
frontend minio_s3_frontend
    bind *:443 ssl crt /etc/haproxy/ssl/minio.pem
    mode http
    default_backend minio_s3_backend

frontend minio_console_frontend
    bind *:9001 ssl crt /etc/haproxy/ssl/console.pem
    mode http
    default_backend minio_console_backend

backend minio_s3_backend
    mode http
    balance leastconn
    option httpchk GET /minio/health/live
    http-check expect status 200
    server minio-1 minio-1:9000 check inter 10s
    server minio-2 minio-2:9000 check inter 10s
    server minio-3 minio-3:9000 check inter 10s
    server minio-4 minio-4:9000 check inter 10s

backend minio_console_backend
    mode http
    balance leastconn
    server minio-1 minio-1:9001 check inter 10s
    server minio-2 minio-2:9001 check inter 10s
    server minio-3 minio-3:9001 check inter 10s
    server minio-4 minio-4:9001 check inter 10s
```

### Health Checks

```bash
# HTTP health check
curl http://minio-1:9000/minio/health/live
# Response: 200 OK

# Cluster health check
curl http://minio-1:9000/minio/health/cluster
# Response: JSON with cluster status
```

---

## Мониторинг

### Prometheus Metrics

MinIO экспортирует метрики на endpoint `/minio/v2/metrics/cluster`

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'minio-cluster'
    metrics_path: /minio/v2/metrics/cluster
    scheme: http
    static_configs:
      - targets:
        - minio-1:9000
        - minio-2:9000
        - minio-3:9000
        - minio-4:9000
```

### Grafana Dashboard

Использовать официальный MinIO dashboard:
- Dashboard ID: 13502
- URL: https://grafana.com/grafana/dashboards/13502

### Ключевые метрики

| Метрика | Описание | Threshold |
|---------|----------|-----------|
| `minio_cluster_nodes_online` | Количество онлайн узлов | = Total nodes |
| `minio_cluster_drive_total` | Всего дисков | = Expected drives |
| `minio_cluster_drive_offline` | Offline дисков | = 0 |
| `minio_cluster_capacity_usable_total_bytes` | Доступное пространство | > 20% free |
| `minio_s3_requests_total` | Запросы/сек | Monitor trends |
| `minio_s3_requests_errors_total` | Ошибки | < 1% of requests |
| `minio_s3_time_ttfb_seconds_bucket` | Time To First Byte | p95 < 100ms |

### Alerting Rules (Prometheus)

```yaml
groups:
  - name: minio
    rules:
      - alert: MinIONodeDown
        expr: minio_cluster_nodes_online < minio_cluster_nodes_total
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "MinIO node down"
          description: "{{ $value }} nodes are offline"

      - alert: MinIODriveOffline
        expr: minio_cluster_drive_offline > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "MinIO drive offline"
          description: "{{ $value }} drives are offline"

      - alert: MinIOStorageSpaceLow
        expr: (minio_cluster_capacity_usable_free_bytes / minio_cluster_capacity_usable_total_bytes) < 0.2
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "MinIO storage space low"
          description: "Less than 20% free space available"

      - alert: MinIOHighErrorRate
        expr: rate(minio_s3_requests_errors_total[5m]) / rate(minio_s3_requests_total[5m]) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate"
          description: "Error rate is {{ $value | humanizePercentage }}"
```

---

## Backup и восстановление

### Site Replication (Multi-Site)

Для георепликации между датацентрами:

```bash
# Site A (Primary)
mc admin replicate add sitea siteA \
  https://minio-a.company.com \
  ${MINIO_ROOT_USER_A} ${MINIO_ROOT_PASSWORD_A}

# Site B (DR)
mc admin replicate add sitea siteB \
  https://minio-b.company.com \
  ${MINIO_ROOT_USER_B} ${MINIO_ROOT_PASSWORD_B}

# Проверить статус
mc admin replicate info sitea
```

### Bucket Replication (Single-Site)

Для репликации в другой S3 (AWS S3, GCS):

```bash
# Настроить remote target
mc admin bucket remote add sitea/datalake \
  https://s3.amazonaws.com/backup-datalake \
  --service s3 \
  --region us-east-1 \
  --access-key ${AWS_ACCESS_KEY} \
  --secret-key ${AWS_SECRET_KEY}

# Включить репликацию
mc replicate add sitea/datalake \
  --remote-bucket backup-datalake \
  --priority 1
```

### Mirror (Continuous Sync)

```bash
# Continuous mirror в другой MinIO/S3
mc mirror --watch --overwrite \
  sitea/datalake \
  sitebackup/datalake-backup
```

### Snapshot Backup

```bash
# Включить versioning
mc version enable sitea/datalake

# Создать lifecycle policy для старых версий
mc ilm add sitea/datalake \
  --noncurrent-expire-days 30

# Скрипт для backup в tape/cold storage
#!/bin/bash
DATE=$(date +%Y%m%d)
mc mirror sitea/datalake /mnt/tape-backup/datalake-${DATE}
```

### Disaster Recovery

#### Восстановление узла

```bash
# 1. Остановить MinIO на проблемном узле
systemctl stop minio

# 2. Проверить/восстановить диски
xfs_repair /dev/sdb
mount /mnt/data1

# 3. Запустить MinIO
systemctl start minio

# 4. MinIO автоматически запустит healing process
# Проверить статус
mc admin heal sitea --recursive
```

#### Восстановление данных из backup

```bash
# Из другого MinIO site
mc mirror sitebackup/datalake-backup sitea/datalake

# Из S3 backup
mc mirror s3backup/backup-datalake sitea/datalake
```

---

## Операции

### Создание bucket

```bash
# Через mc client
mc mb sitea/datalake

# Установить lifecycle policy
mc ilm add sitea/datalake --expiry-days 365 --prefix "archive/"

# Установить versioning
mc version enable sitea/datalake

# Установить quota
mc quota set sitea/datalake --size 10TB
```

### Управление пользователями

```bash
# Создать service account для Kafka Connect
mc admin user add sitea kafka-connect ${PASSWORD}

# Создать policy с минимальными правами
cat > kafka-connect-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::datalake/*",
        "arn:aws:s3:::datalake"
      ]
    }
  ]
}
EOF

mc admin policy create sitea kafka-connect-policy kafka-connect-policy.json
mc admin policy attach sitea kafka-connect-policy --user kafka-connect

# Создать аналогично для Trino, Jupyter
```

### Scaling

#### Добавление узлов (Horizontal Scaling)

Невозможно добавить узлы в существующий distributed set. Нужно:

1. Создать новый server pool:
```bash
# Остановить все узлы
# Изменить команду запуска:
server \
  http://minio-{1...4}/data{1...2} \
  http://minio-{5...8}/data{1...2}  # Новый pool
```

2. Или использовать erasure set expansion (MinIO RELEASE.2023+)

#### Добавление дисков (Vertical Scaling)

```bash
# Подготовить новый диск
mkfs.xfs -f /dev/sdd
mkdir -p /mnt/data3
mount /dev/sdd /mnt/data3

# Добавить в fstab
echo "UUID=<uuid> /mnt/data3 xfs defaults,noatime 0 2" >> /etc/fstab

# Обновить команду запуска (на всех узлах):
server http://minio-{1...4}/data{1...3}  # Было {1...2}, стало {1...3}

# Перезапустить MinIO
systemctl restart minio
```

### Maintenance

#### Healing (Проверка и восстановление данных)

```bash
# Запустить healing для bucket
mc admin heal sitea --recursive --bucket datalake

# Проверить статус healing
mc admin heal sitea --bucket datalake --verbose

# Healing stats
mc admin heal sitea --bucket datalake --json
```

#### Деcommissioning узла

```bash
# 1. Убедиться что есть достаточно узлов (минимум N/2 + 1)
# 2. Graceful shutdown
systemctl stop minio

# 3. MinIO автоматически пересоздаст данные с parity на других узлах
# 4. Удалить узел из load balancer
# 5. Не удалять данные с дисков до полного завершения healing
```

---

## Troubleshooting

### MinIO не стартует

```bash
# Проверить логи
docker logs minio
# или
journalctl -u minio -f

# Частые проблемы:
# 1. Недостаточно дисков (минимум 4 для distributed)
# 2. Недоступность других узлов
# 3. Неправильные permissions на директориях
chown -R minio:minio /mnt/data*

# 4. Clock skew между узлами
timedatectl status
# Настроить NTP на всех узлах
```

### Высокая latency

```bash
# Проверить диски
iostat -x 5

# Проверить сеть
iperf3 -s  # на одном узле
iperf3 -c minio-1  # на другом

# Проверить memory
free -h

# Проверить MinIO performance
mc support perf sitea --duration 60s
```

### Drive offline

```bash
# Проверить статус
mc admin info sitea

# Проверить диск
smartctl -a /dev/sdb

# Проверить mount
df -h | grep /mnt/data

# Если диск OK, перемонтировать
umount /mnt/data1
mount /mnt/data1

# Перезапустить MinIO
systemctl restart minio
```

### Восстановление после полной потери кластера

```bash
# 1. Убедиться что диски с данными сохранены
# 2. Развернуть новый кластер с той же конфигурацией
# 3. Скопировать данные обратно на /mnt/data* на всех узлах
# 4. Запустить MinIO
# 5. MinIO автоматически восстановит метаданные из .minio.sys/
```

---

## Дополнительные ресурсы

- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [MinIO GitHub](https://github.com/minio/minio)
- [MinIO Performance Tuning](https://min.io/docs/minio/linux/operations/performance-tuning.html)
- [Erasure Coding](https://min.io/docs/minio/linux/operations/concepts/erasure-coding.html)
