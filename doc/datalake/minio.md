# MinIO - MVP Deployment

## Назначение

S3-совместимое объектное хранилище для Parquet файлов Data Lake.

**Конфигурация для больших данных**: 2+ TB, оптимизирован для высокой пропускной способности.

---

## Требования к машине

| Параметр | Значение |
|----------|----------|
| CPU | 8 cores |
| RAM | 16 GB |
| Storage | 5 TB SSD (смонтирован в `/mnt/data`) |
| Network | 10 Gbit/s |
| Hostname | minio.company.com |

---

## GitLab структура

```
minio/
├── Dockerfile
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

---

## Dockerfile

`minio/Dockerfile`:

```dockerfile
FROM minio/minio:RELEASE.2024-12-13T22-19-12Z

# Копируем SSL сертификаты
COPY --from=certs /etc/ssl/certs/minio.crt /root/.minio/certs/public.crt
COPY --from=certs /etc/ssl/certs/minio.key /root/.minio/certs/private.key

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:9000/minio/health/live || exit 1

# По умолчанию запускается как single-server
# Параметры передаются через CMD
ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["server", "/data", "--console-address", ":9001"]
```

**Примечание**: Если сертификаты уже на машине в `/etc/ssl/certs/`, можно использовать volume mount вместо COPY.

---

## Environment Variables

`.env.example`:

```bash
# === CREDENTIALS ===
MINIO_ROOT_USER=admin_datalake_2024
MINIO_ROOT_PASSWORD=<SECURE_PASSWORD_32_CHARS>

# === NETWORK ===
MINIO_DOMAIN=minio.company.com
MINIO_SERVER_URL=https://minio.company.com:9000
MINIO_BROWSER_REDIRECT_URL=https://minio.company.com:9001

# === REGION ===
MINIO_REGION_NAME=us-east-1

# === PERFORMANCE (для 2+ TB) ===
# Эти параметры устанавливаются автоматически MinIO на основе доступных ресурсов
# Можно оставить пустыми или не указывать
```

Скопировать и заполнить:
```bash
cp .env.example .env
# Отредактировать .env - установить сильный пароль
```

---

## Build & Deploy

### Вариант 1: Вручную

```bash
# На машине minio.company.com

# 1. Клонировать репозиторий
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure/minio

# 2. Build образа
docker build -t minio-datalake:latest .

# 3. Создать .env из .env.example
cp .env.example .env
nano .env  # Установить пароли

# 4. Запуск контейнера
docker run -d \
  --name minio \
  --restart unless-stopped \
  -p 9000:9000 \
  -p 9001:9001 \
  --env-file .env \
  -v /mnt/data:/data \
  -v /etc/ssl/certs/minio.crt:/root/.minio/certs/public.crt:ro \
  -v /etc/ssl/certs/minio.key:/root/.minio/certs/private.key:ro \
  minio-datalake:latest

# 5. Проверить логи
docker logs -f minio

# 6. Health check
curl -f https://minio.company.com:9000/minio/health/live
```

### Вариант 2: GitLab CI/CD

`.gitlab-ci.yml`:

```yaml
stages:
  - build
  - deploy

variables:
  IMAGE_TAG: ${CI_COMMIT_REF_NAME}-${CI_COMMIT_SHORT_SHA}
  REGISTRY: registry.company.com
  TARGET_HOST: minio.company.com

build:
  stage: build
  script:
    - docker build -t ${REGISTRY}/minio-datalake:${IMAGE_TAG} .
    - docker tag ${REGISTRY}/minio-datalake:${IMAGE_TAG} ${REGISTRY}/minio-datalake:latest
    - docker push ${REGISTRY}/minio-datalake:${IMAGE_TAG}
    - docker push ${REGISTRY}/minio-datalake:latest
  only:
    - main

deploy:
  stage: deploy
  script:
    - ssh deploy@${TARGET_HOST} "docker pull ${REGISTRY}/minio-datalake:latest"
    - ssh deploy@${TARGET_HOST} "docker stop minio || true && docker rm minio || true"
    - ssh deploy@${TARGET_HOST} "docker run -d --name minio --restart unless-stopped -p 9000:9000 -p 9001:9001 --env-file /opt/minio/.env -v /mnt/data:/data -v /etc/ssl/certs/minio.crt:/root/.minio/certs/public.crt:ro -v /etc/ssl/certs/minio.key:/root/.minio/certs/private.key:ro ${REGISTRY}/minio-datalake:latest"
  only:
    - main
  when: manual
```

---

## Первоначальная настройка

### 1. Создать bucket для Data Lake

```bash
# Установить MinIO Client
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/

# Настроить alias
mc alias set datalake https://minio.company.com:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# Создать bucket
mc mb datalake/datalake

# Установить lifecycle policy (опционально, для автоматического удаления старых данных)
mc ilm add datalake/datalake --expiry-days 365 --prefix "archive/"
```

### 2. Создать service account для Kafka Connect

```bash
# Создать пользователя
mc admin user add datalake kafka-connect <SECURE_PASSWORD>

# Создать policy с правами на запись
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

mc admin policy create datalake kafka-connect-policy kafka-connect-policy.json
mc admin policy attach datalake kafka-connect-policy --user kafka-connect

# Сохранить credentials для Kafka Connect
echo "AWS_ACCESS_KEY_ID=kafka-connect" >> /tmp/kafka-connect-minio-creds
echo "AWS_SECRET_ACCESS_KEY=<PASSWORD>" >> /tmp/kafka-connect-minio-creds
```

### 3. Создать service account для Trino

```bash
mc admin user add datalake trino <SECURE_PASSWORD>

cat > trino-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
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

mc admin policy create datalake trino-policy trino-policy.json
mc admin policy attach datalake trino-policy --user trino

echo "AWS_ACCESS_KEY_ID=trino" >> /tmp/trino-minio-creds
echo "AWS_SECRET_ACCESS_KEY=<PASSWORD>" >> /tmp/trino-minio-creds
```

---

## Health Check

```bash
# HTTP API health
curl -f https://minio.company.com:9000/minio/health/live

# Возвращает 200 OK если сервис работает

# Docker healthcheck status
docker inspect minio | grep -A 5 Health
```

---

## Web Console

URL: https://minio.company.com:9001

Логин: значение `MINIO_ROOT_USER` из .env
Пароль: значение `MINIO_ROOT_PASSWORD` из .env

---

## Конфигурация для 2+ TB данных

MinIO автоматически оптимизируется под доступные ресурсы. Для 2+ TB:

### Storage
- Используйте XFS файловую систему (рекомендуется)
- Mount options: `noatime` для лучшей производительности

```bash
# Проверить file system
df -hT /mnt/data

# Если нужно переформатировать (ВНИМАНИЕ: удалит все данные)
# mkfs.xfs -f /dev/sdb
# mount -o noatime /dev/sdb /mnt/data
```

### Network
- Убедитесь что MTU = 9000 (Jumbo Frames) включен для 10Gbit сети

```bash
# Проверить MTU
ip link show | grep mtu

# Установить MTU 9000 (если поддерживается сетью)
# ip link set dev eth0 mtu 9000
```

---

## Troubleshooting

### MinIO не стартует

```bash
# Проверить логи
docker logs minio

# Проверить permissions на /mnt/data
ls -la /mnt/data
# Должен быть доступен для записи

# Проверить сертификаты
ls -la /etc/ssl/certs/minio.*
```

### Медленная запись/чтение

```bash
# Проверить disk performance
sudo fio --name=random-write --ioengine=libaio --rw=randwrite --bs=4k --size=1G --numjobs=4 --runtime=60 --time_based --end_fsync=1 --filename=/mnt/data/test.fio

# Ожидаемая производительность для SSD:
# IOPS: > 10,000
# Bandwidth: > 500 MB/s

# Проверить network
iperf3 -c <another-server>
# Ожидаемая: > 1 Gbit/s

# Проверить MinIO metrics (если порт открыт)
curl https://minio.company.com:9000/minio/v2/metrics/cluster
```

### Cannot connect

```bash
# Проверить DNS
nslookup minio.company.com

# Проверить порты
nc -zv minio.company.com 9000
nc -zv minio.company.com 9001

# Проверить SSL сертификаты
openssl s_client -connect minio.company.com:9000 -showcerts
```

---

## Следующий шаг

После успешного развертывания MinIO переходите к:
👉 [PostgreSQL Metastore](postgres-metastore.md)
