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
FROM registry.company.com/minio/minio:RELEASE.2024-12-13T22-19-12Z

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
variables:
  GIT_STRATEGY: clone

stages:
  - deploy

Deploy MinIO to TEST:
  stage: deploy
  tags: [your_runner_tag]  # Укажите тег вашего GitLab Runner
  needs: []
  when: manual
  allow_failure: false
  before_script:
    - BALANSER_NAME="minio.company.com"  # Балансировщик (если есть)
    - SRV_APP="minio.company.com"        # Целевой сервер
  script:
    - |
      # Создаем переменную с названием образа
      ImageName=minio/minio:RELEASE.2024-12-13T22-19-12Z

      # Создаем переменную с названием контейнера
      ContainerName=minio

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo 'Останавливаем и удаляем старый контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} && echo 'Старый контейнер остановлен и удален.' || echo 'Старого контейнера нет, останавливать нечего.'

      echo 'Создаем директории для данных...'
      mkdir -p /mnt/data

      echo 'Создаем новый контейнер...'
      docker run \
        -d \
        --name ${ContainerName} \
        --restart=always \
        -e MINIO_ROOT_USER=${MINIO_ROOT_USER} \
        -e MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD} \
        -e MINIO_REGION_NAME=us-east-1 \
        -e MINIO_SERVER_URL="https://${BALANSER_NAME}:9000" \
        -e MINIO_BROWSER_URL="https://${BALANSER_NAME}:9001" \
        -p 9000:9000 \
        -p 9001:9001 \
        -v /mnt/data:/data \
        -v /etc/ssl/certs/minio.crt:/root/.minio/certs/public.crt:ro \
        -v /etc/ssl/certs/minio.key:/root/.minio/certs/private.key:ro \
        -h ${SRV_APP} \
        ${ImageName} \
        server /data --console-address ":9001"

      echo "=========================================================================================="
      echo 'ГОТОВО!'
      echo "=========================================================================================="
      echo 'Проверяем состояние контейнера:'
      sleep 10
      docker ps -a --filter name=${ContainerName}
      echo '------------------------------------------------------------------------------------------'
      echo 'Логи контейнера:'
      docker logs ${ContainerName}
      echo '------------------------------------------------------------------------------------------'

      DEPLOY_SCRIPT

      echo "Копируем скрипт на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
      rsync -avz ./ svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}

      echo "Запускаем скрипт деплоя на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "cd ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/ && \
        chmod u+x ./build.sh && ./build.sh"

      echo "Удаляем временные файлы с ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"

Init MinIO to TEST:
  stage: deploy
  tags: [your_runner_tag]
  needs: []
  when: manual
  allow_failure: false
  before_script:
    - BALANSER_NAME="minio.company.com"
    - SRV_APP="minio.company.com"
  script:
    - |
      # Создаем переменную с названием образа
      ImageName=minio/mc:latest

      # Создаем переменную с названием контейнера
      ContainerName=minio-init

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo 'Останавливаем и удаляем старый init контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} && echo 'Старый контейнер остановлен и удален.' || echo 'Старого контейнера нет, останавливать нечего.'

      echo 'Запускаем init контейнер...'
      docker run \
        --network host \
        --entrypoint /bin/sh \
        -d \
        --name ${ContainerName} \
        -e MINIO_ROOT_USER=${MINIO_ROOT_USER} \
        -e MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD} \
        -e HIVE_METASTORE_USER=${HIVE_METASTORE_USER} \
        -e HIVE_METASTORE_PASSWORD=${HIVE_METASTORE_PASSWORD} \
        -e KAFKA_CONNECT_USER=${KAFKA_CONNECT_USER} \
        -e KAFKA_CONNECT_PASSWORD=${KAFKA_CONNECT_PASSWORD} \
        -e TRINO_USER=${TRINO_USER} \
        -e TRINO_PASSWORD=${TRINO_PASSWORD} \
        -v ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/init-minio.sh:/init-minio.sh \
        -h ${SRV_APP} \
        ${ImageName} /init-minio.sh

      echo "=========================================================================================="
      echo 'ГОТОВО!'
      echo "=========================================================================================="
      echo 'Проверяем состояние контейнера:'
      sleep 10
      docker ps -a --filter name=${ContainerName}
      echo '------------------------------------------------------------------------------------------'
      echo 'Логи контейнера:'
      docker logs ${ContainerName}
      echo '------------------------------------------------------------------------------------------'

      DEPLOY_SCRIPT

      echo "Копируем init скрипт и build скрипт на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
      rsync -avz ./ svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}

      echo "Запускаем скрипт инициализации на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "cd ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/ && \
        chmod u+x ./build.sh && ./build.sh"

      # НЕ удаляем временные файлы, чтобы init-minio.sh был доступен для контейнера
```

**Настройка переменных окружения в GitLab**:

В настройках CI/CD вашего проекта GitLab (`Settings > CI/CD > Variables`) добавьте:

| Переменная | Значение | Тип |
|-----------|----------|-----|
| `MINIO_ROOT_USER` | `admin_datalake_2024` | Variable |
| `MINIO_ROOT_PASSWORD` | `<secure_password>` | Variable (Masked) |
| `HIVE_METASTORE_USER` | `hive-metastore` | Variable |
| `HIVE_METASTORE_PASSWORD` | `<secure_password>` | Variable (Masked) |
| `KAFKA_CONNECT_USER` | `kafka-connect` | Variable |
| `KAFKA_CONNECT_PASSWORD` | `<secure_password>` | Variable (Masked) |
| `TRINO_USER` | `trino` | Variable |
| `TRINO_PASSWORD` | `<secure_password>` | Variable (Masked) |

**Примечание**:
- Замените `your_runner_tag` на тег вашего GitLab Runner
- Замените `svc_user` на пользователя для SSH подключения
- Job `Init MinIO to TEST` запускайте ПОСЛЕ успешного деплоя MinIO для создания buckets и пользователей

---

## Первоначальная настройка

### 1. Создать bucket для Data Lake

```bash
# Установить MinIO Client

# Вариант 1: Через корпоративный Nexus (если настроен raw proxy)
wget https://nexus.company.com/repository/raw-proxy/minio-client/mc
chmod +x mc
sudo mv mc /usr/local/bin/

# Вариант 2: Использовать Docker контейнер MinIO Client
# docker run --rm -it --entrypoint=/bin/sh registry.company.com/minio/mc
# или создать alias для удобства:
# alias mc='docker run --rm -it --network=host registry.company.com/minio/mc'

# Вариант 3: Если доступен dl.min.io через proxy
# wget https://dl.min.io/client/mc/release/linux-amd64/mc
# chmod +x mc
# sudo mv mc /usr/local/bin/

# Настроить alias
mc alias set datalake https://minio.company.com:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# Создать bucket
mc mb datalake/datalake

# Установить lifecycle policy (опционально, для автоматического удаления старых данных)
mc ilm add datalake/datalake --expiry-days 365 --prefix "archive/"
```

### 2. Создать service account для Hive Metastore

```bash
# Создать пользователя
mc admin user add datalake hive-metastore <SECURE_PASSWORD>

# Создать policy с правами на чтение и запись
cat > hive-metastore-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
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

mc admin policy create datalake hive-metastore-policy hive-metastore-policy.json
mc admin policy attach datalake hive-metastore-policy --user hive-metastore

# Сохранить credentials для Hive Metastore
echo "AWS_ACCESS_KEY_ID=hive-metastore" >> /tmp/hive-metastore-minio-creds
echo "AWS_SECRET_ACCESS_KEY=<PASSWORD>" >> /tmp/hive-metastore-minio-creds
```

### 3. Создать service account для Kafka Connect

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

### 4. Создать service account для Trino

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

## Автоматическая инициализация (опционально)

Для автоматического создания bucket при старте можно использовать init-контейнер. Это особенно полезно при развертывании через CI/CD.

### Создать init скрипт

`minio/init-minio.sh`:

```bash
#!/bin/sh
# Автоматическая инициализация MinIO при первом запуске

until /usr/bin/mc alias set myminio https://minio.company.com:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}; do
  echo 'Waiting for MinIO to be ready...'
  sleep 2
done

echo "MinIO is ready. Creating bucket and users..."

# Создать bucket
/usr/bin/mc mb myminio/datalake --ignore-existing
/usr/bin/mc policy set download myminio/datalake

echo "Bucket 'datalake' created successfully"

# Создать пользователей
/usr/bin/mc admin user add myminio hive-metastore ${HIVE_METASTORE_PASSWORD}
/usr/bin/mc admin user add myminio kafka-connect ${KAFKA_CONNECT_PASSWORD}
/usr/bin/mc admin user add myminio trino ${TRINO_PASSWORD}

# Создать policies
cat > /tmp/hive-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::datalake/*", "arn:aws:s3:::datalake"]
    }
  ]
}
EOF

/usr/bin/mc admin policy create myminio hive-metastore-policy /tmp/hive-policy.json
/usr/bin/mc admin policy attach myminio hive-metastore-policy --user hive-metastore

echo "MinIO initialization completed successfully"
exit 0
```

### Запустить init контейнер

```bash
docker run --rm \
  --network host \
  -e MINIO_ROOT_USER=${MINIO_ROOT_USER} \
  -e MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD} \
  -e HIVE_METASTORE_PASSWORD=<SECURE_PASSWORD> \
  -e KAFKA_CONNECT_PASSWORD=<SECURE_PASSWORD> \
  -e TRINO_PASSWORD=<SECURE_PASSWORD> \
  -v $(pwd)/init-minio.sh:/init-minio.sh \
  minio/mc:latest /bin/sh /init-minio.sh
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
