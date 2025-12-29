# Jupyter - MVP Deployment

## Назначение

Интерактивная среда для анализа данных Data Lake через SQL (Trino) и прямого чтения Parquet файлов из MinIO.

---

## Требования к машине

| Параметр | Значение |
|----------|----------|
| CPU | 4 cores |
| RAM | 16 GB |
| Storage | 200 GB SSD |
| Network | 1 Gbit/s |
| Hostname | jupyter.company.com |

---

## GitLab структура

```
jupyter/
├── Dockerfile
├── config/
│   └── pip.conf
├── requirements.txt
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

---

## Dockerfile

`jupyter/Dockerfile`:

```dockerfile
FROM registry.company.com/jupyter/scipy-notebook:latest

USER root

# Установка системных пакетов
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Настроить pip для работы с Nexus PyPI
COPY config/pip.conf /etc/pip.conf

USER jovyan

# Копировать requirements
COPY requirements.txt /tmp/

# Установка Python пакетов через Nexus
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Создать рабочую директорию
RUN mkdir -p /home/jovyan/work

WORKDIR /home/jovyan/work

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8888/api || exit 1

EXPOSE 8888

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
```

**Примечание**: В MVP отключена аутентификация (`token=''`). Авторизация пользователей — шаг 2 (см. [advanced/next-stage.md](advanced/next-stage.md)).

---

## Конфигурационные файлы

### pip.conf

`config/pip.conf`:

```ini
[global]
index-url = https://nexus.company.com/repository/pypi/simple
trusted-host = nexus.company.com
```

**Примечание**: Замените `nexus.company.com` на реальный адрес вашего Nexus сервера с PyPI proxy.

---

## requirements.txt

`requirements.txt`:

```txt
# === Data Lake Integration ===
pyarrow>=14.0.0
pandas>=2.0.0
s3fs>=2023.12.0
boto3>=1.34.0

# === Trino Client ===
trino>=0.328.0
sqlalchemy>=2.0.25
sqlalchemy-trino>=0.5.0

# === Visualization ===
matplotlib>=3.8.0
seaborn>=0.13.0
plotly>=5.18.0

# === JupyterLab ===
jupyterlab>=4.0.0
```

---

## Environment Variables

`.env.example`:

```bash
# === JupyterLab ===
JUPYTER_ENABLE_LAB=yes

# === MinIO/S3 Access ===
AWS_ACCESS_KEY_ID=jupyter
AWS_SECRET_ACCESS_KEY=<PASSWORD_FROM_MINIO>
AWS_ENDPOINT_URL=https://minio.company.com:9000
AWS_REGION=us-east-1

# === Trino Connection ===
TRINO_HOST=trino.company.com
TRINO_PORT=8080
TRINO_USER=analyst
```

---

## Build & Deploy

### Вариант 1: Вручную

```bash
# На машине jupyter.company.com

# 1. Клонировать репозиторий
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure/jupyter

# 2. Build образа
docker build -t jupyter-datalake:latest .

# 3. Создать .env
cp .env.example .env
nano .env  # Установить credentials

# 4. Запуск контейнера
docker run -d \
  --name jupyter \
  --restart unless-stopped \
  -p 8888:8888 \
  --env-file .env \
  -v /mnt/data/jupyter/notebooks:/home/jovyan/work \
  jupyter-datalake:latest

# 5. Проверить логи
docker logs -f jupyter

# Найти URL с токеном (если есть):
# http://127.0.0.1:8888/lab

# 6. Health check
curl -f http://jupyter.company.com:8888/api
```

### Вариант 2: GitLab CI/CD

`.gitlab-ci.yml`:

```yaml
variables:
  GIT_STRATEGY: clone

stages:
  - deploy

Deploy Jupyter to TEST:
  stage: deploy
  tags: [your_runner_tag]
  needs: []
  when: manual
  allow_failure: false
  before_script:
    - SRV_APP="jupyter.company.com"
  script:
    - |
      # Создаем переменную с названием образа
      ImageName=jupyter-datalake:latest

      # Создаем переменную с названием контейнера
      ContainerName=jupyter

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo 'Собираем Docker образ...'
      docker build -t ${ImageName} .

      echo 'Останавливаем и удаляем старый контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} && echo 'Старый контейнер остановлен и удален.' || echo 'Старого контейнера нет, останавливать нечего.'

      echo 'Создаем директории для notebooks...'
      mkdir -p /mnt/data/jupyter/notebooks

      echo 'Создаем новый контейнер...'
      docker run \
        -d \
        --name ${ContainerName} \
        --restart=always \
        -e JUPYTER_TOKEN=${JUPYTER_TOKEN} \
        -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
        -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
        -e MINIO_ENDPOINT=${MINIO_ENDPOINT} \
        -e TRINO_HOST=${TRINO_HOST} \
        -p 8888:8888 \
        -v /mnt/data/jupyter/notebooks:/home/jovyan/work \
        -h ${SRV_APP} \
        ${ImageName}

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
      echo 'Проверяем доступность Jupyter API:'
      sleep 5
      curl -f http://localhost:8888/api || echo 'ВНИМАНИЕ: Jupyter API еще не доступен. Дождитесь полной инициализации.'
      echo '------------------------------------------------------------------------------------------'

      DEPLOY_SCRIPT

      echo "Копируем папку на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
      rsync -avz ./ svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}

      echo "Запускаем скрипт деплоя на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "cd ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/ && \
        chmod u+x ./build.sh && ./build.sh"

      echo "Удаляем временные файлы с ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
```

**Настройка переменных окружения в GitLab**:

В настройках CI/CD вашего проекта GitLab (`Settings > CI/CD > Variables`) добавьте:

| Переменная | Значение | Тип |
|-----------|----------|-----|
| `JUPYTER_TOKEN` | `<secure_token>` | Variable (Masked) |
| `AWS_ACCESS_KEY_ID` | `jupyter` | Variable |
| `AWS_SECRET_ACCESS_KEY` | `<password_from_minio>` | Variable (Masked) |
| `MINIO_ENDPOINT` | `https://minio.company.com:9000` | Variable |
| `TRINO_HOST` | `trino.company.com` | Variable |

**Примечание**:
- Замените `your_runner_tag` на тег вашего GitLab Runner
- Замените `svc_user` на пользователя для SSH подключения
- Docker образ собирается локально на целевом сервере из скопированного проекта
- Notebooks сохраняются в `/mnt/data/jupyter/notebooks` и персистентны между перезапусками

---

## Первоначальная настройка MinIO access

Создать service account для Jupyter в MinIO (на машине minio.company.com):

```bash
mc admin user add datalake jupyter <SECURE_PASSWORD>

cat > jupyter-policy.json <<EOF
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

mc admin policy create datalake jupyter-policy jupyter-policy.json
mc admin policy attach datalake jupyter-policy --user jupyter
```

---

## Примеры Notebooks

### 1. Подключение к Trino

Создать `01_trino_connection.ipynb`:

```python
from trino.dbapi import connect
import pandas as pd

# Подключение к Trino
conn = connect(
    host='trino.company.com',
    port=8080,
    user='analyst',
    catalog='iceberg',
    schema='default'
)

# Выполнить SQL запрос
query = """
SELECT * FROM iceberg.sales.orders
WHERE dt = '2024-12-25'
LIMIT 100
"""

df = pd.read_sql(query, conn)
print(df.head())
print(f"Total rows: {len(df)}")
```

### 2. Чтение Parquet из MinIO

Создать `02_read_parquet.ipynb`:

```python
import s3fs
import pandas as pd
import pyarrow.parquet as pq

# Подключение к MinIO через S3FS
s3 = s3fs.S3FileSystem(
    key='jupyter',
    secret='<PASSWORD>',
    client_kwargs={
        'endpoint_url': 'https://minio.company.com:9000',
        'region_name': 'us-east-1'
    }
)

# Чтение одного Parquet файла
parquet_path = 's3://datalake/topics/order-events/calc_id=20241225-120000/dt=2024-12-25/hour=12/part-00001.snappy.parquet'

df = pd.read_parquet(
    parquet_path,
    filesystem=s3
)

print(df.head())
print(df.info())
```

### 3. Аналитика и визуализация

Создать `03_analytics.ipynb`:

```python
from trino.dbapi import connect
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Подключение к Trino
conn = connect(
    host='trino.company.com',
    port=8080,
    user='analyst',
    catalog='iceberg',
    schema='sales'
)

# Запрос данных за последние 30 дней
query = """
SELECT
    dt,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM iceberg.sales.orders
WHERE dt >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY dt
ORDER BY dt
"""

df = pd.read_sql(query, conn)

# Визуализация
fig, axes = plt.subplots(2, 1, figsize=(12, 8))

# График количества заказов
axes[0].plot(df['dt'], df['order_count'], marker='o')
axes[0].set_title('Orders per Day')
axes[0].set_xlabel('Date')
axes[0].set_ylabel('Order Count')
axes[0].grid(True)

# График суммы
axes[1].plot(df['dt'], df['total_amount'], marker='o', color='green')
axes[1].set_title('Total Amount per Day')
axes[1].set_xlabel('Date')
axes[1].set_ylabel('Amount ($)')
axes[1].grid(True)

plt.tight_layout()
plt.show()

# Статистика
print(df.describe())
```

---

## Health Check

```bash
# API health check
curl -f http://jupyter.company.com:8888/api

# Docker healthcheck
docker inspect jupyter | grep -A 5 Health
```

---

## Web UI

URL: http://jupyter.company.com:8888

В MVP версии аутентификация отключена для простоты.

**Важно**: Авторизация пользователей — шаг 2 (см. [advanced/next-stage.md](advanced/next-stage.md)).

---

## Troubleshooting

### Jupyter не стартует

```bash
# Проверить логи
docker logs jupyter

# Проверить порт
netstat -tuln | grep 8888
```

### Cannot connect to Trino

```python
# В notebook проверить подключение
from trino.dbapi import connect

try:
    conn = connect(
        host='trino.company.com',
        port=8080,
        user='analyst'
    )
    print("Connection successful!")
except Exception as e:
    print(f"Error: {e}")
```

### Cannot read from MinIO

```python
# Проверить S3 credentials
import boto3

s3_client = boto3.client(
    's3',
    endpoint_url='https://minio.company.com:9000',
    aws_access_key_id='jupyter',
    aws_secret_access_key='<PASSWORD>',
    region_name='us-east-1'
)

# Листинг buckets
try:
    response = s3_client.list_buckets()
    print("Buckets:", [b['Name'] for b in response['Buckets']])
except Exception as e:
    print(f"Error: {e}")
```

### Out of memory

```bash
# Увеличить memory limit для контейнера
docker update --memory 24g --memory-swap 24g jupyter

# Или при запуске:
docker run -d --name jupyter --memory 24g --memory-swap 24g ...
```

---

## Следующий шаг

После развертывания всех сервисов:
1. Проверить интеграцию между сервисами
2. Создать тестовые таблицы и данные
3. Запустить E2E тест (Kafka → MinIO → Trino → Jupyter)

👉 [Продвинутые возможности](advanced/next-stage.md) - HA, мониторинг, авторизация
