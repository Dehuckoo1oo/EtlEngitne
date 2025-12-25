# Production Deployment Guide для Jupyter в Data Lake

## Содержание

1. [Обзор и роль](#обзор-и-роль)
2. [Архитектура](#архитектура)
3. [Зависимости](#зависимости)
4. [Требования к инфраструктуре](#требования-к-инфраструктуре)
5. [Single-User Jupyter Lab](#single-user-jupyter-lab)
6. [Multi-User JupyterHub](#multi-user-jupyterhub)
7. [Python Packages для Data Lake](#python-packages-для-data-lake)
8. [Конфигурация подключений](#конфигурация-подключений)
9. [Примеры Notebook кода](#примеры-notebook-кода)
10. [Authentication и Авторизация](#authentication-и-авторизация)
11. [Мониторинг](#мониторинг)
12. [Backup и Version Control](#backup-и-version-control)
13. [Troubleshooting](#troubleshooting)

---

## Обзор и роль

### Назначение

Jupyter Lab служит **интерактивной аналитической платформой** для работы с данными в Data Lake. Это основной инструмент для:
- исследовательского анализа (EDA);
- разработки и тестирования аналитического кода;
- создания ad-hoc отчётов;
- прототипирования ML-моделей;
- визуализации данных.

### Позиция в архитектуре

```
┌──────────────┐
│   Kafka      │ Streaming data
└──────┬───────┘
       │
       ↓
┌──────────────┐
│Kafka Connect │ Ingestion to Parquet
└──────┬───────┘
       │
       ↓
┌──────────────┐
│    MinIO     │ Object Storage (S3-compatible)
└──────┬───────┘
       │
   ────┼────────────
   │          │
   ↓          ↓
 Trino     Jupyter Lab ← INTERACTIVE ANALYTICS
  SQL          Notebooks
```

Jupyter Lab работает как **граждане первого класса** в экосистеме аналитики, предоставляя аналитикам полную гибкость для исследования данных через Python, SQL и визуализацию.

---

## Архитектура

### Deployment Topology

#### Development / Single-User

```yaml
Docker Container (Jupyter Lab)
├── JupyterLab Server (port 8888)
├── Python 3.11+ environment
├── Connected to Trino (port 8084)
├── Connected to MinIO (port 9000)
└── Shared notebooks volume
```

#### Production / Multi-User (JupyterHub)

```yaml
Load Balancer (Nginx/HAProxy)
    │
    └─→ JupyterHub Hub (port 8000)
        ├── Authentication (OAuth2, LDAP, etc.)
        ├── User management
        └─→ Multiple Single-User Servers
            ├── jupyter-user1 (spawned in Docker/K8s)
            ├── jupyter-user2
            └─→ jupyter-userN
```

### Stack Components

| Компонент | Version | Роль |
|-----------|---------|------|
| JupyterLab | 4.0.0+ | IDE для интерактивного кода и ноутбуков |
| Python | 3.11+ | Runtime для анализа |
| PyArrow | 14.0.0+ | Работа с Parquet форматом |
| Pandas | 2.0.0+ | Data manipulation |
| S3FS | 2023.12.0+ | Доступ к MinIO как к файловой системе |
| Boto3 | 1.34.0+ | AWS SDK для S3 API |
| Trino Python | Latest | SQL запросы к Trino |
| Matplotlib | 3.8.0+ | Visualization |
| Seaborn | 0.13.0+ | Statistical visualization |

---

## Зависимости

### Инфраструктурные зависимости

Jupyter Lab требует полной функциональности следующих сервисов:

#### 1. MinIO (Mandatory)

**Роль:** Хранилище Parquet файлов

**Требования:**
- S3 API совместимость
- Доступность из Jupyter контейнера (сетевой путь)
- Credentials: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Endpoint: `http://minio:9000` (Docker internal) или `https://minio.prod.example.com` (Production)

**Health Check:**
```bash
aws s3 ls --endpoint-url http://minio:9000 \
  --aws-access-key-id minioadmin \
  --aws-secret-access-key minioadmin
```

#### 2. Trino (Mandatory)

**Роль:** SQL движок для интерактивных запросов

**Требования:**
- Availability на порту 8080 (или кастомном)
- Hive Connector со включённым каталогом `hive`
- S3 connector для доступа к MinIO
- Metastore connection (Hive Metastore via PostgreSQL)

**Health Check:**
```bash
curl -X GET http://trino:8080/v1/info
```

#### 3. Hive Metastore (Mandatory)

**Роль:** Каталог таблиц и метаданные

**Требования:**
- Доступность Trino к Hive Metastore (port 9083)
- PostgreSQL backend
- Proper schema initialization

#### 4. PostgreSQL (Indirect dependency)

**Роль:** Backend для Hive Metastore

**Требования:**
- Database: `metastore_db`
- User: `hive` с правами

---

## Требования к инфраструктуре

### Minimal Setup (Development)

```
CPU:      2 cores
Memory:   4 GB (Jupyter) + 8 GB (dependencies)
Disk:     20 GB (OS + packages + notebooks cache)
Network:  100 Mbps (local Docker network)
```

### Recommended (Single-User Production)

```
CPU:      4 cores
Memory:   8 GB (Jupyter) + 12 GB (Python deps) = 20 GB total
Disk:     50 GB SSD (faster I/O for S3FS)
Network:  1 Gbps (internal DC network)
Firewall: Only allow ports 8888 (Jupyter) from analyst machines
```

### Production Multi-User (JupyterHub Cluster)

```
Hub Server:
  CPU:      2 cores (lightweight, just orchestration)
  Memory:   4 GB
  Disk:     20 GB (state, history)

Per Single-User Instance:
  CPU:      2 cores (can burst to 4)
  Memory:   8 GB
  Disk:     10 GB ephemeral (cleanup after session)

Load Balancer:
  CPU:      2 cores
  Memory:   2 GB
  Disk:     10 GB

Total for 10 concurrent users:
  CPU:      32+ cores
  Memory:   100+ GB
  Network:  10 Gbps recommended
  Storage:  1 TB (shared notebooks Git repo)
```

### Container Requirements

#### Docker Resource Limits

```yaml
resources:
  limits:
    cpus: "4.0"
    memory: 20G
  reservations:
    cpus: "2.0"
    memory: 8G
```

#### Kubernetes Pod Spec

```yaml
resources:
  requests:
    cpu: 2
    memory: 8Gi
  limits:
    cpu: 4
    memory: 20Gi
```

### Network Requirements

| Компонент | Port | Protocol | Requirement |
|-----------|------|----------|-------------|
| JupyterLab | 8888 | HTTP/HTTPS | TLS in production |
| MinIO API | 9000 | HTTP/HTTPS | Internal or VPN |
| Trino | 8080 | HTTP | Internal |
| SSH | 22 | TCP | For debugging (optional) |

---

## Single-User Jupyter Lab

### Dockerfile

```dockerfile
FROM jupyter/scipy-notebook:latest

USER root

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER jovyan

# Copy requirements and install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create notebooks directory
RUN mkdir -p /home/jovyan/work

WORKDIR /home/jovyan/work
```

### Docker Compose Configuration

```yaml
jupyter:
  build:
    context: ./data-lake/jupyter
    dockerfile: Dockerfile
  hostname: jupyter
  container_name: jupyter-notebook
  restart: unless-stopped
  ports:
    - "8888:8888"
  networks:
    - kafka
  environment:
    # JupyterLab settings
    JUPYTER_ENABLE_LAB: "yes"
    JUPYTER_TOKEN: "datalake"  # CHANGE IN PRODUCTION!

    # MinIO credentials (AWS-compatible)
    AWS_ACCESS_KEY_ID: minioadmin
    AWS_SECRET_ACCESS_KEY: minioadmin
    AWS_ENDPOINT_URL: http://minio:9000
    AWS_REGION: us-east-1

    # Optional: for Trino access
    TRINO_HOST: trino
    TRINO_PORT: 8080
    TRINO_CATALOG: hive

    # Notebook kernel settings
    JUPYTER_ENABLE_KERNELSPEC_OVERRIDE: "true"
  volumes:
    # Persistent notebooks storage
    - ./data-lake/jupyter/notebooks:/home/jovyan/work
    # Optional: shared config
    - ./data-lake/jupyter/config:/home/jovyan/.jupyter
  depends_on:
    - minio
    - trino
    - hive-metastore
```

### Starting Jupyter Lab

```bash
# Build image
docker-compose build jupyter

# Start service
docker-compose up -d jupyter

# View logs
docker-compose logs -f jupyter

# Access: http://localhost:8888?token=datalake
```

### Advanced Configuration

#### jupyter_lab_config.py

```python
# Location: ~/.jupyter/jupyter_lab_config.py

c.ServerApp.token = 'secure-token-here'  # Random string
c.ServerApp.allow_root = False
c.ServerApp.allow_password_change = True
c.ServerApp.password = 'hashed-password'  # Use jupyter lab password-hash

# Max upload size: 100 MB
c.ServerApp.max_body_size = 100 * 1024 * 1024

# Auto-save every 2 minutes
c.ContentsManager.autosave_interval = 120

# Disable terminal (for security)
c.ServerApp.terminado_settings = {'shell_command': []}

# Allow notebook downloads
c.NotebookApp.allow_download = True

# Notebook storage
c.ContentsManager.root_dir = '/home/jovyan/work'
```

#### jupyter_notebook_config.py

```python
# Kernel and execution settings
c.NotebookApp.notebook_dir = '/home/jovyan/work'
c.NotebookApp.kernel_spec_manager_class = 'jupyter_client.kernelspec.KernelSpecManager'

# Execution timeout (seconds)
c.NotebookApp.execution_timeout = 3600  # 1 hour

# Store notebooks as .ipynb only
c.NotebookApp.contents_manager_class = 'notebook.services.contents.filemanager.FileContentsManager'
```

---

## Multi-User JupyterHub

### Production-Grade Architecture

JupyterHub для production сценариев с 10+ аналитиками:

#### Hub Configuration

```yaml
jupyterhub:
  image: jupyterhub/jupyterhub:4.0.0
  container_name: jupyterhub-hub
  restart: unless-stopped
  ports:
    - "8000:8000"  # Main hub port
  networks:
    - analytics
  environment:
    # Database for hub state
    JUPYTERHUB_DB: postgresql://hub:hubpassword@postgres:5432/jupyterhub

    # OAuth2 / LDAP settings (see Authentication section)
    OAUTH_CALLBACK_URL: https://jupyter.prod.example.com/hub/oauth_callback
  volumes:
    - ./jupyterhub/jupyterhub_config.py:/srv/jupyterhub/jupyterhub_config.py
    - jupyterhub-data:/srv/jupyterhub
  depends_on:
    - postgres
```

#### jupyterhub_config.py

```python
import os
from dockerspawner import DockerSpawner
from jupyterhub.spawner import SimpleLocalProcessSpawner

# ===========================
# Basic Configuration
# ===========================
c.JupyterHub.hub_ip = '0.0.0.0'
c.JupyterHub.hub_port = 8000
c.JupyterHub.port = 8000

c.JupyterHub.db_url = os.environ.get('JUPYTERHUB_DB',
    'sqlite:////srv/jupyterhub/jupyterhub.sqlite')

c.JupyterHub.log_level = 'DEBUG'
c.JupyterHub.log_format = '%(asctime)s %(levelname)s - %(message)s'

# ===========================
# Spawner Configuration (Docker)
# ===========================
c.JupyterHub.spawner_class = 'dockerspawner.DockerSpawner'

c.DockerSpawner.image = 'etl-engine/jupyter-single-user:latest'
c.DockerSpawner.network_name = 'jupyterhub-network'

# Resource limits per user
c.DockerSpawner.mem_limit = '8G'
c.DockerSpawner.cpu_limit = 4.0

# Volumes
c.DockerSpawner.volumes = {
    'jupyterhub-user-{username}': '/home/jovyan/work',
    'jupyterhub-shared': '/home/jovyan/shared'
}

# Environment variables for each user container
c.DockerSpawner.environment = {
    'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID'),
    'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY'),
    'AWS_ENDPOINT_URL': 'http://minio:9000',
    'AWS_REGION': 'us-east-1',
    'TRINO_HOST': 'trino',
    'TRINO_PORT': '8080',
    'TRINO_CATALOG': 'hive'
}

# Auto-remove containers when user logs out
c.DockerSpawner.remove = True

# ===========================
# Authentication
# ===========================
# See Authentication section below

# ===========================
# User Management
# ===========================
c.Authenticator.whitelist = {
    'analyst1', 'analyst2', 'analyst3'
}

# Admin users
c.Authenticator.admin_users = {'admin', 'data-engineer'}

# ===========================
# Service Configuration
# ===========================
c.JupyterHub.services = [
    {
        'name': 'cull-idle',
        'admin': True,
        'command': ['cull-idle-servers', '--timeout=3600', '--cull-every=600']
    }
]

# ===========================
# Security
# ===========================
c.JupyterHub.cookie_secret_file = '/srv/jupyterhub/jupyterhub_cookie_secret'
c.JupyterHub.proxy_auth_token = os.environ.get('JUPYTERHUB_API_TOKEN')
```

#### Kubernetes Deployment

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: jupyterhub-config
data:
  jupyterhub_config.py: |
    # Config from above

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jupyterhub-hub
spec:
  replicas: 1
  selector:
    matchLabels:
      app: jupyterhub-hub
  template:
    metadata:
      labels:
        app: jupyterhub-hub
    spec:
      containers:
      - name: hub
        image: jupyterhub/jupyterhub:4.0.0
        ports:
        - containerPort: 8000
        env:
        - name: JUPYTERHUB_DB
          valueFrom:
            secretKeyRef:
              name: jupyterhub-db
              key: url
        volumeMounts:
        - name: config
          mountPath: /srv/jupyterhub/jupyterhub_config.py
          subPath: jupyterhub_config.py
        - name: data
          mountPath: /srv/jupyterhub
        resources:
          requests:
            cpu: 1
            memory: 2Gi
          limits:
            cpu: 2
            memory: 4Gi
      volumes:
      - name: config
        configMap:
          name: jupyterhub-config
      - name: data
        persistentVolumeClaim:
          claimName: jupyterhub-data-pvc

---
apiVersion: v1
kind: Service
metadata:
  name: jupyterhub-hub
spec:
  type: LoadBalancer
  ports:
  - port: 80
    targetPort: 8000
    protocol: TCP
  selector:
    app: jupyterhub-hub
```

---

## Python Packages для Data Lake

### requirements.txt

```
# Core data processing
pyarrow>=14.0.0
pandas>=2.0.0

# S3 access
s3fs>=2023.12.0
boto3>=1.34.0

# Visualization
matplotlib>=3.8.0
seaborn>=0.13.0

# JupyterLab
jupyterlab>=4.0.0

# Optional but recommended
numpy>=1.24.0
scipy>=1.10.0
scikit-learn>=1.3.0

# Trino client
trino>=0.20.0

# Notebooks utilities
ipywidgets>=8.0.0
plotly>=5.17.0

# Data quality
great-expectations>=0.17.0

# Development tools
jupyter-contrib-nbextensions>=0.7.0
jupyterlab-lsp>=4.0.0
python-lsp-server>=1.7.0

# AWS CLI (optional)
awscli>=1.29.0
```

### Package Descriptions

#### PyArrow (14.0.0+)

**Назначение:** Native Parquet support

```python
import pyarrow.parquet as pq

# Read Parquet from S3
table = pq.read_table('s3://datalake/table.parquet',
                      endpoint_override='http://minio:9000')
df = table.to_pandas()

# Write with custom compression
pq.write_table(table, 's3://datalake/output.parquet',
               compression='snappy',
               row_group_size=100000)
```

#### S3FS (2023.12.0+)

**Назначение:** File-system interface to MinIO

```python
import s3fs

# Create S3 file system
fs = s3fs.S3FileSystem(
    anon=False,
    endpoint_url='http://minio:9000',
    key='minioadmin',
    secret='minioadmin'
)

# List files
files = fs.ls('datalake/orders/')

# Read directly
with fs.open('datalake/data.parquet', 'rb') as f:
    df = pd.read_parquet(f)
```

#### Boto3 (1.34.0+)

**Назначение:** AWS SDK operations

```python
import boto3

s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

# List buckets
response = s3_client.list_buckets()
for bucket in response['Buckets']:
    print(bucket['Name'])

# Head object (metadata)
obj = s3_client.head_object(Bucket='datalake', Key='orders/data.parquet')
print(f"Size: {obj['ContentLength']} bytes")
print(f"Last modified: {obj['LastModified']}")
```

#### Pandas (2.0.0+)

**Назначение:** Data manipulation and analysis

```python
import pandas as pd

# Read Parquet with pandas
df = pd.read_parquet('s3://datalake/orders.parquet',
                     storage_options={
                         'endpoint_url': 'http://minio:9000',
                         'key': 'minioadmin',
                         'secret': 'minioadmin'
                     })

# Basic analysis
print(df.describe())
print(df.info())

# Grouping and aggregation
summary = df.groupby('category').agg({
    'amount': ['sum', 'mean', 'count'],
    'date': 'max'
})
```

#### Trino Python Client (0.20.0+)

**Назначение:** SQL queries via Trino

```python
from trino.dbapi import connect
import trino.auth

# Connect to Trino
conn = connect(
    host='trino',
    port=8080,
    catalog='hive',
    schema='default',
    user='analyst1',
    auth=trino.auth.BasicAuth('analyst1', 'password')
)

cursor = conn.cursor()

# Execute query
cursor.execute("""
    SELECT order_id, SUM(amount) as total
    FROM orders
    WHERE date >= DATE '2025-01-01'
    GROUP BY order_id
""")

# Fetch results
for row in cursor.fetchall():
    print(row)
```

#### Matplotlib & Seaborn (Visualization)

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

# Create plot
fig, ax = plt.subplots()
df.groupby('date')['amount'].sum().plot(ax=ax, label='Daily Total')
ax.set_title('Order Amount Over Time')
ax.set_xlabel('Date')
ax.set_ylabel('Amount ($)')
plt.show()
```

---

## Конфигурация подключений

### MinIO Configuration

#### Environment Setup

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://minio:9000
export AWS_REGION=us-east-1
export AWS_S3_SIGNATURE_VERSION=s3v4
```

#### Connection Test Notebook

```python
# test_minio_connection.ipynb

import boto3
import s3fs
import pandas as pd

# Test 1: Basic connection with boto3
client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

try:
    response = client.list_buckets()
    print("✓ MinIO connection successful")
    print(f"  Buckets: {[b['Name'] for b in response['Buckets']]}")
except Exception as e:
    print(f"✗ MinIO connection failed: {e}")

# Test 2: S3FS access
fs = s3fs.S3FileSystem(
    anon=False,
    endpoint_url='http://minio:9000',
    key='minioadmin',
    secret='minioadmin'
)

try:
    files = fs.ls('datalake/')
    print("✓ S3FS access successful")
    print(f"  Files: {len(files)}")
except Exception as e:
    print(f"✗ S3FS access failed: {e}")

# Test 3: Parquet read
try:
    df = pd.read_parquet(
        's3://datalake/sample.parquet',
        storage_options={
            'endpoint_url': 'http://minio:9000',
            'key': 'minioadmin',
            'secret': 'minioadmin'
        }
    )
    print("✓ Parquet read successful")
    print(f"  Shape: {df.shape}")
except Exception as e:
    print(f"✗ Parquet read failed: {e}")
```

### Trino Configuration

#### Connection Setup

```python
# trino_config.py

from trino.dbapi import connect
import trino.auth

class TrinoConnection:
    def __init__(self, host='trino', port=8080,
                 user='analyst1', catalog='hive', schema='default'):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.conn = None

    def connect(self):
        """Establish connection to Trino"""
        self.conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            auth=trino.auth.BasicAuth(self.user, 'password')
        )
        return self.conn

    def query(self, sql):
        """Execute query and return results as DataFrame"""
        if not self.conn:
            self.connect()

        cursor = self.conn.cursor()
        cursor.execute(sql)

        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

        import pandas as pd
        return pd.DataFrame(data, columns=columns)

    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()

# Usage
trino = TrinoConnection()
df = trino.query("""
    SELECT * FROM orders LIMIT 10
""")
trino.close()
```

#### Connection Test

```python
# test_trino_connection.ipynb

from trino.dbapi import connect
import trino.auth

try:
    conn = connect(
        host='trino',
        port=8080,
        user='analyst1',
        catalog='hive',
        schema='default'
    )
    cursor = conn.cursor()

    # Simple health check query
    cursor.execute("SELECT 1 as status")
    result = cursor.fetchone()

    print("✓ Trino connection successful")
    print(f"  Status: {result}")

    # List available tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'default'
    """)
    tables = cursor.fetchall()
    print(f"  Available tables: {len(tables)}")
    for table in tables:
        print(f"    - {table[0]}")

    conn.close()
except Exception as e:
    print(f"✗ Trino connection failed: {e}")
    import traceback
    traceback.print_exc()
```

#### Trino Catalog Configuration

```xml
<!-- data-lake/trino/catalog/hive.properties -->
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
hive.allow-drop-table=true
hive.allow-rename-table=true
```

---

## Примеры Notebook кода

### 1. Подключение к Trino и базовый запрос

```python
# notebook: 01_trino_basics.ipynb

import pandas as pd
from trino.dbapi import connect

# ===========================
# Подключение к Trino
# ===========================

conn = connect(
    host='trino',
    port=8080,
    user='analyst1',
    catalog='hive',
    schema='default'
)

cursor = conn.cursor()

# ===========================
# Простой запрос
# ===========================

query = """
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount
FROM orders
WHERE order_date >= DATE '2025-01-01'
LIMIT 1000
"""

cursor.execute(query)
columns = [desc[0] for desc in cursor.description]
data = cursor.fetchall()

df_orders = pd.DataFrame(data, columns=columns)

print(f"Downloaded {len(df_orders)} rows")
print(df_orders.head())

conn.close()
```

### 2. Чтение Parquet с фильтрацией по партициям

```python
# notebook: 02_parquet_reading.ipynb

import pandas as pd
import pyarrow.parquet as pq
import s3fs

# ===========================
# Инициализация S3FS
# ===========================

fs = s3fs.S3FileSystem(
    anon=False,
    endpoint_url='http://minio:9000',
    key='minioadmin',
    secret='minioadmin',
    use_ssl=False
)

# ===========================
# Список доступных файлов
# ===========================

bucket = 'datalake'
prefix = 'calc_id=2025-01-15T10-00/dt=2025-01-15/'

files = fs.glob(f'{bucket}/{prefix}*.parquet')
print(f"Found {len(files)} parquet files")
print("Sample files:")
for f in files[:3]:
    print(f"  - {f}")

# ===========================
# Чтение одного файла
# ===========================

file_path = f's3://{files[0]}'
table = pq.read_table(
    file_path,
    endpoint_override='http://minio:9000',
    storage_options={
        'key': 'minioadmin',
        'secret': 'minioadmin'
    }
)

df_single = table.to_pandas()
print(f"\nFile shape: {df_single.shape}")
print(df_single.dtypes)
print("\nFirst rows:")
print(df_single.head())

# ===========================
# Чтение нескольких файлов (concatenation)
# ===========================

dfs = []
for parquet_file in files[:10]:  # Limit to first 10 files
    try:
        table = pq.read_table(
            f's3://{parquet_file}',
            endpoint_override='http://minio:9000',
            storage_options={
                'key': 'minioadmin',
                'secret': 'minioadmin'
            }
        )
        dfs.append(table.to_pandas())
    except Exception as e:
        print(f"Error reading {parquet_file}: {e}")

if dfs:
    df_combined = pd.concat(dfs, ignore_index=True)
    print(f"\nCombined shape: {df_combined.shape}")
    print(f"Memory usage: {df_combined.memory_usage(deep=True).sum() / 1e9:.2f} GB")
```

### 3. Аналитический анализ с Pandas и визуализацией

```python
# notebook: 03_analysis_and_visualization.ipynb

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from trino.dbapi import connect

# ===========================
# Данные из Trino
# ===========================

conn = connect(
    host='trino',
    port=8080,
    user='analyst1',
    catalog='hive',
    schema='default'
)

cursor = conn.cursor()
cursor.execute("""
    SELECT
        DATE(order_date) as date,
        customer_id,
        SUM(amount) as daily_amount
    FROM orders
    WHERE order_date >= DATE '2025-01-01'
    GROUP BY 1, 2
""")

columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(cursor.fetchall(), columns=columns)
conn.close()

print(f"Loaded {len(df)} rows")
print(df.head())

# ===========================
# Анализ
# ===========================

# Агрегирование по дате
daily_totals = df.groupby('date')['daily_amount'].sum().reset_index()
print(f"\nDaily statistics:")
print(daily_totals.describe())

# Анализ по покупателям
customer_stats = df.groupby('customer_id').agg({
    'daily_amount': ['sum', 'mean', 'count']
}).reset_index()
customer_stats.columns = ['customer_id', 'total_spent', 'avg_daily', 'transaction_count']
customer_stats = customer_stats.sort_values('total_spent', ascending=False)

print(f"\nTop 10 customers:")
print(customer_stats.head(10))

# ===========================
# Визуализация
# ===========================

fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# График 1: Временной ряд
ax1 = axes[0, 0]
ax1.plot(daily_totals['date'], daily_totals['daily_amount'], marker='o')
ax1.set_title('Daily Order Amount Over Time')
ax1.set_xlabel('Date')
ax1.set_ylabel('Amount ($)')
ax1.grid(True)
ax1.tick_params(axis='x', rotation=45)

# График 2: Распределение
ax2 = axes[0, 1]
ax2.hist(df['daily_amount'], bins=50, edgecolor='black')
ax2.set_title('Distribution of Daily Amounts')
ax2.set_xlabel('Amount ($)')
ax2.set_ylabel('Frequency')

# График 3: Top customers
ax3 = axes[1, 0]
top_10 = customer_stats.head(10)
ax3.barh(range(len(top_10)), top_10['total_spent'].values)
ax3.set_yticks(range(len(top_10)))
ax3.set_yticklabels([f"Customer {int(cid)}" for cid in top_10['customer_id'].values])
ax3.set_title('Top 10 Customers by Total Spent')
ax3.set_xlabel('Total Amount ($)')
ax3.invert_yaxis()

# График 4: Статистика
ax4 = axes[1, 1]
ax4.axis('off')
stats_text = f"""
Dataset Statistics
{'='*30}

Total Transactions: {len(df):,}
Date Range: {df['date'].min()} to {df['date'].max()}
Unique Customers: {df['customer_id'].nunique():,}

Amount Statistics:
  Min: ${df['daily_amount'].min():.2f}
  Max: ${df['daily_amount'].max():.2f}
  Mean: ${df['daily_amount'].mean():.2f}
  Median: ${df['daily_amount'].median():.2f}
  Std Dev: ${df['daily_amount'].std():.2f}
"""
ax4.text(0.1, 0.9, stats_text, transform=ax4.transAxes,
         fontfamily='monospace', verticalalignment='top',
         fontsize=10)

plt.tight_layout()
plt.show()

# ===========================
# Экспорт результатов
# ===========================

# Сохранить в CSV
customer_stats.to_csv('/tmp/customer_analysis.csv', index=False)

# Сохранить в Parquet
customer_stats.to_parquet(
    's3://datalake/analysis/customer_stats.parquet',
    storage_options={
        'endpoint_url': 'http://minio:9000',
        'key': 'minioadmin',
        'secret': 'minioadmin'
    }
)

print("\n✓ Analysis complete and exported")
```

### 4. Machine Learning с scikit-learn

```python
# notebook: 04_machine_learning.ipynb

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestRegressor
import matplotlib.pyplot as plt
from trino.dbapi import connect

# ===========================
# Загрузка данных
# ===========================

conn = connect(
    host='trino',
    port=8080,
    user='analyst1',
    catalog='hive',
    schema='default'
)

cursor = conn.cursor()
cursor.execute("""
    SELECT
        customer_id,
        COUNT(*) as num_orders,
        SUM(amount) as total_spent,
        AVG(amount) as avg_order,
        MAX(order_date) as last_order_date,
        MIN(order_date) as first_order_date
    FROM orders
    GROUP BY customer_id
    HAVING COUNT(*) > 5
""")

columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(cursor.fetchall(), columns=columns)
conn.close()

print(f"Loaded {len(df)} customers with 5+ orders")

# ===========================
# Подготовка данных
# ===========================

# Calculate days as customer
df['first_order_date'] = pd.to_datetime(df['first_order_date'])
df['last_order_date'] = pd.to_datetime(df['last_order_date'])
df['days_as_customer'] = (df['last_order_date'] - df['first_order_date']).dt.days
df['order_frequency'] = df['num_orders'] / (df['days_as_customer'] + 1)

# Features for clustering
features = ['num_orders', 'total_spent', 'avg_order', 'order_frequency']
X = df[features].copy()
X = X.fillna(0)

# Normalize
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# ===========================
# Clustering (RFM-подобный анализ)
# ===========================

kmeans = KMeans(n_clusters=4, random_state=42)
df['segment'] = kmeans.fit_predict(X_scaled)

print("\nCustomer Segments:")
for segment in range(4):
    segment_data = df[df['segment'] == segment]
    print(f"\nSegment {segment} ({len(segment_data)} customers):")
    print(f"  Avg Orders: {segment_data['num_orders'].mean():.1f}")
    print(f"  Avg Spent: ${segment_data['total_spent'].mean():.2f}")
    print(f"  Avg Order Value: ${segment_data['avg_order'].mean():.2f}")

# ===========================
# Prediction (простая регрессия)
# ===========================

# Предскажем next month spending
df['next_month_spent'] = df['total_spent'] * np.random.uniform(0.8, 1.2, len(df))

X_train = df[features[:-1]]  # Exclude order_frequency
y_train = df['total_spent']

rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
rf_model.fit(X_train, y_train)

# Feature importance
importance_df = pd.DataFrame({
    'feature': X_train.columns,
    'importance': rf_model.feature_importances_
}).sort_values('importance', ascending=False)

print("\nFeature Importance:")
print(importance_df)

# ===========================
# Визуализация
# ===========================

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Segments in 2D
ax = axes[0, 0]
for segment in range(4):
    mask = df['segment'] == segment
    ax.scatter(df[mask]['total_spent'], df[mask]['num_orders'],
              label=f'Segment {segment}', alpha=0.6, s=50)
ax.set_xlabel('Total Spent ($)')
ax.set_ylabel('Number of Orders')
ax.set_title('Customer Segments')
ax.legend()
ax.grid(True, alpha=0.3)

# Feature importance
ax = axes[0, 1]
ax.barh(importance_df['feature'], importance_df['importance'])
ax.set_xlabel('Importance')
ax.set_title('Feature Importance (Random Forest)')

# Distribution by segment
ax = axes[1, 0]
segment_data = df.groupby('segment')['total_spent'].apply(list)
ax.boxplot([segment_data[i] for i in range(4)])
ax.set_xticklabels([f'Segment {i}' for i in range(4)])
ax.set_ylabel('Total Spent ($)')
ax.set_title('Spending Distribution by Segment')

# Predictions
ax = axes[1, 1]
y_pred = rf_model.predict(X_train)
ax.scatter(y_train, y_pred, alpha=0.5, s=20)
ax.plot([y_train.min(), y_train.max()], [y_train.min(), y_train.max()], 'r--', lw=2)
ax.set_xlabel('Actual Spending ($)')
ax.set_ylabel('Predicted Spending ($)')
ax.set_title('Model Predictions')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print(f"\nR² Score: {rf_model.score(X_train, y_train):.3f}")
```

---

## Authentication и Авторизация

### Single-User (Development)

#### Token-Based (Default)

```bash
# Generate token
jupyter lab --generate-config

# Set token in config
echo "c.ServerApp.token = 'my-secret-token'" >> ~/.jupyter/jupyter_lab_config.py

# Start with token
jupyter lab --NotebookApp.token='my-secret-token'

# Access: http://localhost:8888?token=my-secret-token
```

#### Password-Based

```bash
# Generate password hash
from jupyter.auth import passwd
passwd()  # Enter password, get hash

# Store in config
echo "c.ServerApp.password_required = True" >> ~/.jupyter/jupyter_lab_config.py
echo "c.ServerApp.password = 'sha1:...'" >> ~/.jupyter/jupyter_lab_config.py
```

### Multi-User (JupyterHub)

#### LDAP Authentication

```python
# jupyterhub_config.py

from jupyterhub.auth import LDAPAuthenticator

c.JupyterHub.authenticator_class = LDAPAuthenticator

c.LDAPAuthenticator.server_address = 'ldap://ldap.example.com'
c.LDAPAuthenticator.bind_dn_template = 'uid={username},ou=users,dc=example,dc=com'
c.LDAPAuthenticator.user_search_base = 'ou=users,dc=example,dc=com'
c.LDAPAuthenticator.user_search_filter = '(&(objectClass=inetOrgPerson)(uid={login}))'
c.LDAPAuthenticator.group_search_base = 'ou=groups,dc=example,dc=com'
c.LDAPAuthenticator.group_member_attribute = 'uniqueMember'
```

#### OAuth2 (GitHub, Google)

```python
# jupyterhub_config.py - GitHub OAuth

from oauthlib.oauth2 import WebApplicationClient
from jupyterhub.auth import OAuthenticator

class GitHubOAuthenticator(OAuthenticator):
    client_id = os.environ.get('GITHUB_CLIENT_ID')
    client_secret = os.environ.get('GITHUB_CLIENT_SECRET')
    oauth_callback_url = 'https://jupyter.example.com/hub/oauth_callback'
    authorize_url = 'https://github.com/login/oauth/authorize'
    token_url = 'https://github.com/login/oauth/access_token'
    userdata_url = 'https://api.github.com/user'
    scope = ['user:email']

c.JupyterHub.authenticator_class = GitHubOAuthenticator

# Allow specific organizations
c.GitHubOAuthenticator.allowed_organizations = ['my-company']

# Admin users
c.Authenticator.admin_users = ['github-username1', 'github-username2']
```

#### OIDC (OpenID Connect)

```python
# jupyterhub_config.py - Generic OIDC

from jupyterhub.auth import OAuthenticator

c.JupyterHub.authenticator_class = 'oauthenticator.oidc.OIDCAuthenticator'

c.OIDCAuthenticator.client_id = os.environ.get('OIDC_CLIENT_ID')
c.OIDCAuthenticator.client_secret = os.environ.get('OIDC_CLIENT_SECRET')
c.OIDCAuthenticator.oauth_callback_url = 'https://jupyter.example.com/hub/oauth_callback'
c.OIDCAuthenticator.authorize_url = 'https://oidc.example.com/authorize'
c.OIDCAuthenticator.token_url = 'https://oidc.example.com/token'
c.OIDCAuthenticator.userdata_url = 'https://oidc.example.com/userinfo'
```

#### Access Control and Quotas

```python
# jupyterhub_config.py

# User whitelist
c.Authenticator.whitelist = {
    'analyst1', 'analyst2', 'data-engineer', 'admin'
}

# Admin users (full access, can manage hub)
c.Authenticator.admin_users = {'admin', 'data-engineer'}

# Per-user resource limits
c.Spawner.mem_limit = '8G'
c.Spawner.cpu_limit = 4

# Cull idle servers after 1 hour of inactivity
c.JupyterHub.services = [
    {
        'name': 'cull-idle',
        'admin': True,
        'command': ['cull-idle-servers', '--timeout=3600', '--cull-every=600']
    }
]

# Disable user-initiated server stop (prevent accidental shutdown)
c.JupyterHub.allow_named_servers = False

# Notebook execution timeout
c.Spawner.notebook_dir = '/home/jovyan/work'
c.Spawner.cpu_guarantee = 1.0  # Min CPU reserved
```

---

## Мониторинг

### Metrics to Collect

#### JupyterLab Metrics

```
jupyter_running_kernels          # Number of active kernels
jupyter_notebook_count           # Total notebooks
jupyter_session_count            # Active sessions
jupyter_http_requests_total      # HTTP request count
jupyter_http_request_duration    # Request latency (ms)
jupyter_kernel_start_duration    # Kernel startup time
```

#### System Metrics

```
container_memory_usage_bytes     # Memory consumption
container_cpu_usage_seconds      # CPU usage
container_network_transmit_bytes # Network I/O
container_fs_usage_bytes         # Disk usage
```

### Prometheus Configuration

```yaml
# prometheus/jupyter_targets.yml

- job_name: 'jupyter'
  static_configs:
    - targets: ['jupyter:8888']
  metrics_path: '/metrics'
  scrape_interval: 30s

- job_name: 'jupyterhub'
  static_configs:
    - targets: ['jupyterhub:8081']
  metrics_path: '/hub/metrics'
  scrape_interval: 30s
```

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "Jupyter Lab Monitoring",
    "panels": [
      {
        "title": "Running Kernels",
        "targets": [
          {
            "expr": "jupyter_running_kernels"
          }
        ]
      },
      {
        "title": "Memory Usage",
        "targets": [
          {
            "expr": "container_memory_usage_bytes{container='jupyter'} / 1024 / 1024 / 1024"
          }
        ]
      },
      {
        "title": "HTTP Request Rate",
        "targets": [
          {
            "expr": "rate(jupyter_http_requests_total[5m])"
          }
        ]
      },
      {
        "title": "Kernel Startup Time",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, jupyter_kernel_start_duration)"
          }
        ]
      }
    ]
  }
}
```

### Health Checks

#### Kubernetes Probes

```yaml
livenessProbe:
  httpGet:
    path: /api
    port: 8888
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /api
    port: 8888
  initialDelaySeconds: 10
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 2
```

#### Health Check Script

```bash
#!/bin/bash
# scripts/health_check.sh

# Check JupyterLab is responding
curl -f http://localhost:8888/api || exit 1

# Check kernel is available
curl -s http://localhost:8888/api/kernelspecs | grep -q "python" || exit 1

# Check Trino connectivity
python3 -c "
from trino.dbapi import connect
conn = connect(host='trino', port=8080, user='test', catalog='hive')
cursor = conn.cursor()
cursor.execute('SELECT 1')
conn.close()
" || exit 1

# Check MinIO connectivity
python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin')
s3.head_bucket(Bucket='datalake')
" || exit 1

echo "All health checks passed"
exit 0
```

### Alerting Rules

```yaml
# prometheus/jupyter_alerts.yml

groups:
  - name: jupyter
    rules:
      - alert: JupyterLabDown
        expr: up{job="jupyter"} == 0
        for: 5m
        annotations:
          summary: "Jupyter Lab is down"

      - alert: JupyterHighMemory
        expr: container_memory_usage_bytes{container="jupyter"} / 1024 / 1024 / 1024 > 15
        for: 10m
        annotations:
          summary: "Jupyter Lab memory usage > 15GB"

      - alert: JupyterHighCPU
        expr: rate(container_cpu_usage_seconds{container="jupyter"}[5m]) > 3
        for: 10m
        annotations:
          summary: "Jupyter Lab CPU usage > 3 cores"

      - alert: JupyterKernelFails
        expr: rate(jupyter_kernel_start_failures[5m]) > 0.1
        for: 5m
        annotations:
          summary: "Jupyter kernel startup failures detected"
```

---

## Backup и Version Control

### Notebook Version Control with Git

#### Git Integration Setup

```bash
# Initialize git repo for notebooks
cd data-lake/jupyter/notebooks
git init
git config user.email "bot@example.com"
git config user.name "Jupyter Bot"

# Create .gitignore
cat > .gitignore <<EOF
.ipynb_checkpoints/
__pycache__/
*.pyc
.DS_Store
.env
tmp/
*.tmp
EOF

git add .gitignore
git commit -m "Initial commit: gitignore"
```

#### Jupytext Integration (Sync .ipynb and .py)

```bash
# Install jupytext
pip install jupytext

# Configure in jupyter_lab_config.py
c.ContentsManager.default_jupytext_formats = "ipynb,py"
c.ContentsManager.preferred_jupytext_formats_save = "ipynb"
```

#### Automatic Git Commits

```python
# scripts/auto_commit_notebooks.py

import os
import subprocess
from datetime import datetime

NOTEBOOKS_DIR = '/home/jovyan/work'

def commit_notebooks():
    """Automatically commit changed notebooks"""
    os.chdir(NOTEBOOKS_DIR)

    # Check if there are changes
    result = subprocess.run(['git', 'status', '--porcelain'],
                          capture_output=True, text=True)
    if not result.stdout.strip():
        print("No changes to commit")
        return

    # Add all changes
    subprocess.run(['git', 'add', '.'], check=True)

    # Commit with timestamp
    timestamp = datetime.now().isoformat()
    subprocess.run(['git', 'commit', '-m', f'Auto-save: {timestamp}'],
                  check=True)

    # Push to remote (optional)
    try:
        subprocess.run(['git', 'push', 'origin', 'main'],
                      timeout=30, check=False)
    except Exception as e:
        print(f"Warning: Could not push: {e}")

if __name__ == '__main__':
    commit_notebooks()
```

#### Cron Job for Auto-Commit

```bash
# crontab entry

# Commit notebooks every hour
0 * * * * /usr/local/bin/python3 /scripts/auto_commit_notebooks.py >> /var/log/jupyter_commits.log 2>&1

# Cleanup old notebooks (> 30 days in trash) daily
0 2 * * * find /home/jovyan/work/.trash -type f -mtime +30 -delete
```

### Backup Strategy

#### Volume Snapshots

```yaml
# Kubernetes PersistentVolume backup

apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: jupyter-notebooks-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
  storageClassName: fast-ssd

---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: jupyter-notebooks-snapshot
spec:
  volumeSnapshotClassName: csi-snapshotter
  source:
    persistentVolumeClaimName: jupyter-notebooks-pvc
```

#### S3 Backup

```bash
#!/bin/bash
# scripts/backup_notebooks.sh

NOTEBOOKS_DIR=/home/jovyan/work
BACKUP_BUCKET=s3://datalake-backups
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Create backup tar
tar czf /tmp/notebooks_${TIMESTAMP}.tar.gz \
  --exclude='.ipynb_checkpoints' \
  --exclude='__pycache__' \
  --exclude='.git' \
  ${NOTEBOOKS_DIR}

# Upload to S3
aws s3 cp /tmp/notebooks_${TIMESTAMP}.tar.gz \
  ${BACKUP_BUCKET}/notebooks/ \
  --endpoint-url http://minio:9000 \
  --region us-east-1

# Cleanup local backup
rm /tmp/notebooks_${TIMESTAMP}.tar.gz

# Keep only last 7 days of backups
aws s3 ls ${BACKUP_BUCKET}/notebooks/ --endpoint-url http://minio:9000 | \
  grep "notebooks_" | \
  awk '{print $4}' | \
  sort -r | \
  tail -n +8 | \
  xargs -I {} aws s3 rm ${BACKUP_BUCKET}/notebooks/{} --endpoint-url http://minio:9000

echo "Backup complete: notebooks_${TIMESTAMP}.tar.gz"
```

---

## Troubleshooting

### Common Issues and Solutions

#### 1. Jupyter Lab не запускается

**Симптомы:**
```
ERROR: Container exited with code 1
```

**Решение:**

```bash
# Проверить логи
docker-compose logs jupyter

# Пересобрать образ
docker-compose build --no-cache jupyter

# Проверить требования
docker run --rm -it etl-engine/jupyter:latest \
  python -m pip check

# Запустить с дебагом
docker-compose up jupyter (без -d флага для live логов)
```

#### 2. Cannot connect to MinIO

**Симптомы:**
```python
botocore.exceptions.EndpointConnectionError:
  Could not connect to the endpoint URL: http://minio:9000
```

**Решение:**

```python
# Test connectivity from notebook
import socket

try:
    result = socket.gethostbyname('minio')
    print(f"✓ DNS resolution: minio -> {result}")
except socket.gaierror as e:
    print(f"✗ DNS error: {e}")
    print("  Solution: Check docker network configuration")

# Test port connectivity
import socket
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('minio', 9000))
    s.close()
    print("✓ Port 9000 is open")
except socket.error as e:
    print(f"✗ Connection error: {e}")
    print("  Solution: Check MinIO service is running")
```

#### 3. Trino connection timeout

**Симптомы:**
```
sqlalchemy.exc.DBAPIError: (trino.exceptions.TrinoError)...
  Error connecting to Trino
```

**Решение:**

```bash
# Проверить Trino статус
curl -v http://trino:8080/v1/info

# Проверить логи Trino
docker-compose logs trino | tail -100

# Перезагрузить Trino
docker-compose restart trino

# Проверить Hive Metastore подключение
curl -v http://hive-metastore:9083
```

#### 4. Out of memory (OOM)

**Симптомы:**
```
MemoryError: Unable to allocate X GiB for an array
```

**Решение:**

```python
# Use chunked reading
import pandas as pd

# Read in chunks
chunks = []
for chunk in pd.read_parquet('s3://datalake/large_file.parquet',
                             chunksize=100000,
                             storage_options={...}):
    # Process chunk
    processed = chunk.groupby('id').sum()
    chunks.append(processed)

result = pd.concat(chunks)

# Or use Pandas with less memory
df = pd.read_parquet('s3://...',
                    columns=['col1', 'col2'],  # Select columns
                    filters=[('date', '>=', '2025-01-01')])  # Filter rows

# Monitor memory
import psutil
print(f"Memory: {psutil.virtual_memory().percent}%")
```

#### 5. Kernel crashes

**Симптомы:**
```
Kernel dead. Restarting...
```

**Решение:**

```bash
# Проверить системные ресурсы
docker stats jupyter

# Увеличить лимиты памяти в docker-compose
resources:
  limits:
    memory: 24G  # Increase from 20G
    cpus: 4

# Перезагрузить контейнер
docker-compose restart jupyter

# Очистить кэш ядра
rm -rf ~/.local/share/jupyter/runtime/

# Отключить автоматическое завершение
c.ZMQTerminalInteractiveShell.kernel_timeout = 0
```

#### 6. S3FS slow performance

**Симптомы:**
```
Listing files takes 30+ seconds
Reading files is very slow
```

**Решение:**

```python
import s3fs

# Optimize S3FS
fs = s3fs.S3FileSystem(
    anon=False,
    endpoint_url='http://minio:9000',
    key='minioadmin',
    secret='minioadmin',
    use_ssl=False,
    # Performance tuning
    client_kwargs={'max_pool_connections': 50},
    config_kwargs={'max_concurrent_requests': 20},
    cache_regions=True,  # Enable caching
    asynchronous=False
)

# Use direct boto3 for better performance
import boto3
s3 = boto3.client('s3',
                  endpoint_url='http://minio:9000',
                  config=boto3.session.Config(
                      max_pool_connections=50,
                      retries={'max_attempts': 3}
                  ))

# Use direct parquet read instead of s3fs
import pyarrow.parquet as pq
table = pq.read_table(
    's3://datalake/file.parquet',
    endpoint_override='http://minio:9000',
    storage_options={
        'key': 'minioadmin',
        'secret': 'minioadmin'
    }
)
```

#### 7. JupyterHub user server fails to start

**Симптомы:**
```
Spawning failed with exit code 1
```

**Решение:**

```bash
# Проверить образ DockerSpawner
docker pull your-registry/jupyter-single-user:latest

# Проверить логи Hub
docker-compose logs jupyterhub-hub

# Проверить Docker socket доступ
ls -la /var/run/docker.sock

# Убедиться что сеть существует
docker network ls | grep jupyterhub

# Перезагрузить Hub
docker-compose restart jupyterhub-hub
```

#### 8. Permission denied errors

**Симптомы:**
```
PermissionError: [Errno 13] Permission denied
```

**Решение:**

```bash
# Проверить права на volumes
ls -la data-lake/jupyter/notebooks/

# Установить правильные права
sudo chown -R 1000:100 data-lake/jupyter/notebooks/
chmod -R 755 data-lake/jupyter/notebooks/

# В Dockerfile убедиться что используется правильный пользователь
# FROM jupyter/scipy-notebook:latest
# USER jovyan  # UID 1000, GID 100
```

#### 9. Notebook loses connection to kernel

**Симптомы:**
```
WebSocket connection closed
Kernel appears to have died unexpectedly
```

**Решение:**

```bash
# Проверить heartbeat
c.KernelManager.kernel_manager_class = 'jupyter_client.manager.KernelManager'
c.KernelRestarter.restart_cycle_times = [0.1, 0.25, 0.5, 1, 3, 10]

# Увеличить timeouts
c.KernelRestarter.time_to_dead = 60  # seconds

# Проверить firewall rules
sudo ufw allow 8888

# Проверить browser WebSocket
# Open DevTools (F12) -> Console -> check for WebSocket errors
```

### Debug Mode

```bash
# Start JupyterLab with debug logging
jupyter lab --log-level=DEBUG

# In notebook, enable verbose logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Check environment variables
import os
for key in ['AWS_ACCESS_KEY_ID', 'TRINO_HOST']:
    print(f"{key}: {os.environ.get(key, 'NOT SET')}")

# Test all dependencies
def test_all():
    try:
        import pyarrow; print("✓ PyArrow")
    except: print("✗ PyArrow")

    try:
        import pandas; print("✓ Pandas")
    except: print("✗ Pandas")

    try:
        import s3fs; print("✓ S3FS")
    except: print("✗ S3FS")

    try:
        import boto3; print("✓ Boto3")
    except: print("✗ Boto3")

    try:
        from trino.dbapi import connect; print("✓ Trino")
    except: print("✗ Trino")

    try:
        import matplotlib; print("✓ Matplotlib")
    except: print("✗ Matplotlib")

test_all()
```

---

## Summary

Jupyter Lab в Data Lake архитектуре служит критически важным компонентом для интерактивной аналитики. Этот guide покрывает:

- **Development Setup:** быстрое начало с Docker Compose
- **Production Deployment:** JupyterHub для multi-user сценариев
- **Integration:** полный стек интеграции с MinIO, Trino, Hive Metastore
- **Security:** multiple authentication schemes (LDAP, OAuth2, OIDC)
- **Monitoring:** метрики, alerting, health checks
- **Best Practices:** backup, version control, notebook management
- **Troubleshooting:** решение типичных проблем

Следуя этому guide, вы сможете развернуть production-grade интерактивную аналитическую платформу для вашей организации.

---

## Links and References

- [Jupyter Project](https://jupyter.org/)
- [JupyterHub Documentation](https://jupyterhub.readthedocs.io/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [S3FS Documentation](https://s3fs.readthedocs.io/)
- [Trino Python Client](https://trino.io/docs/current/client/python.html)
- [Docker Spawner](https://github.com/jupyterhub/dockerspawner)
- [OAuthenticator](https://oauthenticator.readthedocs.io/)
