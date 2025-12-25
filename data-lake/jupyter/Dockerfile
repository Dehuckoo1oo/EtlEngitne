FROM jupyter/scipy-notebook:latest

USER root

# Install additional system dependencies if needed
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
