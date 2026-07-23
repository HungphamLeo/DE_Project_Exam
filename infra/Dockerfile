FROM apache/airflow:2.9.1

# Switch to root to install system-level dependencies
USER root

# Install build tools (like gcc) required by packages such as scikit-learn.
# Using --no-install-recommends and cleaning up reduces image size.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the non-root 'airflow' user for security
USER airflow

RUN pip install --upgrade pip
# Copy the requirements file into the image
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

