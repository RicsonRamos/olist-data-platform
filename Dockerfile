# Use Python 3.11 slim image for production
FROM python:3.11-slim

# Install system dependencies for psycopg2 and dbt
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command: Run the pipeline
CMD ["python", "-m", "pipeline.run_pipeline"]
