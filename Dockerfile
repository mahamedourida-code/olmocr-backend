# Multi-stage Dockerfile for FastAPI app with Celery workers
# Use the official Python 3.11 slim image
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PORT=8000

# Create app directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Create necessary directories
RUN mkdir -p /app/temp_storage && \
    chown -R appuser:appuser /app

# Copy application code
COPY app/ ./app/
COPY start.py ./

# Make startup script executable
RUN chmod +x start.py

# Change ownership of the app directory
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose the port the app runs on
EXPOSE $PORT

# Health check - only runs for web service type
HEALTHCHECK --interval=30s --timeout=30s --start-period=40s --retries=3 \
    CMD if [ "$SERVICE_TYPE" = "web" ] || [ -z "$SERVICE_TYPE" ]; then curl -f http://localhost:${PORT:-8000}/api/v1/health || exit 1; else exit 0; fi

# Default command - uses startup script for different service types
CMD ["python", "start.py"]