#!/usr/bin/env python3
"""
Startup script for different service types based on environment variable.
This allows Railway to run different services from the same Docker image.
"""
# Added comment for demonstration - October 11, 2025
import os
import sys
import subprocess
from pathlib import Path

def main():
    service_type = os.getenv("SERVICE_TYPE", "web")
    
    print(f"Starting service type: {service_type}")
    
    if service_type == "web":
        # Start the FastAPI web server with proper timeout and limit settings
        cmd = [
            "uvicorn",
            "app.main:app",
            "--host", "0.0.0.0",
            "--port", str(os.getenv("PORT", "8080")),
            "--workers", "1",
            "--timeout-keep-alive", "75",    # Keep-alive timeout in seconds
            "--timeout-graceful-shutdown", "30",  # Graceful shutdown timeout
            "--limit-concurrency", "100",    # Max concurrent connections
            "--limit-max-requests", "1000",  # Max requests before worker restart
            "--backlog", "2048"              # Socket backlog
        ]
    elif service_type == "worker":
        # Start Celery worker
        concurrency = os.getenv("WORKER_CONCURRENCY", "4")
        queues = os.getenv("WORKER_QUEUES", "image_processing,batch_processing,default")
        cmd = [
            "celery",
            "-A", "app.tasks.celery_app",
            "worker",
            "--loglevel=info",
            f"--concurrency={concurrency}",
            f"--queues={queues}"
        ]
    elif service_type == "beat":
        # Start Celery beat scheduler
        cmd = [
            "celery",
            "-A", "app.tasks.celery_app", 
            "beat",
            "--loglevel=info"
        ]
    elif service_type == "flower":
        # Start Flower monitoring
        port = os.getenv("PORT", "5555")
        cmd = [
            "celery",
            "-A", "app.tasks.celery_app",
            "flower",
            f"--port={port}",
            "--basic_auth=admin:flower123"  # Simple auth for monitoring
        ]
    else:
        print(f"Unknown service type: {service_type}")
        sys.exit(1)
    
    print(f"Executing command: {' '.join(cmd)}")
    
    # Execute the command
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        print("Received interrupt signal, shutting down...")
        sys.exit(0)

if __name__ == "__main__":
    main()