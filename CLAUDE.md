

## 🎯 Project Overview

### Core Mission
Build a FastAPI backend service that converts table screenshots and document images into downloadable XLSX files, with a focus on **batch processing capabilities** as the key differentiator.

### Key Differentiators
- **Batch Processing**: Process multiple images simultaneously in a single API call
- **OlmOCR Integration**: Leverage advanced OCR API for accurate text extraction
- **Railway Deployment**: Cloud-native design optimized for Railway platform
- **Scalable Architecture**: Async processing with Redis task queues

---

## 🏗️ Architecture Principles

### Technology Stack
- **Framework**: FastAPI (Python 3.11+)
- **OCR Service**: OlmOCR API (external service)
- **Data Processing**: pandas, openpyxl for XLSX generation
- **Task Queue**: Celery + Redis for batch processing
- **Storage**: Local filesystem with automatic cleanup
- **Deployment**: Docker + Railway
- **Database**: Redis (for job tracking and caching)



## 📁 Project Structure

```
project-root/
├── app/
│   ├── main.py              # FastAPI application entry point
│   ├── api/
│   │   ├── __init__.py
│   │   ├── v1/
│   │   │   ├── __init__.py
│   │   │   ├── convert.py   # Image conversion endpoints
│   │   │   ├── jobs.py      # Batch job management
│   │   │   └── download.py  # File download endpoints
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py        # Configuration and environment variables
│   │   ├── security.py      # Rate limiting and validation
│   │   └── dependencies.py  # FastAPI dependencies
│   ├── services/
│   │   ├── __init__.py
│   │   ├── olmocr.py        # OlmOCR API integration
│   │   ├── processor.py     # Image processing pipeline
│   │   ├── excel.py         # XLSX generation
│   │   └── storage.py       # File management
│   ├── models/
│   │   ├── __init__.py
│   │   ├── requests.py      # Pydantic request models
│   │   ├── responses.py     # Pydantic response models
│   │   └── jobs.py          # Job status and tracking models
│   ├── tasks/
│   │   ├── __init__.py
│   │   └── batch.py         # Celery tasks for batch processing
│   └── utils/
│       ├── __init__.py
│       ├── validators.py    # Input validation helpers
│       ├── exceptions.py    # Custom exception classes
│       └── helpers.py       # Utility functions
├── tests/
│   ├── __init__.py
│   ├── test_api/
│   ├── test_services/
│   └── test_integration/
├── docker/
│   └── Dockerfile
├── requirements.txt
├── .env.example
├── railway.json
└── README.md
```

---


### 2. Code Organization Patterns
- **Separation of Concerns**: Keep API, business logic, and data access separate
- **Dependency Injection**: Use FastAPI's dependency system for configuration and services
- **Error Handling**: Centralized exception handling with custom exception classes
- **Validation**: Use Pydantic models for request/response validation




### Core Endpoints Structure
```
POST /api/v1/convert/batch      # Batch image processing
GET  /api/v1/jobs/{job_id}      # Job status tracking
GET  /api/v1/download/{file_id} # File download
GET  /api/v1/health             # Health check
```


---


## 🔄 Batch Processing Workflow

### Job Lifecycle
1. **Job Creation**: Generate unique job ID and store job metadata
2. **Queue Management**: Add images to processing queue
3. **Processing**: Sequential or parallel processing based on resource availability
4. **Progress Tracking**: Update job status and progress in real-time
5. **Result Aggregation**: Combine results into final output format
6. **Cleanup**: Clean up temporary files and update job status

### Queue Management
- **Redis Integration**: Use Redis for job queuing and status tracking
- **Worker Processes**: Celery workers for background processing
- **Error Recovery**: Handle worker failures and job retry logic
- **Resource Management**: Limit concurrent jobs based on system resources

---