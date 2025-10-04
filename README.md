# OlmOCR Backend Service

A FastAPI backend service that converts table screenshots and document images into downloadable XLSX files with batch processing capabilities.

## Features

- **Single Image Conversion**: Convert individual images to XLSX format
- **Batch Processing**: Process multiple images simultaneously
- **Session Management**: Secure file access with automatic cleanup
- **OCR Integration**: Advanced text extraction using OlmOCR API via DeepInfra
- **Railway Deployment**: Optimized for Railway platform with ephemeral storage
- **File Management**: 24-hour retention with automatic cleanup

## Quick Start

### Local Development

1. **Clone and Setup**
   ```bash
   git clone <repository-url>
   cd olmocr-project
   cp .env.example .env
   ```

2. **Configure Environment**
   Edit `.env` file with your settings:
   ```bash
   OLMOCR_API_KEY=your_deepinfra_api_key
   DEBUG=true
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the Application**
   ```bash
   python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

5. **Access the API**
   - API Documentation: http://localhost:8000/docs
   - Health Check: http://localhost:8000/api/v1/health

### Railway Deployment

1. **Connect Railway**
   ```bash
   railway login
   railway link
   ```

2. **Set Environment Variables**
   ```bash
   railway variables set OLMOCR_API_KEY=your_deepinfra_api_key
   railway variables set DEBUG=false
   railway variables set ENVIRONMENT=production
   ```

3. **Deploy**
   ```bash
   railway up
   ```

### Redis & Celery Integration

This service uses Redis and Celery for high-performance batch processing:

#### Architecture
- **Web Service**: FastAPI server handling HTTP requests
- **Redis**: Job queue and result storage
- **Celery Workers**: Background image processing
- **Flower Monitoring**: Task monitoring interface (optional)

#### Railway Deployment with Redis

1. **Add Redis Add-on**
   ```bash
   # From Railway dashboard, add Redis add-on to your project
   # This automatically sets REDIS_URL environment variable
   ```

2. **Multi-Service Configuration**
   The application automatically detects Railway's Redis add-on and configures:
   - Job queuing for batch processing
   - Result caching for faster retrieval
   - Session management across restarts

3. **Performance Benefits**
   - Concurrent processing of multiple images
   - Reduced response times for batch jobs
   - Better resource utilization
   - Scalable architecture supporting high concurrent users

#### Local Redis Setup (Optional)

For local development with Redis:
```bash
# Using Docker
docker run -d -p 6379:6379 redis:7-alpine

# Or using Railway CLI
railway run redis-cli
```

Set local environment variable:
```bash
REDIS_URL=redis://localhost:6379/0
```

## API Endpoints

### Core Conversion

- `POST /api/v1/convert/single` - Convert single image to XLSX
- `POST /api/v1/convert/validate` - Validate image for conversion

### Batch Processing

- `POST /api/v1/jobs/batch` - Create batch conversion job
- `GET /api/v1/jobs/{job_id}/status` - Check job status
- `GET /api/v1/jobs/` - List all jobs for session
- `DELETE /api/v1/jobs/{job_id}` - Cancel job

### File Management

- `GET /api/v1/download/{file_id}` - Download XLSX file
- `GET /api/v1/download/{file_id}/info` - Get file information
- `GET /api/v1/download/{file_id}/preview` - Preview file contents
- `DELETE /api/v1/download/{file_id}` - Delete file

### Health & Monitoring

- `GET /api/v1/health` - Comprehensive health check
- `GET /api/v1/ping` - Simple availability check
- `GET /api/v1/version` - Application version info
- `GET /api/v1/stats` - Application statistics

## Usage Examples

### Single Image Conversion

```python
import requests
import base64

# Read and encode image
with open('table_image.jpg', 'rb') as f:
    image_data = base64.b64encode(f.read()).decode()

# Convert image
response = requests.post('http://localhost:8000/api/v1/convert/single', json={
    'image_data': image_data,
    'filename': 'my_table'
})

result = response.json()
if result['success']:
    # Download the XLSX file
    download_url = result['download_url']
    # ... handle download
```

### Batch Processing

```python
import requests
import base64

# Prepare multiple images
images = []
for filename in ['table1.jpg', 'table2.jpg', 'table3.jpg']:
    with open(filename, 'rb') as f:
        image_data = base64.b64encode(f.read()).decode()
    images.append({
        'image_data': image_data,
        'filename': filename.split('.')[0]
    })

# Start batch job
response = requests.post('http://localhost:8000/api/v1/jobs/batch', json={
    'images': images,
    'output_format': 'consolidated',
    'consolidation_strategy': 'separate_sheets'
})

job = response.json()
job_id = job['job_id']

# Check status
status_response = requests.get(f'http://localhost:8000/api/v1/jobs/{job_id}/status')
status = status_response.json()

if status['status'] == 'completed':
    download_url = status['results']['download_url']
    # ... handle download
```

## Architecture

### Project Structure

```
app/
├── main.py                 # FastAPI application entry point
├── api/v1/                # API endpoints
│   ├── convert.py         # Single conversion endpoints
│   ├── jobs.py            # Batch job management
│   ├── download.py        # File download endpoints
│   └── health.py          # Health check endpoints
├── core/                  # Core configuration
│   ├── config.py          # Settings management
│   └── dependencies.py    # FastAPI dependencies
├── services/              # Business logic
│   ├── olmocr.py          # OlmOCR API integration
│   ├── excel.py           # XLSX generation
│   └── storage.py         # File management
├── models/                # Pydantic models
│   ├── requests.py        # Request models
│   ├── responses.py       # Response models
│   └── jobs.py            # Job tracking models
└── utils/                 # Utilities
    ├── exceptions.py      # Custom exceptions
    ├── validators.py      # Input validation
    └── helpers.py         # Helper functions
```

### Key Components

- **FastAPI Application**: Async web framework with automatic OpenAPI docs
- **OlmOCR Integration**: OCR service via DeepInfra API
- **Session Management**: UUID-based sessions with 24-hour expiration
- **Background Tasks**: Async processing for batch operations
- **File Storage**: Ephemeral storage with automatic cleanup
- **Error Handling**: Comprehensive error handling with proper HTTP status codes

### Processing Workflow

1. **Image Upload**: Base64-encoded images via JSON API
2. **Validation**: File type, size, and format validation
3. **OCR Processing**: Text extraction using OlmOCR API
4. **Data Parsing**: CSV parsing and validation
5. **XLSX Generation**: Formatted Excel file creation
6. **Storage**: Temporary file storage with expiration
7. **Cleanup**: Automatic file removal after retention period

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OLMOCR_API_KEY` | DeepInfra API key for OlmOCR | Required |
| `OLMOCR_API_BASE_URL` | OlmOCR API endpoint | `https://api.deepinfra.com/v1/openai` |
| `DEBUG` | Enable debug mode | `false` |
| `PORT` | Application port | `8000` |
| `MAX_FILE_SIZE` | Max upload size (bytes) | `10485760` (10MB) |
| `MAX_BATCH_SIZE` | Max images per batch | `20` |
| `FILE_RETENTION_HOURS` | File retention period | `24` |
| `RATE_LIMIT_REQUESTS` | Requests per window | `100` |
| `RATE_LIMIT_WINDOW` | Rate limit window (seconds) | `3600` |

### Rate Limiting

- **Per-IP Limits**: 100 requests per hour by default
- **File Size Limits**: 10MB maximum per image
- **Batch Limits**: 20 images maximum per batch job
- **Processing Timeouts**: 5 minutes per image

### Storage Management

- **Retention**: Files automatically deleted after 24 hours
- **Session Tracking**: UUID-based sessions for file organization
- **Cleanup**: Periodic background cleanup every 6 hours
- **Security**: Session-based access control for downloads

## Monitoring & Health

### Health Checks

The service provides comprehensive health monitoring:

- **Application Status**: Overall service health
- **External Dependencies**: OlmOCR API connectivity
- **System Resources**: Memory, disk, CPU usage
- **Storage**: File system health

### Logging

- **Request Logging**: All API requests with timing
- **Error Logging**: Detailed error information
- **Performance Metrics**: Processing times and success rates
- **System Events**: Startup, shutdown, cleanup events

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio httpx

# Run tests
pytest
```

### Code Quality

```bash
# Format code
black app/
isort app/

# Lint code
flake8 app/
```

### Local Development with Docker

```bash
# Build image
docker build -t olmocr-backend .

# Run container
docker run -p 8000:8000 --env-file .env olmocr-backend
```

## Deployment

### Railway Deployment

1. **Railway Configuration**: Uses `railway.json` for deployment settings
2. **Environment Variables**: Configure via Railway dashboard or CLI
3. **Dockerfile**: Multi-stage build optimized for production
4. **Health Checks**: Built-in health monitoring for Railway

### Docker Deployment

```bash
# Build and run
docker build -t olmocr-backend .
docker run -d -p 8000:8000 --env-file .env olmocr-backend
```

### Environment-Specific Configs

- **Development**: `.env` file with debug enabled
- **Production**: Railway environment variables
- **Docker**: Environment variables or `.env` file mounting

## API Documentation

Once running, access interactive API documentation:

- **Swagger UI**: `/docs`
- **ReDoc**: `/redoc`
- **OpenAPI JSON**: `/openapi.json`

## Security

- **Input Validation**: Comprehensive validation of all inputs
- **Rate Limiting**: Protection against abuse
- **File Security**: Validation of uploaded files
- **Session Security**: UUID-based session management
- **Data Privacy**: Automatic cleanup ensures no data persistence

## Performance

- **Async Processing**: Non-blocking I/O operations
- **Background Tasks**: Batch processing doesn't block API
- **Connection Pooling**: Efficient external API usage
- **Resource Management**: Automatic cleanup and monitoring

## Troubleshooting

### Common Issues

1. **OlmOCR API Errors**: Check API key and network connectivity
2. **File Upload Issues**: Verify file size and format
3. **Memory Issues**: Monitor system resources via health endpoint
4. **Rate Limiting**: Implement backoff in client applications

### Debug Mode

Enable debug mode for detailed error information:

```bash
export DEBUG=true
```

### Logs

Check application logs for detailed error information:

```bash
# Railway logs
railway logs

# Docker logs
docker logs <container_id>
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:

1. Check the health endpoint: `/api/v1/health`
2. Review application logs
3. Verify environment configuration
4. Test with single image conversion first

---

**Note**: This service is designed for temporary file processing. All uploaded and generated files are automatically deleted after 24 hours. For permanent storage, download files immediately after processing.