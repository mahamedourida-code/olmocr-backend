# Railway Deployment Guide

This guide will help you deploy the OlmOCR Backend to Railway.

## Prerequisites

- Railway account (sign up at https://railway.app)
- Your project code pushed to a Git repository (GitHub, GitLab, or Bitbucket)

## Deployment Architecture

Your deployment will consist of 3 services:

1. **Redis** - Message broker and cache
2. **Web Service** - FastAPI application
3. **Worker Service** - Celery workers for background processing

## Step-by-Step Deployment

### 1. Create a New Project on Railway

1. Go to https://railway.app
2. Click "New Project"
3. Select "Deploy from GitHub repo"
4. Choose your repository

### 2. Deploy Redis Service

1. Click "+ New" in your Railway project
2. Select "Database" → "Add Redis"
3. Railway will automatically provision a Redis instance
4. Note the `REDIS_URL` environment variable (automatically created)

### 3. Deploy Web Service (FastAPI)

1. Click "+ New" → "GitHub Repo"
2. Select your repository
3. Configure the service:
   - **Name**: `olmocr-web`
   - **Start Command**: Automatically detected from Dockerfile
   - **Variables** (click "Variables" tab):
     ```
     SERVICE_TYPE=web
     OLMOCR_API_KEY=your_api_key_here
     OLMOCR_BASE_URL=https://api.deepinfra.com/v1/openai
     OLMOCR_MODEL=allenai/olmOCR-7B-0725-FP8
     REDIS_URL=${{Redis.REDIS_URL}}
     ENVIRONMENT=production
     DEBUG=false
     MAX_FILE_SIZE_MB=10
     MAX_BATCH_SIZE=10
     FILE_RETENTION_HOURS=24
     ALLOWED_ORIGINS=https://your-frontend-domain.com
     SUPABASE_URL=your_supabase_url
     SUPABASE_ANON_KEY=your_supabase_anon_key
     SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key
     SUPABASE_STORAGE_BUCKET=jobs
     ```
4. Click "Deploy"
5. Once deployed, click "Settings" → "Networking" → "Generate Domain" to get your public URL

### 4. Deploy Worker Service (Celery)

1. Click "+ New" → "GitHub Repo"
2. Select the same repository
3. Configure the service:
   - **Name**: `olmocr-worker`
   - **Start Command**: Automatically detected from Dockerfile
   - **Variables**:
     ```
     SERVICE_TYPE=worker
     OLMOCR_API_KEY=your_api_key_here
     OLMOCR_BASE_URL=https://api.deepinfra.com/v1/openai
     OLMOCR_MODEL=allenai/olmOCR-7B-0725-FP8
     REDIS_URL=${{Redis.REDIS_URL}}
     WORKER_CONCURRENCY=4
     WORKER_QUEUES=image_processing,batch_processing,default
     ENVIRONMENT=production
     DEBUG=false
     SUPABASE_URL=your_supabase_url
     SUPABASE_ANON_KEY=your_supabase_anon_key
     SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key
     SUPABASE_STORAGE_BUCKET=jobs
     ```
4. Click "Deploy"

### 5. (Optional) Deploy Flower Monitoring

1. Click "+ New" → "GitHub Repo"
2. Select the same repository
3. Configure the service:
   - **Name**: `olmocr-flower`
   - **Start Command**: Automatically detected from Dockerfile
   - **Variables**:
     ```
     SERVICE_TYPE=flower
     REDIS_URL=${{Redis.REDIS_URL}}
     PORT=5555
     ```
4. Generate a domain for Flower UI access

## Environment Variables Reference

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `SERVICE_TYPE` | Service type (web/worker/flower) | `web` |
| `OLMOCR_API_KEY` | OlmOCR API key | `your_api_key` |
| `OLMOCR_BASE_URL` | OlmOCR API base URL | `https://api.deepinfra.com/v1/openai` |
| `OLMOCR_MODEL` | OlmOCR model name | `allenai/olmOCR-7B-0725-FP8` |
| `REDIS_URL` | Redis connection URL | `redis://...` |
| `SUPABASE_URL` | Supabase project URL | `https://xxx.supabase.co` |
| `SUPABASE_ANON_KEY` | Supabase anonymous key | `eyJ...` |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key | `eyJ...` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ENVIRONMENT` | Environment name | `production` |
| `DEBUG` | Debug mode | `false` |
| `MAX_FILE_SIZE_MB` | Max file size in MB | `10` |
| `MAX_BATCH_SIZE` | Max batch size | `10` |
| `FILE_RETENTION_HOURS` | File retention hours | `24` |
| `WORKER_CONCURRENCY` | Worker concurrency | `4` |
| `ALLOWED_ORIGINS` | CORS allowed origins | `*` |
| `SUPABASE_STORAGE_BUCKET` | Supabase storage bucket | `jobs` |

## Verifying Deployment

### 1. Check Web Service

Visit your web service domain:
```
https://your-app.railway.app/docs
```

You should see the FastAPI Swagger documentation.

### 2. Check Health Endpoint

```bash
curl https://your-app.railway.app/api/v1/health
```

Expected response:
```json
{
  "status": "healthy",
  "service": "olmocr-backend",
  "timestamp": "2025-01-XX..."
}
```

### 3. Check Worker Service

View logs in Railway dashboard to ensure workers are running:
```
[INFO] Starting service type: worker
[INFO] Executing command: celery -A app.tasks.celery_app worker...
```

### 4. Check Redis Connection

Both web and worker services should connect to Redis successfully. Check logs for:
```
Connected to Redis at redis://...
```

## Monitoring

### Railway Dashboard

- View real-time logs for each service
- Monitor resource usage (CPU, Memory)
- Check deployment history

### Flower Dashboard (if deployed)

Visit your Flower service domain to monitor Celery workers:
```
https://your-flower.railway.app
```

Default credentials: `admin` / `flower123` (change this in production!)

## Troubleshooting

### Service Won't Start

1. Check environment variables are set correctly
2. View deployment logs in Railway dashboard
3. Ensure Redis is running and accessible

### Worker Not Processing Tasks

1. Check worker logs for errors
2. Verify Redis connection
3. Ensure `REDIS_URL` is set correctly in both web and worker services

### Health Check Failing

1. Ensure web service is running on correct PORT
2. Check if `/api/v1/health` endpoint is accessible
3. Increase health check timeout in railway.json if needed

### CORS Errors

1. Update `ALLOWED_ORIGINS` environment variable
2. Add your frontend domain to the allowed origins list
3. Redeploy the web service

## Scaling

### Horizontal Scaling

To handle more traffic:

1. Go to service settings in Railway
2. Increase number of replicas (Web Service)
3. Increase worker concurrency or add more worker instances

### Vertical Scaling

Railway automatically scales resources based on usage. You can also manually adjust:

1. Service Settings → Resources
2. Adjust CPU and Memory limits

## Cost Optimization

- Use Railway's sleep mode for non-production environments
- Set appropriate `FILE_RETENTION_HOURS` to clean up old files
- Monitor resource usage and adjust worker concurrency
- Consider using Railway's usage alerts

## Next Steps

1. Set up custom domain for your API
2. Configure SSL/TLS (automatic with Railway)
3. Set up monitoring and alerts
4. Configure backup strategy for Redis data
5. Implement CI/CD pipeline for automatic deployments

## Support

- Railway Docs: https://docs.railway.app
- Railway Discord: https://discord.gg/railway
- GitHub Issues: Your repository issues page
