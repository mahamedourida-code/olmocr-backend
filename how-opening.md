

# Activate virtual environment
ven\Scripts\activate
Start-Process redis-server -WindowStyle Hidden


```
```


### 2. Start FastAPI Server

Open **Terminal 2** (PowerShell):
```powershell

ven\Scripts\activate

$env:OLMOCR_API_KEY="IJfGf6mJLRblDzWqR4xzjooGSoH596Gf"


uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```




### 3. Start Celery Worker (REQUIRED FOR BATCH PROCESSING)

Open **Terminal 3** (PowerShell):
```powershell


ven\Scripts\activate
$env:OLMOCR_API_KEY="IJfGf6mJLRblDzWqR4xzjooGSoH596Gf"
celery -A app.tasks.batch_tasks worker --loglevel=info --pool=solo
```



Job-Specific Monitoring (MOST USEFUL FOR PROGRESS)
Endpoint: ws://localhost:8003/api/v1/ws/jobs/{job_id}
Setup in Insomnia:
URL: ws://localhost:8003/api/v1/ws/jobs/your-actual-job-id?session_id=your-session-id
No body needed - automatically subscribes to job updates
Replace your-actual-job-id with real job ID from batch creation

General Connection (Flexible)
Endpoint: ws://localhost:8003/api/v1/ws/connect
Setup in Insomnia:
URL: ws://localhost:8003/api/v1/ws/connect?session_id=your-session-id

Session-Wide Monitoring
Endpoint: ws://localhost:8003/api/v1/ws/session/{session_id}
Setup in Insomnia:
URL: ws://localhost:8003/api/v1/ws/session/your-session-id