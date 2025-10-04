# OlmOCR Backend - MVP Ready ✅

## 🎯 What's Ready

A production-ready FastAPI backend for converting table images to XLSX files with **real-time WebSocket updates**.

## 🚀 Quick Start

### 1. Start Redis
```bash
redis-server
# Or on Windows: "C:\Program Files\Redis\redis-server.exe"
```

### 2. Start Backend
```bash
cd "olmocr backend"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Test WebSocket
Open `test_websocket.html` in your browser and upload table images.

## 📡 API Endpoints

### Main Endpoints
- **POST** `/api/v1/convert/batch` - Process images
- **GET** `/api/v1/download/{job_id}` - Download XLSX
- **WS** `/api/v1/ws/session/{session_id}` - Real-time updates

### Documentation
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## 🔌 WebSocket Integration (Simplified for MVP)

### Frontend Example
```javascript
// 1. Start processing
const response = await fetch('http://localhost:8000/api/v1/convert/batch', {
  method: 'POST',
  credentials: 'include',
  body: JSON.stringify({
    images: [{image: 'base64...', filename: 'table.png'}]
  })
});

const {job_id, session_id} = await response.json();

// 2. Connect WebSocket (ONLY ONE ENDPOINT NEEDED!)
const ws = new WebSocket(`ws://localhost:8000/api/v1/ws/session/${session_id}`);

// 3. Handle updates
ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);

  if (msg.type === 'job_progress') {
    console.log(`Progress: ${msg.progress}%`);
  }
  else if (msg.type === 'job_completed') {
    console.log('Download:', msg.primary_download_url);
    window.location.href = `http://localhost:8000${msg.primary_download_url}`;
  }
};
```

## 📊 Message Types

### Progress Update
```json
{
  "type": "job_progress",
  "progress": 50,
  "processed_images": 2,
  "total_images": 5,
  "current_image": "table.png"
}
```

### Completion
```json
{
  "type": "job_completed",
  "status": "completed",
  "primary_download_url": "/api/v1/download/uuid",
  "processing_time": 12.5
}
```

### Error
```json
{
  "type": "job_error",
  "error": "Processing failed"
}
```

## 🏗️ Project Structure

```
olmocr backend/
├── app/
│   ├── main.py                 # FastAPI app
│   ├── api/v1/
│   │   ├── convert.py          # Image processing endpoints
│   │   ├── websocket.py        # WebSocket endpoint (MVP: 1 endpoint only!)
│   │   ├── download.py         # File download
│   │   └── jobs.py             # Job status
│   ├── services/
│   │   ├── olmocr.py           # OlmOCR API client
│   │   ├── excel.py            # XLSX generation
│   │   ├── redis_service.py    # Redis & pub/sub
│   │   └── websocket_service.py # WebSocket manager
│   └── models/
│       ├── websocket.py        # WebSocket message types
│       └── ...
├── test_websocket.html         # Test page
├── WEBSOCKET_GUIDE.md          # Integration guide
├── TESTING_SUMMARY.md          # Test results
└── .env                        # Configuration
```

## ⚙️ Configuration (.env)

Required:
```env
OLMOCR_API_KEY=your_key_here
REDIS_URL=redis://localhost:6379/0
```

Optional:
```env
PORT=8000
MAX_FILE_SIZE_MB=10
FILE_RETENTION_HOURS=24
```

## 🎨 Frontend Integration

See `WEBSOCKET_GUIDE.md` for:
- Complete React example
- Vanilla JavaScript example
- Error handling
- Reconnection logic

## 🧪 Testing

### Option 1: HTML Test Page
1. Open `test_websocket.html`
2. Select table images
3. Click "Process Images"
4. Watch real-time updates

### Option 2: API Testing
```bash
# Start a job
curl -X POST http://localhost:8000/api/v1/convert/batch \
  -H "Content-Type: application/json" \
  -d '{"images":[{"image":"base64data","filename":"test.png"}]}'

# Connect WebSocket (in browser console)
const ws = new WebSocket('ws://localhost:8000/api/v1/ws/session/SESSION_ID');
ws.onmessage = (e) => console.log(JSON.parse(e.data));
```

## 🔧 MVP Simplifications

**What was removed for MVP:**
- ❌ `/ws/connect` endpoint (manual subscription)
- ❌ `/ws/jobs/{job_id}` endpoint (job-specific)
- ❌ `/ws/stats` endpoint (admin stats)
- ❌ `/ws/broadcast` endpoint (admin broadcast)
- ❌ Celery workers (using background tasks instead)

**What's kept (all you need):**
- ✅ `/ws/session/{session_id}` - Auto-subscribes to all relevant topics
- ✅ Real-time progress updates
- ✅ Download links on completion
- ✅ Error reporting

## 🚢 Deployment (Railway)

### Environment Variables
```env
OLMOCR_API_KEY=your_key_here
REDIS_URL=${{Redis.REDIS_URL}}  # Provided by Railway
PORT=${{PORT}}                   # Provided by Railway
```

### Railway Setup
1. Add Railway Redis service
2. Connect to GitHub repo
3. Deploy automatically
4. WebSocket works through Railway's proxy

## ✨ Features

- ✅ Real-time WebSocket updates
- ✅ Batch image processing
- ✅ Automatic session management
- ✅ File cleanup & expiration
- ✅ CORS configured
- ✅ Error handling
- ✅ Rate limiting ready
- ✅ Redis pub/sub integration
- ✅ Simple single-endpoint design

## 📝 Files Reference

- `WEBSOCKET_GUIDE.md` - Frontend integration guide
- `TESTING_SUMMARY.md` - Testing results and architecture
- `test_websocket.html` - Working test page
- `CLAUDE` - Project architecture and instructions

## 🎉 Ready for Production

The backend is **MVP-ready** and can be:
1. Integrated with your frontend immediately
2. Deployed to Railway without changes
3. Scaled horizontally with Redis
4. Extended with more features later

## 📞 Support

For issues or questions:
- Check logs: Server console shows detailed logging
- Test page: Use `test_websocket.html` to debug
- API docs: Visit `/docs` for interactive testing
- WebSocket: Browser DevTools → Network → WS tab

---

**Status:** ✅ MVP Complete - Ready for Frontend Integration
