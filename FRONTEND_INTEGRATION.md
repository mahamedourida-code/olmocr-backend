# Frontend Integration Guide

This document outlines how to integrate your frontend with the FastAPI backend's Supabase features.

## Prerequisites

✅ You already have:
- Supabase Auth configured in your frontend
- User authentication working
- JWT tokens from Supabase

## What's New in the Backend

The backend now supports **both authenticated and unauthenticated requests**:

### Authenticated Requests (Recommended)
- Jobs are stored in Supabase database
- Files are saved to Supabase Storage
- Users can access their job history
- Persistent data across sessions

### Unauthenticated Requests (Legacy)
- Jobs are stored in Redis only (temporary)
- Files are saved locally
- Session-based access only

---

## 1. Sending JWT Tokens to Backend

### Get the JWT Token from Supabase

```typescript
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
)

// Get current session
const { data: { session } } = await supabase.auth.getSession()
const accessToken = session?.access_token
```

### Send Token in API Requests

**Option 1: Using Fetch**
```typescript
const response = await fetch('http://localhost:8000/api/v1/convert/batch', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${accessToken}`, // ✅ Add this header
  },
  body: JSON.stringify({
    images: [/* your images */],
    consolidation_strategy: 'separate_sheets',
    output_format: 'xlsx'
  })
})
```

**Option 2: Using Axios**
```typescript
import axios from 'axios'

const api = axios.create({
  baseURL: 'http://localhost:8000/api/v1',
})

// Add interceptor to attach token
api.interceptors.request.use(async (config) => {
  const { data: { session } } = await supabase.auth.getSession()
  if (session?.access_token) {
    config.headers.Authorization = `Bearer ${session.access_token}`
  }
  return config
})

// Make requests
const response = await api.post('/convert/batch', {
  images: [/* your images */],
  consolidation_strategy: 'separate_sheets',
  output_format: 'xlsx'
})
```

---

## 2. API Endpoints

### Convert Images (Batch Processing)

**POST** `/api/v1/convert/batch`

**Headers:**
```
Authorization: Bearer <supabase_jwt_token>  // Optional but recommended
Content-Type: application/json
```

**Request Body:**
```json
{
  "images": [
    {
      "image": "data:image/png;base64,iVBORw0KG...",
      "filename": "invoice1.png"
    },
    {
      "image": "data:image/jpeg;base64,/9j/4AAQ...",
      "filename": "invoice2.jpg"
    }
  ],
  "consolidation_strategy": "separate_sheets",
  "output_format": "xlsx"
}
```

**Response:**
```json
{
  "success": true,
  "job_id": "9e15b5bb-8ff5-43d5-af57-140a2c8447e6",
  "download_url": "/api/v1/download/9e15b5bb-8ff5-43d5-af57-140a2c8447e6",
  "expires_at": "2025-10-05T12:00:00Z",
  "processing_time": 0.0,
  "session_id": "626df28a-8c3b-4dac-a6bd-2bd6b4212f96"
}
```

---

## 3. Accessing Results

### Authenticated Users

When authenticated, files are stored in Supabase Storage and the database.

#### Get Job from Supabase Database

```typescript
// Get job details from processing_jobs table
const { data: job } = await supabase
  .from('processing_jobs')
  .select('*')
  .eq('id', jobId)
  .single()

console.log(job.status)       // 'completed', 'processing', 'failed'
console.log(job.result_url)   // Public URL to XLSX in Supabase Storage
```

#### Get User's Job History

```typescript
// Get all jobs for current user
const { data: { user } } = await supabase.auth.getUser()

const { data: jobs } = await supabase
  .from('processing_jobs')
  .select('*')
  .eq('user_id', user.id)
  .order('created_at', { ascending: false })
  .limit(50)
```

#### Download File from Supabase Storage

**Option 1: Use the public URL from database**
```typescript
const { data: job } = await supabase
  .from('processing_jobs')
  .select('result_url')
  .eq('id', jobId)
  .single()

// Download file
window.open(job.result_url, '_blank')
```

**Option 2: Fetch from Storage directly**
```typescript
const { data } = await supabase.storage
  .from('jobs')
  .download(`${userId}/${jobId}/result.xlsx`)

// Create download link
const url = URL.createObjectURL(data)
const a = document.createElement('a')
a.href = url
a.download = 'result.xlsx'
a.click()
```

### Unauthenticated Users (Session-based)

Use the download URL from the job creation response:

```typescript
const response = await fetch('http://localhost:8000/api/v1/convert/batch', {
  method: 'POST',
  // No Authorization header
  body: JSON.stringify({ /* ... */ })
})

const { download_url, session_id } = await response.json()

// Download file (include session cookie)
window.open(`http://localhost:8000${download_url}`, '_blank')
```

---

## 4. WebSocket Real-time Updates

Connect to WebSocket for real-time job progress updates:

```typescript
const ws = new WebSocket(
  `ws://localhost:8000/api/v1/ws/${sessionId}?session_id=${sessionId}`
)

ws.onmessage = (event) => {
  const message = JSON.parse(event.data)

  switch (message.type) {
    case 'job_progress':
      console.log(`Progress: ${message.progress}%`)
      console.log(`Processing: ${message.current_image}`)
      break

    case 'job_completed':
      console.log('Job completed!')
      console.log('Download URLs:', message.download_urls)
      console.log('Primary URL:', message.primary_download_url)
      break

    case 'job_error':
      console.error('Job failed:', message.error)
      break
  }
}

// Send subscription message
ws.onopen = () => {
  ws.send(JSON.stringify({
    action: 'subscribe',
    job_id: jobId  // Subscribe to specific job updates
  }))
}
```

---

## 5. Complete Integration Example

### React/Next.js Component

```typescript
'use client'

import { useState, useEffect } from 'react'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
)

export default function ImageProcessor() {
  const [session, setSession] = useState(null)
  const [jobId, setJobId] = useState<string | null>(null)
  const [progress, setProgress] = useState(0)
  const [resultUrl, setResultUrl] = useState<string | null>(null)

  useEffect(() => {
    // Get session
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session)
    })

    // Listen for auth changes
    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      (_event, session) => {
        setSession(session)
      }
    )

    return () => subscription.unsubscribe()
  }, [])

  const processImages = async (images: File[]) => {
    // Convert images to base64
    const base64Images = await Promise.all(
      images.map(async (file) => ({
        image: await fileToBase64(file),
        filename: file.name
      }))
    )

    // Get access token if authenticated
    const { data: { session } } = await supabase.auth.getSession()
    const headers: HeadersInit = {
      'Content-Type': 'application/json',
    }

    if (session?.access_token) {
      headers.Authorization = `Bearer ${session.access_token}`
    }

    // Send to backend
    const response = await fetch('http://localhost:8000/api/v1/convert/batch', {
      method: 'POST',
      headers,
      body: JSON.stringify({
        images: base64Images,
        consolidation_strategy: 'separate_sheets',
        output_format: 'xlsx'
      })
    })

    const data = await response.json()
    setJobId(data.job_id)

    // Connect to WebSocket for progress
    const ws = new WebSocket(
      `ws://localhost:8000/api/v1/ws/${data.session_id}?session_id=${data.session_id}`
    )

    ws.onmessage = (event) => {
      const message = JSON.parse(event.data)

      if (message.type === 'job_progress') {
        setProgress(message.progress)
      }

      if (message.type === 'job_completed') {
        setProgress(100)
        // For authenticated users, get from Supabase
        if (session) {
          fetchJobFromSupabase(data.job_id)
        } else {
          setResultUrl(message.primary_download_url)
        }
      }
    }

    ws.onopen = () => {
      ws.send(JSON.stringify({
        action: 'subscribe',
        job_id: data.job_id
      }))
    }

    return data
  }

  const fetchJobFromSupabase = async (jobId: string) => {
    const { data: job } = await supabase
      .from('processing_jobs')
      .select('result_url, status')
      .eq('id', jobId)
      .single()

    if (job?.result_url) {
      setResultUrl(job.result_url)
    }
  }

  return (
    <div>
      <h2>Image Processor</h2>

      {session ? (
        <p>✅ Authenticated as {session.user.email}</p>
      ) : (
        <p>⚠️ Not authenticated (files will be temporary)</p>
      )}

      <input
        type="file"
        multiple
        accept="image/*"
        onChange={(e) => {
          if (e.target.files) {
            processImages(Array.from(e.target.files))
          }
        }}
      />

      {progress > 0 && (
        <div>
          <p>Progress: {progress}%</p>
          <progress value={progress} max={100} />
        </div>
      )}

      {resultUrl && (
        <a href={resultUrl} download>
          Download Result
        </a>
      )}
    </div>
  )
}

// Helper function
async function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.readAsDataURL(file)
    reader.onload = () => resolve(reader.result as string)
    reader.onerror = reject
  })
}
```

---

## 6. Database Schema Reference

### `processing_jobs` Table

```sql
CREATE TABLE processing_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id TEXT NOT NULL,
  status TEXT DEFAULT 'pending',
  image_url TEXT,
  filename TEXT,
  result_url TEXT,
  error_message TEXT,
  processing_metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Fields:**
- `id` - Job UUID (matches job_id from API)
- `user_id` - Supabase Auth user ID
- `status` - `pending`, `processing`, `completed`, `failed`
- `result_url` - Public URL to XLSX in Supabase Storage
- `processing_metadata` - Job details (total_images, processing_time, etc.)

---

## 7. Storage Bucket Structure

Files are stored in the `jobs` bucket with this structure:

```
jobs/
├── {user_id}/
│   ├── {job_id}/
│   │   ├── image1_processed.xlsx
│   │   ├── image2_processed.xlsx
│   │   └── image3_processed.xlsx
```

**Example:**
```
jobs/a1b2c3d4-e5f6-7890-abcd-ef1234567890/9e15b5bb-8ff5-43d5-af57-140a2c8447e6/invoice_processed.xlsx
```

---

## 8. Error Handling

### Backend Errors

```typescript
try {
  const response = await fetch('/api/v1/convert/batch', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data)
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || 'Processing failed')
  }

  const result = await response.json()
  // Handle success
} catch (error) {
  console.error('Error:', error)
  // Show error to user
}
```

### Common Error Codes

| Code | Error | Solution |
|------|-------|----------|
| 401 | Unauthorized | Token expired/invalid - refresh auth |
| 413 | File too large | Reduce image size/quality |
| 400 | Invalid image data | Check base64 encoding |
| 500 | Server error | Retry or contact support |

---

## 9. Environment Variables

### Frontend `.env.local`

```env
# Supabase (you already have these)
NEXT_PUBLIC_SUPABASE_URL=https://iawkqvdtktnvxqgpupvt.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=eyJhbGc...

# Backend API
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000

# For production
# NEXT_PUBLIC_API_URL=https://your-backend.railway.app
# NEXT_PUBLIC_WS_URL=wss://your-backend.railway.app
```

---

## 10. Checklist

### ✅ Already Done (Backend)
- [x] Supabase integration in backend
- [x] JWT verification
- [x] Database operations
- [x] Storage upload
- [x] WebSocket updates

### 📋 Frontend TODO

- [ ] Get JWT token from Supabase Auth
- [ ] Send token in `Authorization: Bearer <token>` header
- [ ] Handle authenticated vs unauthenticated users
- [ ] Fetch job results from Supabase database
- [ ] Download files from Supabase Storage
- [ ] Connect to WebSocket for real-time updates
- [ ] Display job history for authenticated users
- [ ] Handle errors gracefully

---

## 11. Testing

### Test Authentication Flow

```typescript
// 1. Sign in
await supabase.auth.signInWithPassword({
  email: 'test@example.com',
  password: 'password'
})

// 2. Get session
const { data: { session } } = await supabase.auth.getSession()
console.log('Access Token:', session?.access_token)

// 3. Make authenticated request
const response = await fetch('/api/v1/convert/batch', {
  headers: {
    'Authorization': `Bearer ${session?.access_token}`
  },
  // ...
})

// 4. Verify job in database
const { data: job } = await supabase
  .from('processing_jobs')
  .select('*')
  .eq('id', jobId)
  .single()

console.log('Job created:', job)
```

---

## Support

If you encounter issues:
1. Check browser console for errors
2. Verify JWT token is valid
3. Ensure Supabase Storage bucket `jobs` exists
4. Check backend logs for detailed errors
5. Test with unauthenticated request to isolate auth issues

---

## Summary

**What changed:**
- Backend now supports authenticated requests via Supabase JWT
- Jobs are saved to Supabase database
- Files are uploaded to Supabase Storage
- Existing session-based flow still works for unauthenticated users

**What you need to do:**
1. Send `Authorization: Bearer <token>` header with requests
2. Fetch results from Supabase database using `processing_jobs` table
3. Download files from Supabase Storage using `result_url`
4. Everything else stays the same!

---

## AI Agent Prompt for Frontend Implementation

Use this prompt with your frontend AI assistant to implement the backend integration:

```
I need you to integrate our Next.js frontend with a FastAPI backend for OCR image processing. The backend supports both authenticated (via Supabase JWT) and unauthenticated requests.

## Backend API Details

**Base URL:**
- Development: `http://localhost:8000`
- Production: `https://your-backend.railway.app` (update when deployed)

**WebSocket URL:**
- Development: `ws://localhost:8000`
- Production: `wss://your-backend.railway.app` (update when deployed)

## Required Implementation

### 1. API Client Setup

Create an API client (`lib/api-client.ts`) that:
- Automatically attaches Supabase JWT token to requests when user is authenticated
- Uses Axios with interceptors for token management
- Handles both authenticated and unauthenticated states
- Provides typed request/response interfaces

**Base configuration:**
```typescript
import axios from 'axios'
import { createClient } from '@supabase/supabase-js'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
```

### 2. Main Features to Implement

#### A. Image Upload & Processing Component
Create a component that:
- Accepts multiple image uploads (drag-drop and file picker)
- Converts images to base64 format
- Sends images to `POST /api/v1/convert/batch` endpoint with JWT token (if authenticated)
- Handles consolidation strategies: "separate_sheets", "single_workbook", "combined_table"
- Shows authentication status indicator

**Request payload structure:**
```json
{
  "images": [
    {
      "image": "data:image/png;base64,...",
      "filename": "invoice1.png"
    }
  ],
  "consolidation_strategy": "separate_sheets",
  "output_format": "xlsx"
}
```

#### B. WebSocket Progress Tracking
Implement real-time job progress updates:
- Connect to WebSocket endpoint: `ws://localhost:8000/api/v1/ws/{session_id}?session_id={session_id}`
- Subscribe to job updates by sending: `{action: 'subscribe', job_id: 'xxx'}`
- Handle message types: `job_progress`, `job_completed`, `job_error`
- Display progress bar and current processing status
- Update UI when job completes

#### C. Job History (Authenticated Users Only)
Create a job history page that:
- Fetches jobs from Supabase `processing_jobs` table
- Displays: job ID, status, created_at, file download link
- Filters by status (pending, processing, completed, failed)
- Shows pagination for large result sets
- Allows re-downloading previous results

**Database query:**
```typescript
const { data: jobs } = await supabase
  .from('processing_jobs')
  .select('*')
  .eq('user_id', user.id)
  .order('created_at', { ascending: false })
```

#### D. File Download Handler
Implement download functionality that:
- For authenticated users: Uses `result_url` from Supabase database
- For unauthenticated: Uses `download_url` from API response
- Handles both Supabase Storage URLs and backend endpoints
- Shows download progress indicator
- Handles download errors gracefully

### 3. Environment Variables Needed

Add to `.env.local`:
```env
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000
NEXT_PUBLIC_SUPABASE_URL=https://iawkqvdtktnvxqgpupvt.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=eyJhbGc...
```

### 4. TypeScript Interfaces Required

Define types for:
- `ImageInput` - image data and filename
- `BatchConvertRequest` - API request payload
- `BatchConvertResponse` - API response with job_id, download_url, session_id
- `JobStatus` - 'pending' | 'processing' | 'completed' | 'failed'
- `ProcessingJob` - matches Supabase database schema
- `WebSocketMessage` - typed WebSocket event messages

### 5. Error Handling

Implement error handling for:
- 401 Unauthorized - token expired/invalid (trigger re-authentication)
- 413 File too large - show user-friendly message
- 400 Invalid image data - validate before sending
- 500 Server error - retry logic with exponential backoff
- WebSocket connection failures - automatic reconnection

### 6. UI/UX Requirements

- Show clear authentication status indicator
- Display different UI for authenticated vs unauthenticated users
- Warning for unauthenticated users that files are temporary
- Real-time progress updates during processing
- Success notification when job completes
- Error messages with actionable solutions
- Loading states for all async operations

### 7. File Structure

Create these files:
- `lib/api-client.ts` - Axios instance with interceptors
- `lib/websocket.ts` - WebSocket connection manager
- `types/api.ts` - TypeScript interfaces
- `components/ImageUploader.tsx` - Upload and processing component
- `components/JobProgress.tsx` - Progress tracking component
- `components/JobHistory.tsx` - Job history table
- `app/convert/page.tsx` - Main conversion page
- `app/history/page.tsx` - Job history page

### 8. Additional Considerations

- Implement file size validation before upload (max 10MB per image recommended)
- Add image preview before processing
- Support canceling in-progress jobs
- Cache job results to avoid redundant API calls
- Add analytics tracking for conversion events
- Implement rate limiting warnings
- Add keyboard shortcuts for power users
- Support both light and dark themes

## Testing Checklist

After implementation, test:
- [ ] Upload and process images while signed in
- [ ] Upload and process images while signed out
- [ ] Real-time progress updates via WebSocket
- [ ] Download files from completed jobs
- [ ] View job history for authenticated users
- [ ] Token refresh when expired
- [ ] Error handling for all error codes
- [ ] File validation and size limits
- [ ] WebSocket reconnection on disconnect
- [ ] Mobile responsive design

## Database Schema Reference

The backend stores jobs in this table structure:
```sql
CREATE TABLE processing_jobs (
  id UUID PRIMARY KEY,
  user_id TEXT NOT NULL,
  status TEXT DEFAULT 'pending',
  image_url TEXT,
  filename TEXT,
  result_url TEXT,
  error_message TEXT,
  processing_metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Example API Flow

1. User uploads images → Convert to base64
2. Send POST to `/api/v1/convert/batch` with JWT token
3. Receive job_id and session_id
4. Connect to WebSocket with session_id
5. Subscribe to job_id updates
6. Show real-time progress
7. On completion, fetch from Supabase (if authenticated) or use download_url (if not)
8. Download XLSX file

Please implement all of the above features following Next.js 14+ best practices with TypeScript, using Tailwind CSS for styling and shadcn/ui components where appropriate.
```

---

Copy the above prompt and use it with your frontend AI assistant to implement the complete backend integration.
