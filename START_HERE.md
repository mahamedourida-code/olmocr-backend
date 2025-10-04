# 🚀 START HERE - Quick Server Start

## The Error You're Getting

When you run:
```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

You get:
```
Fatal error in launcher: Unable to create process...
```

## ✅ THE FIX - Copy and Paste This Command

Open PowerShell in the project folder and run:

```powershell
python -m uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

**Notice:** Use `python -m uvicorn` instead of just `uvicorn`

## Why This Works

- `uvicorn` (exe) → ❌ Has wrong hardcoded path
- `python -m uvicorn` (module) → ✅ Always uses correct Python

## Even Easier - Double Click This

1. Find `START_SERVER.bat` in the project folder
2. **Double-click it**
3. Done! Server starts automatically

## Verify It's Working

You should see:
```
INFO:     Uvicorn running on http://0.0.0.0:8001 (Press CTRL+C to quit)
INFO:     Started reloader process
INFO:     Started server process
INFO:     Application startup complete
```

Then visit: http://localhost:8001/docs

## Summary

**DON'T USE:**
```powershell
uvicorn app.main:app ...  ❌
```

**DO USE:**
```powershell
python -m uvicorn app.main:app ...  ✅
```

**OR EASIEST:**
```powershell
# Just double-click START_SERVER.bat
```

---

**Current Status:**
- Port 8000: Already running (my test server)
- Port 8001: Available for you to use

You can use either port, they both work!
