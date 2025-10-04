# Start OlmOCR Backend Server
Write-Host "Starting OlmOCR Backend Server..." -ForegroundColor Green
Write-Host ""

# Change to script directory
Set-Location $PSScriptRoot

# Activate virtual environment
& .\ven\Scripts\Activate.ps1

# Start server on port 8001
Write-Host "Server starting on http://localhost:8001" -ForegroundColor Cyan
Write-Host "API Docs: http://localhost:8001/docs" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
Write-Host ""

# Use python -m to avoid shebang issues
python -m uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
