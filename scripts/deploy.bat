@echo off
REM Quick deployment script for Windows
REM This script helps you deploy the backend to Fly.io

echo ========================================
echo   OlmOCR Backend Deployment Script
echo ========================================
echo.

REM Check if flyctl is installed
where flyctl >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Fly CLI not found!
    echo.
    echo Please install Fly CLI first:
    echo   1. Go to: https://fly.io/docs/hands-on/install-flyctl/
    echo   2. Download and install for Windows
    echo   3. Run: flyctl auth login
    echo.
    pause
    exit /b 1
)

echo [1/5] Checking Fly CLI authentication...
flyctl auth whoami >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Not logged in to Fly.io
    echo Please login first:
    flyctl auth login
    if %ERRORLEVEL% NEQ 0 (
        echo Login failed!
        pause
        exit /b 1
    )
)
echo ✓ Authenticated

echo.
echo [2/5] Checking current app status...
flyctl status -a olmocr-backend-vl616a
if %ERRORLEVEL% NEQ 0 (
    echo Warning: Could not get app status
    echo The app might not exist yet
    pause
)

echo.
echo ========================================
echo   IMPORTANT: Set JWT Secret First!
echo ========================================
echo.
echo Have you set the SUPABASE_JWT_SECRET in Fly.io?
echo.
echo If NOT, run this command first:
echo   flyctl secrets set SUPABASE_JWT_SECRET="your-secret-here"
echo.
echo See scripts/get_jwt_secret.md for instructions on getting your JWT secret
echo.
set /p CONFIRM="Have you set the JWT secret? (y/n): "
if /I not "%CONFIRM%"=="y" (
    echo.
    echo Please set the JWT secret first, then run this script again.
    echo.
    pause
    exit /b 1
)

echo.
echo [3/5] Checking secrets...
flyctl secrets list -a olmocr-backend-vl616a

echo.
echo [4/5] Starting deployment...
echo This will build and deploy your backend to Fly.io
echo.
set /p DEPLOY="Continue with deployment? (y/n): "
if /I not "%DEPLOY%"=="y" (
    echo Deployment cancelled.
    pause
    exit /b 0
)

echo.
echo Deploying...
flyctl deploy
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ========================================
    echo   Deployment Failed!
    echo ========================================
    echo.
    echo Check the error messages above
    echo Common issues:
    echo   - Build errors: Check your code for syntax errors
    echo   - Redis connection: Verify Redis add-on is attached
    echo   - Secrets not set: Run 'flyctl secrets list' to verify
    echo.
    pause
    exit /b 1
)

echo.
echo ========================================
echo   Deployment Successful!
echo ========================================

echo.
echo [5/5] Verifying deployment...
echo.
echo Testing health endpoint...
curl -s https://olmocr-backend-vl616a.fly.dev/api/v1/health
echo.
echo.

echo Checking application status...
flyctl status -a olmocr-backend-vl616a

echo.
echo ========================================
echo   Next Steps:
echo ========================================
echo   1. Test your API: https://olmocr-backend-vl616a.fly.dev/docs
echo   2. Check logs: flyctl logs -a olmocr-backend-vl616a
echo   3. Monitor app: https://fly.io/dashboard/olmocr-backend-vl616a
echo.
echo   To test JWT verification:
echo   - Get a token from your frontend (login first)
echo   - Make an API request with the token
echo   - Check logs for "JWT signature verified successfully"
echo.
echo ========================================

pause
