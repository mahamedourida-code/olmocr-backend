#!/bin/bash
# Quick deployment script for Linux/Mac
# This script helps you deploy the backend to Fly.io

set -e  # Exit on error

echo "========================================"
echo "  OlmOCR Backend Deployment Script"
echo "========================================"
echo ""

# Check if flyctl is installed
if ! command -v flyctl &> /dev/null; then
    echo "ERROR: Fly CLI not found!"
    echo ""
    echo "Please install Fly CLI first:"
    echo "  curl -L https://fly.io/install.sh | sh"
    echo ""
    echo "Then run: flyctl auth login"
    echo ""
    exit 1
fi

echo "[1/5] Checking Fly CLI authentication..."
if ! flyctl auth whoami &> /dev/null; then
    echo "Not logged in to Fly.io"
    echo "Please login first:"
    flyctl auth login
fi
echo "✓ Authenticated"

echo ""
echo "[2/5] Checking current app status..."
flyctl status -a olmocr-backend-vl616a || echo "Warning: Could not get app status"

echo ""
echo "========================================"
echo "  IMPORTANT: Set JWT Secret First!"
echo "========================================"
echo ""
echo "Have you set the SUPABASE_JWT_SECRET in Fly.io?"
echo ""
echo "If NOT, run this command first:"
echo "  flyctl secrets set SUPABASE_JWT_SECRET=\"your-secret-here\""
echo ""
echo "See scripts/get_jwt_secret.md for instructions"
echo ""
read -p "Have you set the JWT secret? (y/n): " CONFIRM
if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
    echo ""
    echo "Please set the JWT secret first, then run this script again."
    echo ""
    exit 1
fi

echo ""
echo "[3/5] Checking secrets..."
flyctl secrets list -a olmocr-backend-vl616a

echo ""
echo "[4/5] Starting deployment..."
echo "This will build and deploy your backend to Fly.io"
echo ""
read -p "Continue with deployment? (y/n): " DEPLOY
if [ "$DEPLOY" != "y" ] && [ "$DEPLOY" != "Y" ]; then
    echo "Deployment cancelled."
    exit 0
fi

echo ""
echo "Deploying..."
if ! flyctl deploy; then
    echo ""
    echo "========================================"
    echo "  Deployment Failed!"
    echo "========================================"
    echo ""
    echo "Check the error messages above"
    echo "Common issues:"
    echo "  - Build errors: Check your code for syntax errors"
    echo "  - Redis connection: Verify Redis add-on is attached"
    echo "  - Secrets not set: Run 'flyctl secrets list' to verify"
    echo ""
    exit 1
fi

echo ""
echo "========================================"
echo "  Deployment Successful!"
echo "========================================"

echo ""
echo "[5/5] Verifying deployment..."
echo ""
echo "Testing health endpoint..."
curl -s https://olmocr-backend-vl616a.fly.dev/api/v1/health | jq . || curl -s https://olmocr-backend-vl616a.fly.dev/api/v1/health
echo ""
echo ""

echo "Checking application status..."
flyctl status -a olmocr-backend-vl616a

echo ""
echo "========================================"
echo "  Next Steps:"
echo "========================================"
echo "  1. Test your API: https://olmocr-backend-vl616a.fly.dev/docs"
echo "  2. Check logs: flyctl logs -a olmocr-backend-vl616a"
echo "  3. Monitor app: https://fly.io/dashboard/olmocr-backend-vl616a"
echo ""
echo "  To test JWT verification:"
echo "  - Get a token from your frontend (login first)"
echo "  - Make an API request with the token"
echo "  - Check logs for 'JWT signature verified successfully'"
echo ""
echo "========================================"
