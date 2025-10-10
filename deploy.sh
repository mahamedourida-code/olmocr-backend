#!/bin/bash

# OlmOCR Backend Deployment Script
# This script automates deployment to Fly.io

set -e  # Exit on error

APP_NAME="backend-lively-hill-7043"
REGION="arn"

echo "🚀 Starting deployment to Fly.io..."
echo ""

# Check if flyctl is installed
if ! command -v fly &> /dev/null; then
    echo "❌ Error: Fly CLI is not installed"
    echo "Install it from: https://fly.io/docs/hands-on/install-flyctl/"
    exit 1
fi

# Check if logged in
if ! fly auth whoami &> /dev/null; then
    echo "❌ Error: Not logged in to Fly.io"
    echo "Run: fly auth login"
    exit 1
fi

echo "✅ Fly CLI installed and authenticated"
echo ""

# Function to set secrets
setup_secrets() {
    echo "🔐 Setting up secrets..."

    # Check if secrets already exist
    echo "Checking existing secrets..."
    fly secrets list -a $APP_NAME | grep -q "REDIS_URL" && echo "✓ REDIS_URL already set" || echo "⚠️  REDIS_URL not set"

    read -p "Do you want to (re)configure secrets? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "Setting Redis URL..."
        fly secrets set REDIS_URL="redis://default:AZ2UACQgNjUwMjVkM2YtNDM5YS00NTQzLWE3MTQtOTUyNjk1ODYzMjgwZjQ4ZmE4YjE4ZGE2NDQzMzgxYjkyNjZiMjU3YTU2OTY=@fly-waw.upstash.io:6379" -a $APP_NAME

        echo "Setting OlmOCR API Key..."
        fly secrets set OLMOCR_API_KEY="IJfGf6mJLRblDzWqR4xzjooGSoH596Gf" -a $APP_NAME

        echo "Setting Supabase URL..."
        fly secrets set SUPABASE_URL="https://iawkqvdtktnvxqgpupvt.supabase.co" -a $APP_NAME

        # Note: You'll need to add the actual keys manually if not in .env
        echo ""
        echo "⚠️  Note: Supabase keys need to be set manually if not already configured"
        echo "Run: fly secrets set SUPABASE_ANON_KEY=\"your_key\" -a $APP_NAME"
        echo "Run: fly secrets set SUPABASE_SERVICE_ROLE_KEY=\"your_key\" -a $APP_NAME"
        echo "Run: fly secrets set SUPABASE_JWT_SECRET=\"your_secret\" -a $APP_NAME"
    fi
    echo ""
}

# Function to deploy
deploy() {
    echo "📦 Deploying to Fly.io..."
    echo ""

    # Deploy
    fly deploy -a $APP_NAME

    if [ $? -eq 0 ]; then
        echo ""
        echo "✅ Deployment successful!"
        echo ""
        echo "🔍 Checking deployment status..."
        fly status -a $APP_NAME
        echo ""
        echo "📊 Recent logs (last 100 lines):"
        fly logs -a $APP_NAME | tail -100
        echo ""
        echo "🌐 Your backend is live at: https://$APP_NAME.fly.dev"
        echo "📚 API Docs: https://$APP_NAME.fly.dev/docs"
    else
        echo ""
        echo "❌ Deployment failed!"
        echo "Check logs: fly logs -a $APP_NAME"
        exit 1
    fi
}

# Function to check health
check_health() {
    echo ""
    echo "🏥 Checking service health..."

    # Wait a bit for service to start
    sleep 5

    # Check if /docs is accessible
    if curl -f -s https://$APP_NAME.fly.dev/docs > /dev/null; then
        echo "✅ Service is healthy - API docs accessible"
    else
        echo "⚠️  Service may not be fully ready yet"
        echo "Check status: fly status -a $APP_NAME"
    fi

    # Check Redis connection in logs
    echo ""
    echo "Checking Redis connection..."
    fly logs -a $APP_NAME | grep -i "redis" | tail -5

    echo ""
}

# Main menu
echo "What would you like to do?"
echo "1) Full deployment (secrets + deploy + health check)"
echo "2) Setup secrets only"
echo "3) Deploy only"
echo "4) Check health only"
echo ""
read -p "Choose option (1-4): " -n 1 -r
echo ""

case $REPLY in
    1)
        setup_secrets
        deploy
        check_health
        ;;
    2)
        setup_secrets
        ;;
    3)
        deploy
        check_health
        ;;
    4)
        check_health
        ;;
    *)
        echo "Invalid option"
        exit 1
        ;;
esac

echo ""
echo "✨ Done!"
echo ""
echo "📋 Useful commands:"
echo "  fly logs -a $APP_NAME          # View logs"
echo "  fly status -a $APP_NAME        # Check status"
echo "  fly ssh console -a $APP_NAME   # SSH into VM"
echo "  fly secrets list -a $APP_NAME  # List secrets"
echo ""
