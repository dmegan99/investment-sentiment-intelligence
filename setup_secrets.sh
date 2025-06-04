#!/bin/bash

# GitHub Secrets Setup Script for Enhanced Sentiment Intelligence
# Run this script to set up all required API keys

echo "🔐 Setting up GitHub Secrets for Enhanced Sentiment Intelligence..."
echo "Repository: dmegan99/investment-sentiment-intelligence"
echo ""

# List of required secrets
secrets=(
    "GOOGLE_CLOUD_KEY"
    "SENDGRID_API_KEY"
    "BRAVE_API_KEY"
    "CLAUDE_API_KEY"
    "YOUTUBE_API_KEY"
    "API_NINJAS_KEY"
    "NEWS_API_KEY"
    "BLUESKY_USERNAME"
    "BLUESKY_PASSWORD"
    "GOOGLE_CUSTOM_SEARCH_API_KEY"
    "GOOGLE_CUSTOM_SEARCH_ENGINE_ID"
    "GOOGLE_GEMINI_API"
    "SMART_SERVICE_ACCOUNT_KEY"
)

echo "📋 Required secrets (13 total):"
for secret in "${secrets[@]}"; do
    echo "  - $secret"
done

echo ""
echo "🚀 To set up secrets:"
echo "1. Go to: https://github.com/dmegan99/investment-sentiment-intelligence/settings/secrets/actions"
echo "2. Click 'New repository secret' for each one"
echo "3. Or use this script with your values:"
echo ""

for secret in "${secrets[@]}"; do
    echo "# gh secret set $secret --body \"YOUR_${secret}_VALUE\""
done

echo ""
echo "⚡ Once secrets are set, your sentiment system will run automatically:"
echo "  - Daily at 6:30 AM ET (Morning brief)"
echo "  - Daily at 7:00 PM ET (Evening wrap-up)"
echo "  - Manual runs via GitHub Actions tab"
