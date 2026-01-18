#!/bin/bash
set -e

echo "🚀 Preparing to publish @vyr-e/backstage..."

# Ensure we are in the correct directory
cd "$(dirname "$0")"

echo "📦 Installing dependencies..."
bun install

echo "🛠️ Building package..."
bun run build

echo "🧪 Running tests..."
bun test

echo "📤 Publishing to npm..."
echo "Note: You make need to enter your 2FA OTP if prompted."
npm publish --access public

echo "✅ Published successfully!"
