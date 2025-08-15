#!/usr/bin/env python3
# debug_test.py - Minimal test to diagnose Render issues

import sys
import os
import requests
from datetime import datetime

print("🔍 DEBUG: Script starting...")
print(f"🔍 DEBUG: Python version: {sys.version}")
print(f"🔍 DEBUG: Current time: {datetime.now()}")

# Test environment variables
print("🔍 DEBUG: Checking environment variables...")
env_vars = ['SUPABASE_URL', 'SUPABASE_ANON_KEY', 'WEBFLOW_API_TOKEN', 'WEBFLOW_SITE_ID', 'WEBFLOW_COLLECTION_ID']
for var in env_vars:
    value = os.getenv(var)
    if value:
        print(f"✅ {var}: {value[:20]}..." if len(value) > 20 else f"✅ {var}: {value}")
    else:
        print(f"❌ {var}: MISSING")

# Test basic request
print("🔍 DEBUG: Testing basic HTTP request...")
try:
    response = requests.get("https://httpbin.org/get", timeout=10)
    print(f"✅ HTTP test successful: {response.status_code}")
except Exception as e:
    print(f"❌ HTTP test failed: {e}")

# Test Supabase connection
print("🔍 DEBUG: Testing Supabase connection...")
try:
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_ANON_KEY')
    
    headers = {
        'apikey': supabase_key,
        'Authorization': f'Bearer {supabase_key}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(
        f'{supabase_url}/rest/v1/players?limit=1',
        headers=headers,
        timeout=10
    )
    print(f"✅ Supabase test: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Supabase data: {len(data)} records returned")
    else:
        print(f"❌ Supabase error: {response.text}")
        
except Exception as e:
    print(f"❌ Supabase test failed: {e}")

print("🔍 DEBUG: Script completed successfully!")
print("🔍 DEBUG: If you see this message, the environment is working")
