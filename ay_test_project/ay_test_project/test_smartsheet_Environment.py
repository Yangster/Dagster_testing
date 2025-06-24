#!/usr/bin/env python3
"""
Test script to verify Smartsheet connection in your Dagster environment
"""

import os
import smartsheet
import requests
import certifi
import ssl
import sys

def test_environment_info():
    """Print environment information"""
    print("=== Environment Information ===")
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    print(f"SSL version: {ssl.OPENSSL_VERSION}")
    print(f"Certifi bundle location: {certifi.where()}")
    print(f"Requests version: {requests.__version__}")
    print(f"Smartsheet SDK version: {smartsheet.__version__}")
    print()

def test_direct_api_call():
    """Test direct API call to Smartsheet"""
    print("=== Testing Direct API Call ===")
    
    bearer = os.environ.get("SMARTSHEETS_TOKEN")
    if not bearer:
        print("❌ SMARTSHEETS_TOKEN environment variable not found")
        return False
    
    try:
        response = requests.get(
            "https://api.smartsheet.com/2.0/sheets",
            headers={"Authorization": f"Bearer {bearer}"},
            timeout=10
        )
        print(f"✅ Direct API call successful! Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   Found {len(data.get('data', []))} sheets")
        return True
    except Exception as e:
        print(f"❌ Direct API call failed: {e}")
        return False

def test_smartsheet_sdk():
    """Test using Smartsheet SDK"""
    print("=== Testing Smartsheet SDK ===")
    
    bearer = os.environ.get("SMARTSHEETS_TOKEN")
    if not bearer:
        print("❌ SMARTSHEETS_TOKEN environment variable not found")
        return False
    
    try:
        ss_client = smartsheet.Smartsheet(f"Bearer {bearer}")
        response = ss_client.Sheets.list_sheets(include_all=True)
        print(f"✅ Smartsheet SDK successful!")
        print(f"   Found {len(response.data)} sheets")
        
        # Print first few sheet names for verification
        if response.data:
            print("   First few sheets:")
            for sheet in response.data[:3]:
                print(f"     - {sheet.name}")
        
        return True
    except Exception as e:
        print(f"❌ Smartsheet SDK failed: {e}")
        return False

def test_with_ssl_fixes():
    """Test Smartsheet SDK with SSL configuration fixes"""
    print("=== Testing Smartsheet SDK with SSL fixes ===")
    
    bearer = os.environ.get("SMARTSHEETS_TOKEN")
    if not bearer:
        print("❌ SMARTSHEETS_TOKEN environment variable not found")
        return False
    
    try:
        # Set certificate bundle environment variables
        cert_bundle = certifi.where()
        os.environ['REQUESTS_CA_BUNDLE'] = cert_bundle
        os.environ['SSL_CERT_FILE'] = cert_bundle
        
        ss_client = smartsheet.Smartsheet(f"Bearer {bearer}")
        
        # Configure the session with explicit certificate bundle
        session = requests.Session()
        session.verify = cert_bundle
        ss_client._session = session
        
        response = ss_client.Sheets.list_sheets(include_all=True)
        print(f"✅ Smartsheet SDK with SSL fixes successful!")
        print(f"   Found {len(response.data)} sheets")
        return True
    except Exception as e:
        print(f"❌ Smartsheet SDK with SSL fixes failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing Smartsheet connection in Dagster environment...\n")
    
    test_environment_info()
    
    # Test 1: Direct API call
    api_success = test_direct_api_call()
    print()
    
    # Test 2: Smartsheet SDK
    sdk_success = test_smartsheet_sdk()
    print()
    
    # Test 3: SDK with SSL fixes (if basic SDK failed)
    if not sdk_success:
        ssl_success = test_with_ssl_fixes()
        print()
    
    # Summary
    print("=== Summary ===")
    if api_success and sdk_success:
        print("✅ All tests passed! Your Smartsheet connection should work in Dagster.")
    elif api_success and not sdk_success:
        print("⚠️  Direct API works but SDK fails. This is an SSL configuration issue.")
        print("   Try the SSL fixes in your Dagster asset.")
    else:
        print("❌ Connection failed. Check your token and network connectivity.")
    
    print("\nNext steps:")
    if not api_success:
        print("1. Verify your SMARTSHEETS_TOKEN environment variable")
        print("2. Check your internet connection")
        print("3. Verify the token has the correct permissions")
    elif not sdk_success:
        print("1. Use the SSL fixes in your Dagster asset code")
        print("2. Consider updating certificates: conda update certifi ca-certificates")