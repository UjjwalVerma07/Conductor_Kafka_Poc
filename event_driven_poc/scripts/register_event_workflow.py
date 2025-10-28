#!/usr/bin/env python3
"""
Script to register EVENT task definition and event-driven workflow
"""

import requests
import json
import sys
import time

# Configuration
CONDUCTOR_SERVER_URL = "http://localhost:8080/api"

def register_event_task_definition():
    """Register the EVENT task definition"""
    try:
        with open('task_definitions/event_task.json', 'r') as f:
            task_def = json.load(f)
        
        print("📋 Registering EVENT task definition...")
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/metadata/taskdefs",
            json=task_def
        )
        
        if response.status_code == 200:
            print("✅ EVENT task definition registered successfully")
            return True
        else:
            print(f"❌ Failed to register EVENT task definition: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error registering EVENT task definition: {e}")
        return False

def register_event_workflow():
    """Register the event-driven sequential workflow"""
    try:
        with open('workflows/event_driven_sequential_workflow.json', 'r') as f:
            workflow_def = json.load(f)
        
        print("🔄 Registering event-driven sequential workflow...")
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/metadata/workflow",
            json=workflow_def
        )
        
        if response.status_code == 200:
            print("✅ Event-driven sequential workflow registered successfully")
            return True
        else:
            print(f"❌ Failed to register workflow: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error registering workflow: {e}")
        return False

def check_conductor_health():
    """Check if Conductor is healthy"""
    try:
        response = requests.get(f"{CONDUCTOR_SERVER_URL.replace('/api', '')}/health")
        if response.status_code == 200:
            health_data = response.json()
            if health_data.get('healthy', False):
                print("✅ Conductor is healthy")
                return True
            else:
                print("❌ Conductor is not healthy")
                return False
        else:
            print(f"❌ Conductor health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error checking Conductor health: {e}")
        return False

def main():
    """Main function"""
    print("🚀 Starting EVENT task workflow registration...")
    
    # Check Conductor health
    if not check_conductor_health():
        print("❌ Conductor is not available. Please start the services first.")
        sys.exit(1)
    
    # Register EVENT task definition
    if not register_event_task_definition():
        print("❌ Failed to register EVENT task definition")
        sys.exit(1)
    
    # Wait a moment for task definition to be processed
    print("⏳ Waiting for task definition to be processed...")
    time.sleep(2)
    
    # Register workflow
    if not register_event_workflow():
        print("❌ Failed to register event-driven workflow")
        sys.exit(1)
    
    print("\n🎉 EVENT task workflow registration completed successfully!")
    print("\n📋 Available workflows:")
    print("  - sequential_pipeline_workflow (WAIT-based)")
    print("  - event_driven_sequential_workflow (EVENT-based)")
    
    print("\n🧪 To test the new workflow:")
    print('curl -X POST "http://localhost:8080/api/workflow/event_driven_sequential_workflow" \\')
    print('  -H "Content-Type: application/json" \\')
    print('  -d \'{"rawData": "test_data", "records": 100}\'')

if __name__ == "__main__":
    main()
