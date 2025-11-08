#!/usr/bin/env python3
"""
Trigger Event-Driven Sequential Workflow
Simple script to trigger the event_driven_sequential_workflow
"""

import requests
import sys

# Configuration
CONDUCTOR_API_URL = "http://localhost:8080/api"
WORKFLOW_NAME = "event_driven_sequential_workflow"

def trigger_workflow():
    """Trigger the event-driven sequential workflow"""
    url = f"{CONDUCTOR_API_URL}/workflow/{WORKFLOW_NAME}"
    payload = {}
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        workflow_id = response.text.strip()
        print(f"Workflow triggered successfully!")
        print(f"Workflow ID: {workflow_id}")
        return workflow_id
    except requests.exceptions.RequestException as e:
        print(f"Error triggering workflow: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        sys.exit(1)

if __name__ == "__main__":
    trigger_workflow()
