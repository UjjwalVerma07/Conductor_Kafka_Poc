#!/usr/bin/env python3
"""
Data Enricher Microservice
This service enriches data with additional information via Conductor with Kafka backend
"""

import time
import requests
import json
from typing import Dict, Any
import random

# Configuration
CONDUCTOR_URL = "http://localhost:8080/api"
TASK_TYPE = "enrich_data"
WORKER_ID = "data-enricher-1"
POLLING_INTERVAL = 1  # seconds


def enrich_data(email: str, is_valid: bool) -> Dict[str, Any]:
    """
    Enrich data with additional information
    Simulates looking up user data, company info, etc.
    """
    # Simulate data enrichment
    domain = email.split('@')[-1] if '@' in email else 'unknown'
    
    enriched_data = {
        "email": email,
        "is_valid": is_valid,
        "domain": domain,
        "enrichment": {
            "domain_type": "business" if domain in ["gmail.com", "yahoo.com", "outlook.com"] else "corporate",
            "risk_score": random.randint(1, 100),
            "country": "US",
            "timezone": "America/New_York",
            "enriched_at": time.time(),
            "enricher": WORKER_ID
        }
    }
    
    return enriched_data


def poll_task() -> Dict[str, Any]:
    """
    Poll Conductor for a task from Kafka queue
    """
    try:
        url = f"{CONDUCTOR_URL}/tasks/poll/{TASK_TYPE}"
        params = {"workerid": WORKER_ID}
        response = requests.get(url, params=params, timeout=5)
        
        if response.status_code == 200 and response.text:
            task = response.json()
            return task
        return None
    except Exception as e:
        print(f"Error polling task: {e}")
        return None


def update_task(task_id: str, workflow_instance_id: str, status: str, output: Dict[str, Any]) -> bool:
    """
    Update task status back to Conductor (publishes to Kafka)
    """
    try:
        url = f"{CONDUCTOR_URL}/tasks"
        payload = {
            "workflowInstanceId": workflow_instance_id,
            "taskId": task_id,
            "status": status,
            "outputData": output
        }
        
        response = requests.post(url, json=payload, timeout=5)
        return response.status_code == 200
    except Exception as e:
        print(f"Error updating task: {e}")
        return False


def register_task_definition():
    """
    Register task definition with Conductor
    """
    try:
        task_def = {
            "name": TASK_TYPE,
            "description": "Enriches data with additional information",
            "retryCount": 3,
            "timeoutSeconds": 300,
            "responseTimeoutSeconds": 180,
            "retryLogic": "FIXED",
            "retryDelaySeconds": 60,
            "timeoutPolicy": "TIME_OUT_WF",
            "ownerEmail": "team@example.com"
        }
        
        url = f"{CONDUCTOR_URL}/metadata/taskdefs"
        response = requests.post(url, json=[task_def], timeout=5)
        
        if response.status_code in [200, 204]:
            print(f"✓ Task definition '{TASK_TYPE}' registered")
            return True
        elif response.status_code == 409:
            print(f"✓ Task definition '{TASK_TYPE}' already exists")
            return True
        else:
            print(f"✗ Failed to register task: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Error registering task: {e}")
        return False


def check_conductor_health() -> bool:
    """
    Check if Conductor is healthy
    """
    try:
        response = requests.get(f"http://localhost:8080/health", timeout=5)
        if response.status_code == 200:
            print("✓ Conductor is healthy")
            return True
        return False
    except Exception as e:
        print(f"✗ Conductor health check failed: {e}")
        return False


def main():
    """
    Main worker loop
    """
    print(f"""
╔═══════════════════════════════════════════════════════════╗
║           DATA ENRICHER MICROSERVICE                      ║
║           Task Type: {TASK_TYPE:<35} ║
║           Worker ID: {WORKER_ID:<35} ║
║           Kafka Backend: ENABLED                          ║
╚═══════════════════════════════════════════════════════════╝
    """)
    
    # Check health
    if not check_conductor_health():
        print("Waiting for Conductor...")
        time.sleep(5)
        if not check_conductor_health():
            print("Cannot connect to Conductor. Exiting.")
            return
    
    # Register task
    register_task_definition()
    
    print(f"\n🔄 Polling for tasks (Kafka-backed queue)...")
    print("Press Ctrl+C to stop\n")
    
    task_count = 0
    
    try:
        while True:
            # Poll for task from Kafka via Conductor
            task = poll_task()
            
            if task and task.get('taskId'):
                task_count += 1
                print(f"\n{'='*60}")
                print(f"📊 Task #{task_count} Received from Kafka Queue")
                print(f"Task ID: {task['taskId']}")
                print(f"Workflow: {task['workflowInstanceId']}")
                print(f"Input: {json.dumps(task.get('inputData', {}), indent=2)}")
                print(f"{'='*60}")
                
                try:
                    # Extract data from input
                    input_data = task.get('inputData', {})
                    email = input_data.get('email', '')
                    is_valid = input_data.get('is_valid', False)
                    
                    if not email:
                        raise ValueError("No email provided in input")
                    
                    # Enrich data
                    result = enrich_data(email, is_valid)
                    
                    print(f"✓ Data enriched: Domain={result['domain']}, "
                          f"Risk Score={result['enrichment']['risk_score']}")
                    
                    # Update task as completed
                    if update_task(task['taskId'], task['workflowInstanceId'], "COMPLETED", result):
                        print(f"✓ Task completed and published to Kafka\n")
                    else:
                        print(f"✗ Failed to update task\n")
                        
                except Exception as e:
                    print(f"✗ Error processing task: {e}")
                    error_output = {
                        "error": str(e),
                        "email": task.get('inputData', {}).get('email', '')
                    }
                    update_task(task['taskId'], task['workflowInstanceId'], "FAILED", error_output)
            else:
                # No task, wait
                time.sleep(POLLING_INTERVAL)
                
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Worker stopped. Processed {task_count} tasks.")
    except Exception as e:
        print(f"\n✗ Worker error: {e}")


if __name__ == "__main__":
    main()

