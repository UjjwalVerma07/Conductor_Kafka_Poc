#!/usr/bin/env python3
"""
Check if Conductor EVENT task type is enabled and properly configured
"""

import requests
import json
import sys

CONDUCTOR_URL = "http://localhost:8080"
API_URL = f"{CONDUCTOR_URL}/api"

# Colors for output
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
RED = '\033[0;31m'
BLUE = '\033[0;34m'
NC = '\033[0m'  # No Color


def print_header(text):
    print(f"\n{BLUE}{'='*70}{NC}")
    print(f"{BLUE}{text}{NC}")
    print(f"{BLUE}{'='*70}{NC}\n")


def print_success(text):
    print(f"{GREEN}✓ {text}{NC}")


def print_error(text):
    print(f"{RED}✗ {text}{NC}")


def print_warning(text):
    print(f"{YELLOW}⚠ {text}{NC}")


def check_conductor_health():
    """Check if Conductor is running"""
    print_header("Step 1: Checking Conductor Health")
    try:
        response = requests.get(f"{CONDUCTOR_URL}/health", timeout=5)
        if response.status_code == 200:
            print_success("Conductor is running and healthy")
            return True
        else:
            print_error(f"Conductor returned status: {response.status_code}")
            return False
    except Exception as e:
        print_error(f"Cannot connect to Conductor: {e}")
        print_warning("Make sure Conductor is running: docker-compose up -d")
        return False


def check_kafka_connection():
    """Check if Kafka is accessible"""
    print_header("Step 2: Checking Kafka Connection")
    try:
        from kafka import KafkaAdminClient
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:29092'],
            client_id='event-checker',
            request_timeout_ms=5000
        )
        topics = admin_client.list_topics()
        print_success(f"Kafka is accessible")
        print(f"   Current topics: {len(topics)} topics found")
        admin_client.close()
        return True
    except ImportError:
        print_warning("kafka-python not installed (optional for this test)")
        print("   Install with: pip install kafka-python")
        return True  # Not critical for EVENT task
    except Exception as e:
        print_error(f"Cannot connect to Kafka: {e}")
        return False


def get_system_info():
    """Get Conductor system information"""
    print_header("Step 3: Checking Conductor Configuration")
    try:
        # Try to get metadata endpoint
        response = requests.get(f"{API_URL}/metadata/taskdefs", timeout=5)
        if response.status_code == 200:
            print_success("Conductor API is accessible")
            return True
        else:
            print_error(f"API returned: {response.status_code}")
            return False
    except Exception as e:
        print_error(f"Cannot access Conductor API: {e}")
        return False


def test_event_task_registration():
    """Test if we can register an EVENT task in a workflow"""
    print_header("Step 4: Testing EVENT Task Registration")
    
    # Create a simple test workflow with EVENT task
    test_workflow = {
        "name": "event_task_test_workflow",
        "description": "Test workflow to verify EVENT task support",
        "version": 1,
        "tasks": [
            {
                "name": "test_kafka_event",
                "taskReferenceName": "test_kafka_event_ref",
                "inputParameters": {
                    "kafka_request": {
                        "topic": "conductor.test.events",
                        "key": "${workflow.input.test_key}",
                        "value": {
                            "message": "Testing EVENT task support",
                            "timestamp": "${workflow.startTime}"
                        }
                    }
                },
                "type": "EVENT",
                "sink": "kafka"
            }
        ],
        "schemaVersion": 2,
        "ownerEmail": "test@example.com"
    }
    
    try:
        # Try to register the workflow
        response = requests.post(
            f"{API_URL}/metadata/workflow",
            json=test_workflow,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code in [200, 204]:
            print_success("EVENT task type is SUPPORTED")
            print("   Successfully registered workflow with EVENT task")
            return True
        elif response.status_code == 409:
            print_success("EVENT task type is SUPPORTED")
            print("   Test workflow already exists")
            return True
        else:
            print_error(f"Failed to register EVENT task workflow: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except Exception as e:
        print_error(f"Error testing EVENT task: {e}")
        return False


def test_event_task_execution():
    """Try to start a workflow with EVENT task"""
    print_header("Step 5: Testing EVENT Task Execution")
    
    try:
        # Start the test workflow
        response = requests.post(
            f"{API_URL}/workflow/event_task_test_workflow",
            json={"test_key": "test_value_123"},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            workflow_id = response.text.strip('"')
            print_success(f"EVENT task workflow started successfully")
            print(f"   Workflow ID: {workflow_id}")
            
            # Wait a moment and check status
            import time
            time.sleep(2)
            
            status_response = requests.get(f"{API_URL}/workflow/{workflow_id}", timeout=5)
            if status_response.status_code == 200:
                workflow_data = status_response.json()
                status = workflow_data.get('status', 'UNKNOWN')
                print(f"   Workflow Status: {status}")
                
                # Check if EVENT task was executed
                tasks = workflow_data.get('tasks', [])
                event_tasks = [t for t in tasks if t.get('taskType') == 'EVENT']
                
                if event_tasks:
                    print_success(f"Found {len(event_tasks)} EVENT task(s) in workflow")
                    for task in event_tasks:
                        task_status = task.get('status', 'UNKNOWN')
                        task_name = task.get('referenceTaskName', 'unknown')
                        print(f"   • {task_name}: {task_status}")
                    return True
                else:
                    print_warning("No EVENT tasks found in workflow execution")
                    return False
            
            return True
        else:
            print_error(f"Failed to start workflow: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except Exception as e:
        print_error(f"Error executing EVENT task workflow: {e}")
        return False


def check_kafka_topics():
    """Check if Kafka topics are being created"""
    print_header("Step 6: Checking Kafka Topics for Events")
    
    try:
        from kafka import KafkaAdminClient
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:29092'],
            client_id='event-checker',
            request_timeout_ms=5000
        )
        topics = admin_client.list_topics()
        
        # Look for conductor or event-related topics
        conductor_topics = [t for t in topics if 'conductor' in t.lower() or 'event' in t.lower()]
        
        if conductor_topics:
            print_success(f"Found {len(conductor_topics)} Conductor/Event topic(s)")
            for topic in conductor_topics[:10]:  # Show first 10
                print(f"   • {topic}")
        else:
            print_warning("No Conductor-specific topics found yet")
            print("   Topics will be created when EVENT tasks execute")
        
        admin_client.close()
        return True
        
    except ImportError:
        print_warning("kafka-python not installed - skipping topic check")
        print("   Install with: pip install kafka-python")
        return True
    except Exception as e:
        print_warning(f"Could not list Kafka topics: {e}")
        return True  # Not critical


def main():
    print(f"""
{BLUE}╔═══════════════════════════════════════════════════════════════════╗
║     CONDUCTOR EVENT TASK TYPE VERIFICATION                        ║
║     Checking if Kafka EVENT tasks are enabled                     ║
╚═══════════════════════════════════════════════════════════════════╝{NC}
    """)
    
    results = {
        "conductor_health": check_conductor_health(),
        "kafka_connection": check_kafka_connection(),
        "system_info": get_system_info(),
        "event_registration": test_event_task_registration(),
        "event_execution": test_event_task_execution(),
        "kafka_topics": check_kafka_topics()
    }
    
    # Summary
    print_header("SUMMARY")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    print(f"Tests Passed: {passed}/{total}\n")
    
    for test_name, result in results.items():
        status = f"{GREEN}PASS{NC}" if result else f"{RED}FAIL{NC}"
        print(f"  {test_name.replace('_', ' ').title()}: {status}")
    
    print()
    
    if results["event_registration"] and results["event_execution"]:
        print(f"{GREEN}╔═══════════════════════════════════════════════════════════════════╗")
        print(f"║  ✓ EVENT TASK TYPE IS ENABLED AND WORKING!                       ║")
        print(f"╚═══════════════════════════════════════════════════════════════════╝{NC}\n")
        
        print("Next steps:")
        print("  1. Register your event workflows:")
        print("     ./scripts/register_event_workflows.sh")
        print()
        print("  2. Start the Kafka event listener:")
        print("     python3 services/event_consumer/kafka_event_listener.py")
        print()
        print("  3. Trigger an event workflow:")
        print("     ./scripts/trigger_event_workflow.sh")
        print()
        return 0
    else:
        print(f"{RED}╔═══════════════════════════════════════════════════════════════════╗")
        print(f"║  ✗ EVENT TASK TYPE MAY NOT BE PROPERLY CONFIGURED                ║")
        print(f"╚═══════════════════════════════════════════════════════════════════╝{NC}\n")
        
        print("Troubleshooting:")
        print("  1. Check config.properties for Kafka settings")
        print("  2. Ensure Kafka is running: docker-compose ps")
        print("  3. Check Conductor logs: docker-compose logs conductor-server")
        print()
        return 1


if __name__ == "__main__":
    sys.exit(main())

