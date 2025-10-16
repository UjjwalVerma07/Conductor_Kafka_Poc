#!/usr/bin/env python3
"""
Register V2 Workflow and Tasks with Conductor
Includes visible processing tasks
"""

import os
import json
import sys
import requests

# Configuration
CONDUCTOR_API = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')


def register_task_definitions(task_file):
    """Register task definitions with Conductor"""
    print(f"\n{'='*60}")
    print("Registering V2 Task Definitions")
    print(f"{'='*60}\n")
    
    # Read task definitions
    with open(task_file, 'r') as f:
        tasks = json.load(f)
    
    # Register each task
    url = f"{CONDUCTOR_API}/metadata/taskdefs"
    
    try:
        response = requests.post(url, json=tasks)
        
        if response.status_code in [200, 204]:
            print(f"✅ Successfully registered {len(tasks)} task definitions:")
            for task in tasks:
                print(f"   - {task['name']}")
            return True
        else:
            print(f"❌ Failed to register tasks: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error registering tasks: {e}")
        return False


def register_workflow_definition(workflow_file):
    """Register workflow definition with Conductor"""
    print(f"\n{'='*60}")
    print("Registering V2 Workflow Definition")
    print(f"{'='*60}\n")
    
    # Read workflow definition
    with open(workflow_file, 'r') as f:
        workflow = json.load(f)
    
    # Register workflow
    url = f"{CONDUCTOR_API}/metadata/workflow"
    
    try:
        response = requests.post(url, json=workflow)
        
        if response.status_code in [200, 204]:
            print(f"✅ Successfully registered workflow: {workflow['name']}")
            print(f"   Version: {workflow['version']}")
            print(f"   Tasks: {len(workflow['tasks'])}")
            print(f"\n   Task Flow:")
            for i, task in enumerate(workflow['tasks'], 1):
                print(f"   {i}. {task['name']} ({task['taskReferenceName']})")
            return True
        else:
            print(f"❌ Failed to register workflow: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error registering workflow: {e}")
        return False


def main():
    print(f"\n{'='*60}")
    print("Conductor V2 Workflow Registration")
    print("(With Visible Processing Tasks)")
    print(f"{'='*60}")
    print(f"Conductor API: {CONDUCTOR_API}\n")
    
    # Get file paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    workflows_dir = os.path.join(script_dir, '..', 'workflows')
    
    task_file = os.path.join(workflows_dir, 'task_definitions_v2.json')
    workflow_file = os.path.join(workflows_dir, 'multi_stage_pipeline_v2.json')
    
    # Check files exist
    if not os.path.exists(task_file):
        print(f"❌ Task definitions file not found: {task_file}")
        sys.exit(1)
    
    if not os.path.exists(workflow_file):
        print(f"❌ Workflow file not found: {workflow_file}")
        sys.exit(1)
    
    # Register tasks
    tasks_ok = register_task_definitions(task_file)
    
    if not tasks_ok:
        print("\n❌ Failed to register tasks. Aborting.")
        sys.exit(1)
    
    # Register workflow
    workflow_ok = register_workflow_definition(workflow_file)
    
    if not workflow_ok:
        print("\n❌ Failed to register workflow. Aborting.")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print("✅ V2 Registration Complete!")
    print(f"{'='*60}\n")
    print("Workflow: multi_stage_pipeline_visible")
    print("\nThis workflow includes:")
    print("  ✅ Visible email_validation task")
    print("  ✅ Visible phone_validation task")
    print("  ✅ Visible enrichment task")
    print("\nYou can now trigger it with:")
    print("  python3 trigger_workflow_v2.py")
    print()


if __name__ == '__main__':
    main()

