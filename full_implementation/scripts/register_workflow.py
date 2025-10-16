#!/usr/bin/env python3
"""
Register Workflow and Tasks with Conductor
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
    print("Registering Task Definitions")
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
    print("Registering Workflow Definition")
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
            return True
        else:
            print(f"❌ Failed to register workflow: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error registering workflow: {e}")
        return False


def verify_registration():
    """Verify that tasks and workflow were registered"""
    print(f"\n{'='*60}")
    print("Verifying Registration")
    print(f"{'='*60}\n")
    
    try:
        # Check tasks
        tasks_url = f"{CONDUCTOR_API}/metadata/taskdefs"
        response = requests.get(tasks_url)
        
        if response.status_code == 200:
            all_tasks = response.json()
            our_tasks = [t for t in all_tasks if 'file' in t.get('name', '').lower() or 'publish' in t.get('name', '').lower() or 'wait' in t.get('name', '').lower()]
            print(f"✅ Found {len(our_tasks)} registered tasks")
            for task in our_tasks:
                print(f"   - {task['name']}")
        
        # Check workflow
        workflow_url = f"{CONDUCTOR_API}/metadata/workflow/multi_stage_pipeline?version=1"
        response = requests.get(workflow_url)
        
        if response.status_code == 200:
            workflow = response.json()
            print(f"\n✅ Workflow 'multi_stage_pipeline' verified")
            print(f"   Tasks in workflow: {len(workflow.get('tasks', []))}")
        else:
            print(f"\n⚠️  Could not verify workflow")
            
    except Exception as e:
        print(f"❌ Error verifying: {e}")


def main():
    print(f"\n{'='*60}")
    print("Conductor Workflow Registration")
    print(f"{'='*60}")
    print(f"Conductor API: {CONDUCTOR_API}\n")
    
    # Get file paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    workflows_dir = os.path.join(script_dir, '..', 'workflows')
    
    task_file = os.path.join(workflows_dir, 'task_definitions.json')
    workflow_file = os.path.join(workflows_dir, 'multi_stage_pipeline.json')
    
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
    
    # Verify
    verify_registration()
    
    print(f"\n{'='*60}")
    print("✅ Registration Complete!")
    print(f"{'='*60}\n")
    print("You can now trigger the workflow with:")
    print("  python3 trigger_workflow.py")
    print()


if __name__ == '__main__':
    main()

