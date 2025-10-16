#!/usr/bin/env python3
"""
Trigger the V2 Multi-Stage Pipeline Workflow (with visible tasks)
"""

import os
import json
import sys
import time
import requests

# Configuration
CONDUCTOR_API = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')
WORKFLOW_NAME = 'multi_stage_pipeline_visible'
WORKFLOW_VERSION = 1


def trigger_workflow(input_bucket, input_key):
    """Trigger the workflow with input data"""
    print(f"\n{'='*60}")
    print("Triggering V2 Workflow (with visible tasks)")
    print(f"{'='*60}\n")
    
    # Workflow input
    workflow_input = {
        "input_bucket": input_bucket,
        "input_key": input_key
    }
    
    print(f"Workflow: {WORKFLOW_NAME} (version {WORKFLOW_VERSION})")
    print(f"Input:")
    print(f"  - Bucket: {input_bucket}")
    print(f"  - Key: {input_key}\n")
    
    # Start workflow
    url = f"{CONDUCTOR_API}/workflow/{WORKFLOW_NAME}"
    
    try:
        response = requests.post(url, json=workflow_input)
        
        if response.status_code == 200:
            workflow_id = response.text.strip('"')
            print(f"✅ Workflow started successfully!")
            print(f"   Workflow ID: {workflow_id}\n")
            return workflow_id
        else:
            print(f"❌ Failed to start workflow: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error starting workflow: {e}")
        return None


def monitor_workflow(workflow_id, timeout=300):
    """Monitor workflow execution"""
    print(f"{'='*60}")
    print("Monitoring Workflow Execution")
    print(f"{'='*60}\n")
    
    url = f"{CONDUCTOR_API}/workflow/{workflow_id}"
    start_time = time.time()
    
    try:
        while time.time() - start_time < timeout:
            response = requests.get(url)
            
            if response.status_code == 200:
                workflow = response.json()
                status = workflow.get('status')
                
                print(f"Status: {status}", end='')
                
                # Count task states by name
                tasks = workflow.get('tasks', [])
                task_summary = {}
                for t in tasks:
                    task_name = t.get('taskDefName', 'unknown')
                    task_status = t.get('status', 'unknown')
                    if task_name not in task_summary:
                        task_summary[task_name] = []
                    task_summary[task_name].append(task_status)
                
                print(f" | Tasks: {len([t for t in tasks if t.get('status') == 'COMPLETED'])}/{len(tasks)} completed")
                
                # Show processing tasks specifically
                for task in tasks:
                    if task.get('taskDefName') in ['email_validation', 'phone_validation', 'enrichment']:
                        ref_name = task.get('referenceTaskName', '')
                        task_status = task.get('status', '')
                        output = task.get('outputData', {})
                        stats = output.get('processing_stats', {})
                        
                        if task_status == 'COMPLETED' and stats:
                            print(f"   ✅ {task.get('taskDefName')}: {stats}")
                        elif task_status == 'IN_PROGRESS':
                            print(f"   ⏳ {task.get('taskDefName')}: Processing...")
                
                if status in ['COMPLETED', 'FAILED', 'TERMINATED']:
                    print(f"\n{'='*60}")
                    
                    if status == 'COMPLETED':
                        print("✅ Workflow Completed Successfully!")
                        print(f"{'='*60}\n")
                        
                        # Show output
                        output = workflow.get('output', {})
                        print("Workflow Output:")
                        print(json.dumps(output, indent=2))
                        
                        print(f"\n{'='*60}")
                        print(f"Final Output Location:")
                        print(f"  - Bucket: {output.get('final_output_bucket')}")
                        print(f"  - Key: {output.get('final_output_key')}")
                        print(f"{'='*60}\n")
                        
                        return True
                    else:
                        print(f"❌ Workflow {status}")
                        print(f"{'='*60}\n")
                        
                        # Show failed tasks
                        failed_tasks = [t for t in tasks if t.get('status') in ['FAILED', 'FAILED_WITH_TERMINAL_ERROR']]
                        if failed_tasks:
                            print("Failed Tasks:")
                            for task in failed_tasks:
                                print(f"  - {task.get('taskType')}: {task.get('reasonForIncompletion', 'Unknown error')}")
                        
                        return False
                
                time.sleep(5)
            else:
                print(f"❌ Error fetching workflow status: {response.status_code}")
                return False
        
        print(f"\n⏱️  Timeout reached after {timeout} seconds")
        return False
        
    except Exception as e:
        print(f"\n❌ Error monitoring workflow: {e}")
        return False


def main():
    print(f"\n{'='*60}")
    print("Multi-Stage Pipeline V2 Workflow Trigger")
    print("(With Visible Processing Tasks)")
    print(f"{'='*60}")
    print(f"Conductor API: {CONDUCTOR_API}\n")
    
    # Get input parameters
    input_bucket = os.getenv('INPUT_BUCKET', 'input-data')
    input_key = os.getenv('INPUT_KEY', 'test_data.csv')
    
    # Allow command line override
    if len(sys.argv) > 1:
        input_key = sys.argv[1]
    if len(sys.argv) > 2:
        input_bucket = sys.argv[2]
    
    # Trigger workflow
    workflow_id = trigger_workflow(input_bucket, input_key)
    
    if not workflow_id:
        print("\n❌ Failed to start workflow")
        sys.exit(1)
    
    # Monitor execution
    success = monitor_workflow(workflow_id)
    
    if success:
        print("🎉 Pipeline completed successfully!")
        print("\nVisible tasks showed:")
        print("  ✅ email_validation - with stats")
        print("  ✅ phone_validation - with stats")
        print("  ✅ enrichment - with stats")
        print("\nView in Conductor UI:")
        print(f"  http://localhost:5000/execution/{workflow_id}")
        sys.exit(0)
    else:
        print("\n❌ Pipeline failed or timed out")
        print(f"\nView workflow details in Conductor UI:")
        print(f"  http://localhost:5000/execution/{workflow_id}")
        sys.exit(1)


if __name__ == '__main__':
    main()

