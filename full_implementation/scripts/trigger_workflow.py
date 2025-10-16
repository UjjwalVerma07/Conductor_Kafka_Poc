#!/usr/bin/env python3
"""
Trigger the Multi-Stage Pipeline Workflow
"""

import os
import json
import sys
import time
import requests

# Configuration
CONDUCTOR_API = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')
WORKFLOW_NAME = 'multi_stage_pipeline'
WORKFLOW_VERSION = 2


def trigger_workflow(input_bucket, input_key):
    """Trigger the workflow with input data"""
    print(f"\n{'='*60}")
    print("Triggering Workflow")
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
                
                # Count task states
                tasks = workflow.get('tasks', [])
                completed = sum(1 for t in tasks if t.get('status') == 'COMPLETED')
                in_progress = sum(1 for t in tasks if t.get('status') == 'IN_PROGRESS')
                scheduled = sum(1 for t in tasks if t.get('status') == 'SCHEDULED')
                
                print(f" | Tasks: {completed}/{len(tasks)} completed, {in_progress} in progress, {scheduled} scheduled")
                
                if status in ['COMPLETED', 'FAILED', 'TERMINATED']:
                    print(f"\n{'='*60}")
                    
                    if status == 'COMPLETED':
                        print("✅ Workflow Completed Successfully!")
                        print(f"{'='*60}\n")
                        
                        # Show output
                        output = workflow.get('output', {})
                        print("Workflow Output:")
                        print(json.dumps(output, indent=2))
                        
                        # Show stage results
                        print(f"\n{'='*60}")
                        print("Stage Results:")
                        print(f"{'='*60}\n")
                        
                        for stage_key in ['stage1_output', 'stage2_output', 'stage3_output']:
                            stage_output = output.get(stage_key, {})
                            stats = stage_output.get('processing_stats', {})
                            if stats:
                                stage_num = stage_key.replace('stage', '').replace('_output', '')
                                print(f"Stage {stage_num} Stats: {json.dumps(stats, indent=2)}")
                        
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
    print("Multi-Stage Pipeline Workflow Trigger")
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
        print("\nYou can download the results from MinIO:")
        print("  - MinIO Console: http://localhost:9001")
        print("  - Bucket: output-data")
        print("  - File: final-enriched.json")
        sys.exit(0)
    else:
        print("\n❌ Pipeline failed or timed out")
        print(f"\nView workflow details in Conductor UI:")
        print(f"  http://localhost:5000/execution/{workflow_id}")
        sys.exit(1)


if __name__ == '__main__':
    main()

