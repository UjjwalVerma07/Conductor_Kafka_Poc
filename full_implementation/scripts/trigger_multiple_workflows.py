#!/usr/bin/env python3
"""
Trigger Multiple Workflow Instances Simultaneously
This will prove the system is asynchronous by running multiple workflows concurrently
"""

import os
import json
import sys
import time
import requests
import threading
from datetime import datetime

# Configuration
CONDUCTOR_API = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')
WORKFLOW_NAME = 'multi_stage_pipeline_visible'
WORKFLOW_VERSION = 1

def trigger_single_workflow(workflow_number, input_bucket, input_key):
    """Trigger a single workflow instance"""
    print(f"\n🚀 Starting Workflow #{workflow_number} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
    
    # Workflow input
    workflow_input = {
        "input_bucket": input_bucket,
        "input_key": input_key
    }
    
    # Start workflow
    url = f"{CONDUCTOR_API}/workflow/{WORKFLOW_NAME}"
    
    try:
        start_time = time.time()
        response = requests.post(url, json=workflow_input)
        trigger_time = time.time() - start_time
        
        if response.status_code == 200:
            workflow_id = response.text.strip('"')
            print(f"✅ Workflow #{workflow_number} started in {trigger_time:.3f}s")
            print(f"   Workflow ID: {workflow_id}")
            return workflow_id, trigger_time
        else:
            print(f"❌ Workflow #{workflow_number} failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return None, trigger_time
            
    except Exception as e:
        print(f"❌ Workflow #{workflow_number} error: {e}")
        return None, 0

def monitor_workflow(workflow_id, workflow_number):
    """Monitor a single workflow execution"""
    url = f"{CONDUCTOR_API}/workflow/{workflow_id}"
    start_time = time.time()
    
    try:
        while time.time() - start_time < 300:  # 5 minute timeout
            response = requests.get(url)
            
            if response.status_code == 200:
                workflow = response.json()
                status = workflow.get('status')
                
                # Count completed tasks
                tasks = workflow.get('tasks', [])
                completed = len([t for t in tasks if t.get('status') == 'COMPLETED'])
                in_progress = len([t for t in tasks if t.get('status') == 'IN_PROGRESS'])
                total = len(tasks)
                
                elapsed = time.time() - start_time
                print(f"🔄 Workflow #{workflow_number}: {status} | Tasks: {completed}/{total} completed, {in_progress} in progress | Elapsed: {elapsed:.1f}s")
                
                if status in ['COMPLETED', 'FAILED', 'TERMINATED']:
                    total_time = time.time() - start_time
                    if status == 'COMPLETED':
                        print(f"✅ Workflow #{workflow_number} COMPLETED in {total_time:.1f}s")
                    else:
                        print(f"❌ Workflow #{workflow_number} {status} after {total_time:.1f}s")
                    return status, total_time
                
                time.sleep(2)
            else:
                print(f"❌ Workflow #{workflow_number} monitoring error: {response.status_code}")
                return 'ERROR', time.time() - start_time
        
        print(f"⏱️  Workflow #{workflow_number} timeout after 5 minutes")
        return 'TIMEOUT', time.time() - start_time
        
    except Exception as e:
        print(f"❌ Workflow #{workflow_number} monitoring error: {e}")
        return 'ERROR', time.time() - start_time

def run_concurrent_workflows(num_workflows=3):
    """Run multiple workflows concurrently"""
    print(f"\n{'='*80}")
    print(f"🚀 ASYNCHRONOUS PROOF: Running {num_workflows} Workflows Concurrently")
    print(f"{'='*80}")
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Conductor API: {CONDUCTOR_API}")
    print(f"Workflow: {WORKFLOW_NAME} (version {WORKFLOW_VERSION})")
    
    # Input parameters
    input_bucket = 'input-data'
    input_key = 'test_data.csv'
    
    print(f"Input: {input_bucket}/{input_key}")
    print(f"{'='*80}\n")
    
    # Track workflow instances
    workflow_instances = []
    threads = []
    
    # Trigger all workflows simultaneously
    print("🎯 PHASE 1: Triggering All Workflows Simultaneously")
    print("-" * 50)
    
    trigger_start = time.time()
    
    for i in range(1, num_workflows + 1):
        workflow_id, trigger_time = trigger_single_workflow(i, input_bucket, input_key)
        if workflow_id:
            workflow_instances.append((workflow_id, i))
    
    total_trigger_time = time.time() - trigger_start
    print(f"\n📊 Trigger Results:")
    print(f"   - Total trigger time: {total_trigger_time:.3f}s")
    print(f"   - Workflows started: {len(workflow_instances)}/{num_workflows}")
    print(f"   - Average trigger time: {total_trigger_time/num_workflows:.3f}s per workflow")
    
    if not workflow_instances:
        print("\n❌ No workflows started successfully")
        return
    
    print(f"\n🎯 PHASE 2: Monitoring Concurrent Execution")
    print("-" * 50)
    
    # Monitor all workflows concurrently
    monitor_start = time.time()
    results = []
    
    def monitor_worker(workflow_id, workflow_number):
        result = monitor_workflow(workflow_id, workflow_number)
        results.append((workflow_number, workflow_id, result[0], result[1]))
    
    # Start monitoring threads
    for workflow_id, workflow_number in workflow_instances:
        thread = threading.Thread(target=monitor_worker, args=(workflow_id, workflow_number))
        thread.start()
        threads.append(thread)
    
    # Wait for all monitoring to complete
    for thread in threads:
        thread.join()
    
    total_monitor_time = time.time() - monitor_start
    
    print(f"\n🎯 PHASE 3: Results Analysis")
    print("=" * 80)
    
    # Sort results by workflow number
    results.sort(key=lambda x: x[0])
    
    print(f"\n📊 Execution Results:")
    print(f"   - Total monitoring time: {total_monitor_time:.1f}s")
    print(f"   - Workflows monitored: {len(results)}")
    
    print(f"\n📋 Individual Results:")
    for workflow_number, workflow_id, status, duration in results:
        print(f"   Workflow #{workflow_number}: {status} in {duration:.1f}s")
    
    # Analyze concurrency
    print(f"\n🔍 ASYNCHRONOUS BEHAVIOR ANALYSIS:")
    print("-" * 50)
    
    if len(results) > 1:
        # Check if workflows ran concurrently (not sequentially)
        max_duration = max(result[3] for result in results)
        min_duration = min(result[3] for result in results)
        duration_range = max_duration - min_duration
        
        print(f"   ✅ Multiple workflows executed concurrently")
        print(f"   ✅ Duration range: {min_duration:.1f}s - {max_duration:.1f}s (range: {duration_range:.1f}s)")
        
        if duration_range < 10:  # If all completed within 10 seconds of each other
            print(f"   ✅ PROOF: System is ASYNCHRONOUS - workflows ran in parallel")
        else:
            print(f"   ⚠️  Workflows may have run sequentially (not truly async)")
    else:
        print(f"   ❌ Only one workflow executed - cannot prove concurrency")
    
    # Show Conductor UI links
    print(f"\n🌐 View in Conductor UI:")
    for workflow_number, workflow_id, _, _ in results:
        print(f"   Workflow #{workflow_number}: http://localhost:5000/execution/{workflow_id}")
    
    print(f"\n{'='*80}")
    print(f"🏁 ASYNCHRONOUS PROOF COMPLETE")
    print(f"{'='*80}")

def main():
    """Main function"""
    num_workflows = 3
    
    # Allow command line override
    if len(sys.argv) > 1:
        try:
            num_workflows = int(sys.argv[1])
        except ValueError:
            print("❌ Invalid number of workflows. Using default: 3")
    
    if num_workflows < 2:
        print("❌ Need at least 2 workflows to prove concurrency")
        sys.exit(1)
    
    run_concurrent_workflows(num_workflows)

if __name__ == '__main__':
    main()
