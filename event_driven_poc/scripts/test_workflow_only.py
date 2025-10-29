#!/usr/bin/env python3
"""
Simple test to trigger the direct integration workflow
"""

import requests
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CONDUCTOR_SERVER_URL = "http://localhost:8080"
WORKFLOW_NAME = "direct_integration_workflow"

def trigger_workflow():
    """Trigger the direct integration workflow"""
    try:
        workflow_input = {
            "input_file": "customer_data_sample.csv",
            "records": 1000
        }
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/api/workflow/{WORKFLOW_NAME}",
            json=workflow_input,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            workflow_id = response.text.strip()
            logger.info(f"✅ Workflow triggered successfully with ID: {workflow_id}")
            return workflow_id
        else:
            logger.error(f"❌ Failed to trigger workflow: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error triggering workflow: {e}")
        return None

def check_workflow_status(workflow_id):
    """Check workflow execution status"""
    try:
        response = requests.get(f"{CONDUCTOR_SERVER_URL}/api/workflow/{workflow_id}")
        
        if response.status_code == 200:
            workflow_data = response.json()
            status = workflow_data.get('status', 'UNKNOWN')
            logger.info(f"📊 Workflow Status: {status}")
            
            # Show task details
            tasks = workflow_data.get('tasks', [])
            for task in tasks:
                task_name = task.get('taskDefName', task.get('referenceTaskName', 'Unknown'))
                task_status = task.get('status', 'UNKNOWN')
                logger.info(f"  - {task_name}: {task_status}")
            
            return status
        else:
            logger.error(f"❌ Failed to get workflow status: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error checking workflow status: {e}")
        return None

def main():
    """Main test function"""
    logger.info("🚀 Starting Direct Integration Workflow Test")
    logger.info("=" * 60)
    
    # Trigger workflow
    workflow_id = trigger_workflow()
    if not workflow_id:
        logger.error("❌ Failed to trigger workflow. Exiting.")
        return
    
    # Monitor workflow execution
    logger.info("📊 Monitoring workflow execution...")
    for i in range(30):  # Monitor for up to 5 minutes
        status = check_workflow_status(workflow_id)
        
        if status in ['COMPLETED', 'FAILED', 'TERMINATED']:
            logger.info(f"🏁 Workflow finished with status: {status}")
            break
        
        logger.info(f"⏳ Waiting... ({i+1}/30)")
        time.sleep(10)
    
    logger.info("=" * 60)
    logger.info("✅ Test completed!")

if __name__ == '__main__':
    main()
