#!/usr/bin/env python3
"""
Register Event-Driven Workflows
Registers task definitions and workflows for event-driven POC
"""

import os
import json
import requests
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONDUCTOR_SERVER_URL = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')

def register_task_definitions():
    """Register task definitions"""
    try:
        with open('workflows/task_definitions.json', 'r') as f:
            task_definitions = json.load(f)
        
        for task_def in task_definitions:
            response = requests.post(
                f"{CONDUCTOR_SERVER_URL}/metadata/taskdefs",
                json=task_def
            )
            
            if response.status_code == 200:
                logger.info(f"Registered task definition: {task_def['name']}")
            else:
                logger.error(f"Failed to register task definition {task_def['name']}: {response.status_code}")
                
    except Exception as e:
        logger.error(f"Error registering task definitions: {e}")

def register_workflow():
    """Register workflow"""
    try:
        with open('workflows/event_pipeline.json', 'r') as f:
            workflow = json.load(f)
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/metadata/workflow",
            json=workflow
        )
        
        if response.status_code == 200:
            logger.info(f"Registered workflow: {workflow['name']}")
        else:
            logger.error(f"Failed to register workflow {workflow['name']}: {response.status_code}")
            
    except Exception as e:
        logger.error(f"Error registering workflow: {e}")

def main():
    """Register all workflows and task definitions"""
    logger.info("Registering event-driven workflows...")
    logger.info(f"Conductor: {CONDUCTOR_SERVER_URL}")
    
    # Register task definitions
    register_task_definitions()
    
    # Register workflow
    register_workflow()
    
    logger.info("Workflow registration completed!")

if __name__ == '__main__':
    main()
