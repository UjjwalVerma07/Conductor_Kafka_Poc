#!/usr/bin/env python3
"""
Processing Task Worker
Handles email_validation, phone_validation, and enrichment tasks
These tasks are updated by external microservices via HTTP callbacks
"""

import os
import time
import logging
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.configuration.configuration import Configuration
from conductor.client.worker.worker import Worker
from conductor.client.http.models import Task, TaskResult
from conductor.client.http.models.task_result_status import TaskResultStatus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONDUCTOR_SERVER_URL = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')


class ProcessingTaskWorker:
    """
    Worker that handles processing tasks (email_validation, phone_validation, enrichment)
    These tasks don't actually do work - they just mark themselves as IN_PROGRESS
    and wait for the external microservice to update them via HTTP API
    """
    
    def __init__(self, task_name):
        self.task_name = task_name
    
    def execute(self, task: Task) -> TaskResult:
        """
        Mark task as IN_PROGRESS and provide task details
        The external microservice will update this task via HTTP
        """
        task_result = TaskResult(
            task_id=task.task_id,
            workflow_instance_id=task.workflow_instance_id,
            worker_id=f'{self.task_name}_worker'
        )
        
        try:
            logger.info(f"[{self.task_name}] Task started: {task.task_id}")
            logger.info(f"[{self.task_name}] Workflow: {task.workflow_instance_id}")
            logger.info(f"[{self.task_name}] Input: {task.input_data}")
            
            # Mark as IN_PROGRESS
            # The external microservice will poll Conductor or receive this info
            # and update the task status via HTTP API when processing completes
            task_result.status = TaskResultStatus.IN_PROGRESS
            task_result.output_data = {
                'status': 'processing',
                'message': f'{self.task_name} in progress',
                'input_bucket': task.input_data.get('input_bucket'),
                'input_key': task.input_data.get('input_key'),
                'output_bucket': task.input_data.get('output_bucket'),
                'output_key': task.input_data.get('output_key')
            }
            
            # Return IN_PROGRESS - external service will complete it
            task_result.callback_after_seconds = 300  # Check again in 5 minutes if not completed
            
            logger.info(f"[{self.task_name}] Marked as IN_PROGRESS, waiting for external service")
            
        except Exception as e:
            logger.error(f"[{self.task_name}] Error: {str(e)}", exc_info=True)
            task_result.status = TaskResultStatus.FAILED
            task_result.reason_for_incompletion = str(e)
        
        return task_result


def main():
    """Start the processing task workers"""
    logger.info("Starting Processing Task Workers")
    logger.info(f"Conductor: {CONDUCTOR_SERVER_URL}")
    
    # Configure Conductor client
    configuration = Configuration(
        server_api_url=CONDUCTOR_SERVER_URL,
        debug=True
    )
    
    # Create workers for each processing task
    workers = [
        Worker(
            task_definition_name='email_validation',
            execute_function=ProcessingTaskWorker('email_validation').execute,
            poll_interval=1.0,
            domain=None
        ),
        Worker(
            task_definition_name='phone_validation',
            execute_function=ProcessingTaskWorker('phone_validation').execute,
            poll_interval=1.0,
            domain=None
        ),
        Worker(
            task_definition_name='enrichment',
            execute_function=ProcessingTaskWorker('enrichment').execute,
            poll_interval=1.0,
            domain=None
        )
    ]
    
    # Start task handler
    task_handler = TaskHandler(
        workers=workers,
        configuration=configuration
    )
    
    # Start polling
    logger.info("Processing task workers started and polling...")
    task_handler.start_processes()
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down workers...")
        task_handler.stop_processes()


if __name__ == '__main__':
    main()

