#!/usr/bin/env python3
"""
Airflow Adapter Service - Simplified Phase 1
Executes the test_1 script to trigger Airflow DAGs via Kafka events
"""

import os
import json
import time
import logging
import subprocess
from kafka import KafkaConsumer, KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
SERVICE_NAME = os.getenv('SERVICE_NAME', 'airflow-adapter')

# Airflow Configuration (for script parameters)
AIRFLOW_HOST = os.getenv('AIRFLOW_HOST', 'papdpsaplr001l:8080')
AIRFLOW_USERNAME = os.getenv('AIRFLOW_USERNAME', 'airflow')
AIRFLOW_PASSWORD = os.getenv('AIRFLOW_PASSWORD', 'airflow')

# Script path
SCRIPT_PATH = os.path.join(os.path.dirname(__file__), 'trigger_airflow.sh')

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


class AirflowAdapterService:
    """Simplified Airflow Adapter Service - Executes test_1 script"""
    
    def __init__(self):
        self.kafka_producer = kafka_producer
        self.script_path = SCRIPT_PATH
        
        # Ensure script is executable
        os.chmod(self.script_path, 0o755)
        
        logger.info(f"✅ Airflow Adapter Service initialized")
        logger.info(f"   Script: {self.script_path}")
        logger.info(f"   Airflow Host: {AIRFLOW_HOST}")
    
    def execute_airflow_script(self, jobid, metadata_url, execution_id, dag_id=None):
        """Execute the trigger_airflow.sh script"""
        try:
            dag_id = dag_id or 'nua-culturecoding-process'
            
            logger.info(f"🚀 Executing Airflow trigger script")
            logger.info(f"   DAG ID: {dag_id}")
            logger.info(f"   Job ID: {jobid}")
            logger.info(f"   Metadata URL: {metadata_url}")
            logger.info(f"   Execution ID: {execution_id}")
            
            # Execute the bash script with parameters
            result = subprocess.run(
                [
                    '/bin/bash',
                    self.script_path,
                    jobid,           # $1
                    metadata_url,    # $2
                    execution_id,    # $3
                    dag_id,          # $4
                    AIRFLOW_HOST,    # $5
                    AIRFLOW_USERNAME,# $6
                    AIRFLOW_PASSWORD # $7
                ],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info(f"✅ Airflow DAG triggered successfully")
                if result.stdout:
                    logger.debug(f"   Script output: {result.stdout}")
                return True, result.stdout
            else:
                error_msg = f"Script execution failed: {result.stderr or result.stdout}"
                logger.error(f"❌ {error_msg}")
                return False, error_msg
                
        except subprocess.TimeoutExpired:
            error_msg = "Script execution timed out"
            logger.error(f"❌ {error_msg}")
            return False, error_msg
        except Exception as e:
            error_msg = f"Error executing script: {e}"
            logger.error(f"❌ {error_msg}")
            return False, error_msg
    
    def publish_completion_event(self, workflow_id, task_id, dag_id, jobid, 
                                  status, error_message=None, metadata_url=None):
        """Publish completion event to Conductor"""
        try:
            event_type = "airflow_dag_completed"  # Matches sink in workflow
            
            completion_event = {
                "workflowId": workflow_id,
                "taskId": task_id,
                "eventType": event_type,
                "data": {
                    "dag_id": dag_id,
                    "jobid": jobid,
                    "status": status,
                    "result": "success" if status == "success" else "failure",
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Add metadata_url if available
            if metadata_url:
                completion_event["data"]["metadata_url"] = metadata_url
            
            # Add error message if failed
            if status == "failed" and error_message:
                completion_event["data"]["error"] = error_message
            
            # Publish to conductor-events topic (Conductor sink)
            self.kafka_producer.send('conductor-events', completion_event)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Published completion event for workflow {workflow_id}")
            logger.info(f"   DAG: {dag_id}, Job: {jobid}, Status: {status}")
            
        except Exception as e:
            logger.error(f"❌ Error publishing completion event: {e}", exc_info=True)
    
    def process_task_event(self, event):
        """Process a task event from Kafka"""
        try:
            workflow_id = event.get('workflowId')
            task_id = event.get('taskId')
            data = event.get('data', {})
            
            # Extract Airflow configuration
            dag_id = data.get('dag_id', 'nua-culturecoding-process')
            execution_id = data.get('execution_id', 'WBCultureCoding')
            
            # Generate jobid (use workflow instance ID if provided, else generate)
            jobid = data.get('jobid') or workflow_id or f"job-{int(time.time())}"
            
            # Construct metadata_url from template or use provided
            metadata_url_template = data.get('metadata_url_template')
            if metadata_url_template:
                metadata_url = metadata_url_template.format(jobid=jobid)
            else:
                metadata_url = data.get('metadata_url', 
                    f"scp://dpsadmin@papdpsetld001l//intstripe/abinitio/temp/unit-testing-prod/culture_coding/stcdpsetlp008l/{jobid}/{jobid}.1.meta.{jobid}.3.sub.new.json")
            
            logger.info(f"🔧 Processing Airflow trigger request")
            logger.info(f"   Workflow ID: {workflow_id}")
            logger.info(f"   Task ID: {task_id}")
            logger.info(f"   DAG ID: {dag_id}")
            logger.info(f"   Job ID: {jobid}")
            
            # Execute the Airflow trigger script
            success, output = self.execute_airflow_script(
                jobid=jobid,
                metadata_url=metadata_url,
                execution_id=execution_id,
                dag_id=dag_id
            )
            
            # Publish completion event
            self.publish_completion_event(
                workflow_id=workflow_id,
                task_id=task_id,
                dag_id=dag_id,
                jobid=jobid,
                status="success" if success else "failed",
                error_message=None if success else output,
                metadata_url=metadata_url
            )
            
            logger.info(f"✅ Airflow task completed for workflow {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error processing Airflow task event: {e}", exc_info=True)
            
            # Publish failure event
            workflow_id = event.get('workflowId', 'unknown')
            task_id = event.get('taskId', 'unknown')
            
            self.publish_completion_event(
                workflow_id=workflow_id,
                task_id=task_id,
                dag_id=event.get('data', {}).get('dag_id', 'unknown'),
                jobid='unknown',
                status='failed',
                error_message=str(e),
                metadata_url=None
            )
    
    def consume_task_events(self):
        """Consume task events from Kafka"""
        consumer = KafkaConsumer(
            'airflow-trigger-requests',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'{SERVICE_NAME}-group'
        )
        
        logger.info(f"🚀 {SERVICE_NAME} started - listening for Airflow trigger events")
        logger.info(f"   Kafka Topic: airflow-trigger-requests")
        logger.info(f"   Airflow Host: {AIRFLOW_HOST}")
        
        for message in consumer:
            try:
                event = message.value
                
                # Handle both JSON object and string cases
                if isinstance(event, str):
                    try:
                        event = json.loads(event)
                    except json.JSONDecodeError:
                        logger.error(f"❌ Failed to parse JSON string: {event}")
                        continue
                
                event_type = event.get('eventType', 'unknown')
                
                if event_type == 'airflow_trigger_request':
                    logger.info(f"📥 Received Airflow trigger request")
                    self.process_task_event(event)
                else:
                    logger.warning(f"⚠️ Ignoring event type: {event_type}")
                    
            except Exception as e:
                logger.error(f"💥 Error processing message: {e}", exc_info=True)


def main():
    """Start the Airflow Adapter Service"""
    logger.info(f"🚀 Starting {SERVICE_NAME} Service")
    logger.info(f"🔌 Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"☁️ Airflow: {AIRFLOW_HOST}")
    
    # Wait for services to be ready
    logger.info("⏳ Waiting 10 seconds for services to initialize...")
    time.sleep(10)
    
    # Start service
    service = AirflowAdapterService()
    service.consume_task_events()


if __name__ == '__main__':
    main()
