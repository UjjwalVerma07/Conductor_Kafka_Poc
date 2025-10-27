#!/usr/bin/env python3
"""
Test Simple Kafka Workflow
Tests the simple_kafka_test.json workflow to verify basic Kafka integration
"""

import os
import json
import time
import requests
import logging
from kafka import KafkaConsumer, KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONDUCTOR_SERVER_URL = os.getenv('CONDUCTOR_SERVER_URL', 'http://localhost:8080/api')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')

def check_conductor_health():
    """Check if Conductor server is healthy"""
    try:
        response = requests.get(f"{CONDUCTOR_SERVER_URL.replace('/api', '')}/health", timeout=10)
        if response.status_code == 200:
            logger.info("✅ Conductor server is healthy")
            return True
        else:
            logger.error(f"❌ Conductor server health check failed: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Cannot connect to Conductor server: {e}")
        return False

def register_task_definition():
    """Register the KAFKA_PUBLISH task definition"""
    try:
        task_def = {
            "name": "KAFKA_PUBLISH",
            "description": "Kafka publish task for event-driven architecture",
            "retryCount": 3,
            "timeoutSeconds": 300,
            "inputKeys": ["kafka_request"],
            "outputKeys": ["result"],
            "timeoutPolicy": "TIME_OUT_WF",
            "retryLogic": "FIXED",
            "retryDelaySeconds": 60,
            "responseTimeoutSeconds": 300,
            "concurrentExecLimit": None,
            "rateLimitPerFrequency": 0,
            "rateLimitFrequencyInSeconds": 1,
            "isolationGroupId": None,
            "executionNameSpace": None,
            "pollTimeoutSeconds": None,
            "backoffScaleFactor": 1
        }
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/metadata/taskdefs",
            json=[task_def]  # Send as array
        )
        
        if response.status_code == 200:
            logger.info("✅ Registered KAFKA_PUBLISH task definition")
            return True
        else:
            logger.error(f"❌ Failed to register task definition: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error registering task definition: {e}")
        return False

def register_workflow():
    """Register the simple Kafka test workflow"""
    try:
        workflow = {
            "name": "simple_kafka_test",
            "description": "Simple Kafka test with basic message",
            "version": 1,
            "tasks": [
                {
                    "name": "kafka_test_task",
                    "taskReferenceName": "kafka_test_task",
                    "type": "KAFKA_PUBLISH",
                    "inputParameters": {
                        "kafka_request": {
                            "topic": "conductor-events",
                            "bootStrapServers": "kafka:9092",
                            "value": "Hello from Conductor!",
                            "key": "test-key"
                        }
                    }
                }
            ],
            "inputParameters": [],
            "outputParameters": {
                "result": "${kafka_test_task.output}"
            },
            "schemaVersion": 2,
            "restartable": True,
            "workflowStatusListenerEnabled": False,
            "timeoutPolicy": "ALERT_ONLY",
            "timeoutSeconds": 0
        }
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/metadata/workflow",
            json=workflow
        )
        
        if response.status_code == 200:
            logger.info("✅ Registered simple_kafka_test workflow")
            return True
        else:
            logger.error(f"❌ Failed to register workflow: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error registering workflow: {e}")
        return False

def start_workflow():
    """Start the simple Kafka test workflow"""
    try:
        workflow_input = {}
        
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/workflow/simple_kafka_test",
            json=workflow_input
        )
        
        if response.status_code == 200:
            workflow_id = response.json()['workflowId']
            logger.info(f"✅ Started workflow with ID: {workflow_id}")
            return workflow_id
        else:
            logger.error(f"❌ Failed to start workflow: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error starting workflow: {e}")
        return None

def monitor_workflow(workflow_id):
    """Monitor workflow execution"""
    try:
        max_attempts = 30  # 5 minutes max
        attempt = 0
        
        while attempt < max_attempts:
            response = requests.get(f"{CONDUCTOR_SERVER_URL}/workflow/{workflow_id}")
            
            if response.status_code == 200:
                workflow_status = response.json()
                status = workflow_status.get('status', 'UNKNOWN')
                
                logger.info(f"Workflow status: {status}")
                
                if status == 'COMPLETED':
                    logger.info("✅ Workflow completed successfully!")
                    return workflow_status
                elif status == 'FAILED':
                    logger.error("❌ Workflow failed!")
                    logger.error(f"Failure reason: {workflow_status.get('reasonForIncompletion', 'Unknown')}")
                    return workflow_status
                elif status in ['RUNNING', 'PAUSED']:
                    time.sleep(10)
                    attempt += 1
                else:
                    logger.warning(f"Unknown workflow status: {status}")
                    time.sleep(10)
                    attempt += 1
            else:
                logger.error(f"Failed to get workflow status: {response.status_code}")
                time.sleep(10)
                attempt += 1
        
        logger.error("❌ Workflow monitoring timed out")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error monitoring workflow: {e}")
        return None

def verify_kafka_message():
    """Verify that the message was published to Kafka"""
    try:
        logger.info("Checking Kafka for published message...")
        
        consumer = KafkaConsumer(
            'conductor-events',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=10000
        )
        
        message_count = 0
        for message in consumer:
            message_count += 1
            logger.info(f"📨 Received Kafka message: {message.value}")
            
            # Check if this is our test message
            if isinstance(message.value, dict) and message.value.get('value') == 'Hello from Conductor!':
                logger.info("✅ Found our test message in Kafka!")
                consumer.close()
                return True
            elif isinstance(message.value, str) and message.value == 'Hello from Conductor!':
                logger.info("✅ Found our test message in Kafka!")
                consumer.close()
                return True
        
        if message_count == 0:
            logger.warning("⚠️ No messages found in Kafka topic")
        else:
            logger.info(f"📊 Found {message_count} messages in Kafka topic")
        
        consumer.close()
        return False
        
    except Exception as e:
        logger.error(f"❌ Error checking Kafka: {e}")
        return False

def main():
    """Main test function"""
    logger.info("🚀 Starting Simple Kafka Workflow Test")
    logger.info(f"Conductor: {CONDUCTOR_SERVER_URL}")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Step 1: Check Conductor health
    if not check_conductor_health():
        logger.error("❌ Conductor server is not healthy. Please start the services first.")
        return False
    
    # Step 2: Register task definition
    if not register_task_definition():
        logger.error("❌ Failed to register task definition")
        return False
    
    # Step 3: Register workflow
    if not register_workflow():
        logger.error("❌ Failed to register workflow")
        return False
    
    # Step 4: Start workflow
    workflow_id = start_workflow()
    if not workflow_id:
        logger.error("❌ Failed to start workflow")
        return False
    
    # Step 5: Monitor workflow
    workflow_result = monitor_workflow(workflow_id)
    if not workflow_result:
        logger.error("❌ Workflow monitoring failed")
        return False
    
    # Step 6: Verify Kafka message
    if verify_kafka_message():
        logger.info("🎉 Simple Kafka workflow test completed successfully!")
        return True
    else:
        logger.warning("⚠️ Kafka message verification failed, but workflow completed")
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
