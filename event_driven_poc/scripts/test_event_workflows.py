#!/usr/bin/env python3
"""
Test Event-Driven Workflows
Demonstrates EVENT task types and event-driven orchestration
"""

import os
import json
import time
import requests
import logging
from kafka import KafkaProducer

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

def register_event_workflows():
    """Register event-driven workflows"""
    workflows = [
        'workflows/event_listener_workflow.json',
        'workflows/advanced_event_workflow.json'
    ]
    
    for workflow_file in workflows:
        try:
            with open(workflow_file, 'r') as f:
                workflow = json.load(f)
            
            response = requests.post(
                f"{CONDUCTOR_SERVER_URL}/metadata/workflow",
                json=workflow
            )
            
            if response.status_code == 200:
                logger.info(f"✅ Registered workflow: {workflow['name']}")
            else:
                logger.error(f"❌ Failed to register workflow {workflow['name']}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Error registering workflow {workflow_file}: {e}")

def start_event_listener_workflow():
    """Start the event listener workflow"""
    try:
        response = requests.post(
            f"{CONDUCTOR_SERVER_URL}/workflow/event_listener_workflow",
            json={}
        )
        
        if response.status_code == 200:
            workflow_id = response.json()['workflowId']
            logger.info(f"✅ Started event listener workflow: {workflow_id}")
            return workflow_id
        else:
            logger.error(f"❌ Failed to start event listener workflow: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error starting event listener workflow: {e}")
        return None

def send_test_events():
    """Send test events to trigger workflow execution"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Send email validation completed event
        email_event = {
            "eventId": f"email_validation_{int(time.time() * 1000)}",
            "eventType": "email_validation_completed",
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "source": "email-validator",
            "data": {
                "status": "SUCCESS",
                "validated_emails": 150,
                "invalid_emails": 5,
                "processing_time": "2.3s"
            }
        }
        
        producer.send('conductor-events', email_event)
        logger.info("📧 Sent email validation completed event")
        
        time.sleep(2)
        
        # Send phone validation completed event
        phone_event = {
            "eventId": f"phone_validation_{int(time.time() * 1000)}",
            "eventType": "phone_validation_completed",
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "source": "phone-validator",
            "data": {
                "status": "SUCCESS",
                "validated_phones": 200,
                "invalid_phones": 12,
                "processing_time": "1.8s"
            }
        }
        
        producer.send('conductor-events', phone_event)
        logger.info("📱 Sent phone validation completed event")
        
        producer.flush()
        producer.close()
        
    except Exception as e:
        logger.error(f"❌ Error sending test events: {e}")

def monitor_workflow(workflow_id):
    """Monitor workflow execution"""
    try:
        max_attempts = 30
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

def check_kafka_messages():
    """Check Kafka messages to see event processing"""
    try:
        from kafka import KafkaConsumer
        
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
            logger.info(f"📨 Kafka message: {message.value}")
        
        if message_count == 0:
            logger.warning("⚠️ No new messages found in Kafka")
        else:
            logger.info(f"📊 Found {message_count} new messages in Kafka")
        
        consumer.close()
        
    except Exception as e:
        logger.error(f"❌ Error checking Kafka messages: {e}")

def main():
    """Main test function"""
    logger.info("🚀 Starting Event-Driven Workflow Test")
    logger.info(f"Conductor: {CONDUCTOR_SERVER_URL}")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    
    # Step 1: Check Conductor health
    if not check_conductor_health():
        logger.error("❌ Conductor server is not healthy")
        return False
    
    # Step 2: Register workflows
    register_event_workflows()
    
    # Step 3: Start event listener workflow
    workflow_id = start_event_listener_workflow()
    if not workflow_id:
        logger.error("❌ Failed to start event listener workflow")
        return False
    
    # Step 4: Wait a moment for workflow to start
    logger.info("⏳ Waiting for workflow to start...")
    time.sleep(5)
    
    # Step 5: Send test events
    logger.info("📤 Sending test events...")
    send_test_events()
    
    # Step 6: Monitor workflow
    logger.info("👀 Monitoring workflow execution...")
    workflow_result = monitor_workflow(workflow_id)
    
    # Step 7: Check Kafka messages
    logger.info("📨 Checking Kafka messages...")
    check_kafka_messages()
    
    if workflow_result and workflow_result.get('status') == 'COMPLETED':
        logger.info("🎉 Event-driven workflow test completed successfully!")
        return True
    else:
        logger.warning("⚠️ Event-driven workflow test completed with issues")
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
