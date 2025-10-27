#!/usr/bin/env python3
"""
Enhanced Sequential Event Router Service
Bidirectional event routing: Conductor ↔ Microservices
Routes events from Conductor to microservices and handles results back
"""

import os
import json
import time
import logging
import threading
from kafka import KafkaConsumer, KafkaProducer
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
CONDUCTOR_API_URL = os.getenv('CONDUCTOR_API_URL', 'http://conductor-server:8080/api')

# Initialize Kafka producers
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Service routing configuration
SERVICE_ROUTING = {
    'pipeline_started': 'email-validation-requests',
    'phone_validation_requested': 'phone-validation-requests',
    'enrichment_requested': 'enrichment-requests',
    'pipeline_completed': 'pipeline-completion'
}

# Results routing configuration
RESULTS_ROUTING = {
    'email-validation-results': 'email_validation_completed',
    'phone-validation-results': 'phone_validation_completed',
    'enrichment-results': 'enrichment_completed'
}

class EnhancedSequentialEventRouter:
    """Enhanced Event Router with bidirectional communication"""
    
    def __init__(self):
        self.kafka_producer = kafka_producer
        self.pipeline_state = {}  # Track pipeline state per workflow
        self.running = True
        
    def _safe_deserialize(self, message_bytes):
        """Safely deserialize Kafka message, handling both JSON and string formats"""
        try:
            # First try to decode as UTF-8
            message_str = message_bytes.decode('utf-8')
            
            # Remove surrounding quotes if present
            message_str = message_str.strip()
            if message_str.startswith('"') and message_str.endswith('"'):
                message_str = message_str[1:-1]
                # Unescape JSON string
                message_str = message_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t')
            
            # If it looks like JSON, try to parse it
            if message_str.strip().startswith('{'):
                return json.loads(message_str)
            else:
                # Return as string for non-JSON messages
                return message_str
                
        except UnicodeDecodeError:
            logger.error(f"Failed to decode message as UTF-8: {message_bytes}")
            return None
        except json.JSONDecodeError as e:
            # Return as string if JSON parsing fails
            logger.warning(f"JSON parse failed, treating as string: {e}")
            return message_str
        except Exception as e:
            logger.error(f"Unexpected error deserializing message: {e}")
            return None
    
    def process_pipeline_event(self, event):
        """Process pipeline event and route to appropriate microservice"""
        try:
            event_type = event.get('eventType', 'unknown')
            workflow_id = event.get('workflowId', 'unknown')
            data = event.get('data', {})
            pipeline_stage = data.get('pipelineStage', 'unknown')
            
            logger.info(f"Processing pipeline event: {event_type} for workflow: {workflow_id}")
            logger.info(f"Pipeline stage: {pipeline_stage}")
            
            # Update pipeline state
            if workflow_id not in self.pipeline_state:
                self.pipeline_state[workflow_id] = {
                    'current_stage': 'start',
                    'stages_completed': [],
                    'data_flow': {},
                    'waiting_for': None
                }
            
            # Route based on event type
            if event_type == 'pipeline_started':
                self.route_to_email_validation(event)
            elif event_type == 'phone_validation_requested':
                self.route_to_phone_validation(event)
            elif event_type == 'enrichment_requested':
                self.route_to_enrichment(event)
            elif event_type == 'pipeline_completed':
                self.route_to_completion(event)
            else:
                logger.warning(f"Unknown event type: {event_type}")
                
        except Exception as e:
            logger.error(f"❌ Error processing pipeline event: {e}", exc_info=True)
    
    def process_result_event(self, event, topic):
        """Process result event from microservice and notify Conductor"""
        try:
            workflow_id = event.get('workflowId', 'unknown')
            task_id = event.get('taskId', 'unknown')
            event_type = event.get('eventType', 'unknown')
            data = event.get('data', {})
            
            logger.info(f"Processing result event: {event_type} for workflow: {workflow_id}, task: {task_id}")
            
            # Map result topic to completion event type
            completion_event_type = RESULTS_ROUTING.get(topic)
            if not completion_event_type:
                logger.warning(f"No completion event mapping for topic: {topic}")
                return
            
            # Create completion event for Conductor
            completion_event = {
                'workflowId': workflow_id,
                'taskId': task_id,
                'eventType': completion_event_type,
                'data': {
                    'result': data.get('result', 'success'),
                    'processedRecords': data.get('processedRecords', 0),
                    'failedRecords': data.get('failedRecords', 0),
                    'outputData': data.get('outputData', ''),
                    'pipelineStage': data.get('pipelineStage', 'unknown'),
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send completion event back to Conductor
            self.send_to_conductor(completion_event)
            
            # Update pipeline state
            if workflow_id in self.pipeline_state:
                self.pipeline_state[workflow_id]['stages_completed'].append(completion_event_type)
                self.pipeline_state[workflow_id]['waiting_for'] = None
                logger.info(f"✅ Updated pipeline state for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error processing result event: {e}", exc_info=True)
    
    def send_to_conductor(self, event):
        """Send completion event back to Conductor via conductor-events topic"""
        try:
            self.kafka_producer.send('conductor-events', event)
            self.kafka_producer.flush()
            logger.info(f"✅ Sent completion event to Conductor: {event.get('eventType')} for workflow: {event.get('workflowId')}")
        except Exception as e:
            logger.error(f"❌ Error sending to Conductor: {e}")
    
    def route_to_email_validation(self, event):
        """Route to email validation stage"""
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Create email validation request
            email_request = {
                'workflowId': workflow_id,
                'taskId': 'email_validation_task',
                'eventType': 'email_validation_request',
                'data': {
                    'inputData': data.get('inputData', 'raw_data'),
                    'records': data.get('records', 100),
                    'pipelineStage': 'email_validation',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to email validation topic
            self.kafka_producer.send('email-validation-requests', email_request)
            self.kafka_producer.flush()
            
            # Update pipeline state
            if workflow_id in self.pipeline_state:
                self.pipeline_state[workflow_id]['waiting_for'] = 'email_validation_completed'
            
            logger.info(f"✅ Routed to email validation for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to email validation: {e}")
    
    def route_to_phone_validation(self, event):
        """Route to phone validation stage"""
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Create phone validation request
            phone_request = {
                'workflowId': workflow_id,
                'taskId': 'phone_validation_task',
                'eventType': 'phone_validation_request',
                'data': {
                    'inputData': data.get('inputData', 'email_validated_data'),
                    'records': data.get('records', 95),
                    'pipelineStage': 'phone_validation',
                    'previousStage': 'email_validation',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to phone validation topic
            self.kafka_producer.send('phone-validation-requests', phone_request)
            self.kafka_producer.flush()
            
            # Update pipeline state
            if workflow_id in self.pipeline_state:
                self.pipeline_state[workflow_id]['waiting_for'] = 'phone_validation_completed'
            
            logger.info(f"✅ Routed to phone validation for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to phone validation: {e}")
    
    def route_to_enrichment(self, event):
        """Route to enrichment stage"""
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Create enrichment request
            enrichment_request = {
                'workflowId': workflow_id,
                'taskId': 'enrichment_task',
                'eventType': 'enrichment_request',
                'data': {
                    'inputData': data.get('inputData', 'phone_validated_data'),
                    'records': data.get('records', 90),
                    'pipelineStage': 'enrichment',
                    'previousStage': 'phone_validation',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to enrichment topic
            self.kafka_producer.send('enrichment-requests', enrichment_request)
            self.kafka_producer.flush()
            
            # Update pipeline state
            if workflow_id in self.pipeline_state:
                self.pipeline_state[workflow_id]['waiting_for'] = 'enrichment_completed'
            
            logger.info(f"✅ Routed to enrichment for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to enrichment: {e}")
    
    def route_to_completion(self, event):
        """Route to pipeline completion"""
        try:
            workflow_id = event.get('workflowId')
            data = event.get('data', {})
            
            # Create completion event
            completion_event = {
                'workflowId': workflow_id,
                'taskId': 'pipeline_completion_task',
                'eventType': 'pipeline_completed',
                'data': {
                    'finalData': data.get('inputData', 'enriched_data'),
                    'records': data.get('records', 90),
                    'pipelineStage': 'completed',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ')
                }
            }
            
            # Send to completion topic
            self.kafka_producer.send('pipeline-completion', completion_event)
            self.kafka_producer.flush()
            
            logger.info(f"✅ Pipeline completed for workflow: {workflow_id}")
            
        except Exception as e:
            logger.error(f"❌ Error routing to completion: {e}")
    
    def consume_conductor_events(self):
        """Consume events from Conductor and route them to microservices"""
        consumer = KafkaConsumer(
            'conductor-events',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: self._safe_deserialize(m),
            auto_offset_reset='earliest',
            group_id='enhanced-router-conductor-group',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        
        logger.info("Enhanced Event Router started - listening for conductor events")
        
        for message in consumer:
            if not self.running:
                break
                
            try:
                event = message.value
                
                # Skip if event is None or empty
                if not event:
                    logger.warning("Received empty or null event, skipping")
                    continue
                
                # Handle both string and dict formats
                if isinstance(event, str):
                    # Skip simple string messages (like "Hello from Conductor!")
                    if not event.strip().startswith('{'):
                        logger.info(f"Received non-JSON string event (skipping): {event[:50]}...")
                        continue
                    try:
                        event = json.loads(event)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON string: {event[:100]}... Error: {e}")
                        continue
                
                # Ensure event is a dictionary
                if not isinstance(event, dict):
                    logger.warning(f"Received non-dict event: {type(event)} - {str(event)[:100]}...")
                    continue
                
                event_type = event.get('eventType', 'unknown')
                logger.info(f"Received conductor event: {event_type}")
                
                # Process pipeline events
                self.process_pipeline_event(event)
                    
            except Exception as e:
                logger.error(f"Error processing conductor event: {e}", exc_info=True)
    
    def consume_result_events(self):
        """Consume result events from microservices and route them back to Conductor"""
        consumer = KafkaConsumer(
            'email-validation-results',
            'phone-validation-results', 
            'enrichment-results',
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: self._safe_deserialize(m),
            auto_offset_reset='earliest',
            group_id='enhanced-router-results-group',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        
        logger.info("Enhanced Event Router started - listening for microservice results")
        
        for message in consumer:
            if not self.running:
                break
                
            try:
                event = message.value
                topic = message.topic
                
                # Skip if event is None or empty
                if not event:
                    logger.warning("Received empty or null result event, skipping")
                    continue
                
                # Handle both string and dict formats
                if isinstance(event, str):
                    if not event.strip().startswith('{'):
                        logger.info(f"Received non-JSON string result event (skipping): {event[:50]}...")
                        continue
                    try:
                        event = json.loads(event)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON string: {event[:100]}... Error: {e}")
                        continue
                
                # Ensure event is a dictionary
                if not isinstance(event, dict):
                    logger.warning(f"Received non-dict result event: {type(event)} - {str(event)[:100]}...")
                    continue
                
                logger.info(f"Received result event from {topic}: {event.get('eventType', 'unknown')}")
                
                # Process result events
                self.process_result_event(event, topic)
                    
            except Exception as e:
                logger.error(f"Error processing result event: {e}", exc_info=True)
    
    def start(self):
        """Start the enhanced event router with bidirectional communication"""
        logger.info("Starting Enhanced Sequential Event Router Service")
        logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
        logger.info(f"Conductor API: {CONDUCTOR_API_URL}")
        logger.info(f"Service routing: {SERVICE_ROUTING}")
        logger.info(f"Results routing: {RESULTS_ROUTING}")
        
        # Wait for services to be ready
        logger.info("Waiting 10 seconds for services to initialize...")
        time.sleep(10)
        
        # Start both consumers in separate threads
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both consumer functions
            conductor_future = executor.submit(self.consume_conductor_events)
            results_future = executor.submit(self.consume_result_events)
            
            try:
                # Wait for both consumers to complete
                conductor_future.result()
                results_future.result()
            except KeyboardInterrupt:
                logger.info("Shutting down Enhanced Event Router...")
                self.running = False
                conductor_future.cancel()
                results_future.cancel()

def main():
    """Start the Enhanced Sequential Event Router"""
    router = EnhancedSequentialEventRouter()
    router.start()

if __name__ == '__main__':
    main()
