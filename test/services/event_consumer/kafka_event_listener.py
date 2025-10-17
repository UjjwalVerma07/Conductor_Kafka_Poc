#!/usr/bin/env python3
"""
Kafka Event Consumer for Conductor EVENT tasks
This service listens to Kafka topics where Conductor publishes EVENT tasks
"""

from kafka import KafkaConsumer
import json
import time
import signal
import sys

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092']
TOPICS = [
    'conductor.email.validation.requests',
    'conductor.data.enrichment.requests',
    'conductor.workflow.completed',
    'email_validation_events',
    'data_enrichment_events',
    'workflow_completion_events'
]

class EventListener:
    def __init__(self):
        self.consumer = None
        self.running = True
        self.message_count = 0
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum, frame):
        """Graceful shutdown"""
        print(f"\n\n⏹️  Shutting down... Processed {self.message_count} events")
        self.running = False
        if self.consumer:
            self.consumer.close()
        sys.exit(0)
    
    def connect(self):
        """Connect to Kafka"""
        print("🔌 Connecting to Kafka...")
        try:
            self.consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='conductor-event-listener',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                consumer_timeout_ms=1000
            )
            print(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            print(f"✓ Subscribed to topics: {', '.join(TOPICS)}")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to Kafka: {e}")
            return False
    
    def process_event(self, message):
        """Process incoming event"""
        self.message_count += 1
        
        topic = message.topic
        key = message.key
        value = message.value
        partition = message.partition
        offset = message.offset
        timestamp = message.timestamp
        
        print(f"\n{'='*70}")
        print(f"📨 EVENT #{self.message_count} RECEIVED")
        print(f"{'='*70}")
        print(f"Topic:     {topic}")
        print(f"Key:       {key}")
        print(f"Partition: {partition}")
        print(f"Offset:    {offset}")
        print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1000))}")
        print(f"\nPayload:")
        print(json.dumps(value, indent=2))
        print(f"{'='*70}\n")
        
        # Process based on topic
        if 'validation' in topic:
            self.handle_validation_event(value)
        elif 'enrichment' in topic:
            self.handle_enrichment_event(value)
        elif 'completed' in topic or 'completion' in topic:
            self.handle_completion_event(value)
    
    def handle_validation_event(self, data):
        """Handle email validation events"""
        email = data.get('email', 'unknown')
        workflow_id = data.get('workflowId', 'unknown')
        print(f"🔍 Processing validation request for: {email}")
        print(f"   Workflow ID: {workflow_id}")
    
    def handle_enrichment_event(self, data):
        """Handle data enrichment events"""
        email = data.get('email', 'unknown')
        workflow_id = data.get('workflowId', 'unknown')
        print(f"📊 Processing enrichment request for: {email}")
        print(f"   Workflow ID: {workflow_id}")
    
    def handle_completion_event(self, data):
        """Handle workflow completion events"""
        workflow_id = data.get('workflowId', 'unknown')
        status = data.get('status', 'unknown')
        print(f"✅ Workflow completed: {workflow_id}")
        print(f"   Status: {status}")
    
    def run(self):
        """Main event loop"""
        print(f"""
╔═══════════════════════════════════════════════════════════════════╗
║         KAFKA EVENT LISTENER FOR CONDUCTOR                        ║
║         Monitoring Conductor EVENT Tasks                          ║
╚═══════════════════════════════════════════════════════════════════╝
        """)
        
        if not self.connect():
            print("Failed to connect to Kafka. Is Kafka running?")
            print("Start with: cd test && docker-compose up -d")
            return
        
        print("\n🎧 Listening for events from Conductor EVENT tasks...")
        print("Press Ctrl+C to stop\n")
        
        try:
            while self.running:
                try:
                    # Poll for messages with timeout
                    messages = self.consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, records in messages.items():
                        for message in records:
                            self.process_event(message)
                    
                    # Keep alive
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"Error processing messages: {e}")
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            self.shutdown(None, None)


if __name__ == "__main__":
    listener = EventListener()
    listener.run()

