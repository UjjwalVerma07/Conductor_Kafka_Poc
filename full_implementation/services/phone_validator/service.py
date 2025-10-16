#!/usr/bin/env python3
"""
Phone Validator Service
Consumes from Kafka, downloads JSON from MinIO, validates phones, uploads result
"""

import os
import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from validator import process_csv_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
SERVICE_NAME = os.getenv('SERVICE_NAME', 'phone-validator')
STAGE = int(os.getenv('STAGE', '2'))

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def download_from_minio(bucket, key):
    """Download file from MinIO"""
    try:
        logger.info(f"Downloading from MinIO: {bucket}/{key}")
        response = minio_client.get_object(bucket, key)
        content = response.read().decode('utf-8')
        response.close()
        response.release_conn()
        logger.info(f"Downloaded {len(content)} bytes")
        return content
    except S3Error as e:
        logger.error(f"MinIO download error: {e}")
        raise


def upload_to_minio(bucket, key, content):
    """Upload file to MinIO"""
    try:
        logger.info(f"Uploading to MinIO: {bucket}/{key}")
        
        # Ensure bucket exists
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            logger.info(f"Created bucket: {bucket}")
        
        # Upload content
        content_bytes = content.encode('utf-8') if isinstance(content, str) else content
        content_stream = BytesIO(content_bytes)
        
        minio_client.put_object(
            bucket,
            key,
            content_stream,
            length=len(content_bytes),
            content_type='application/json'
        )
        
        logger.info(f"Uploaded to {bucket}/{key}")
        return key
    except S3Error as e:
        logger.error(f"MinIO upload error: {e}")
        raise


def process_message(message_data):
    """Process a file request message"""
    try:
        workflow_id = message_data['workflowId']
        task_id = message_data['taskId']
        stage = message_data['stage']
        input_bucket = message_data['input_bucket']
        input_key = message_data['input_key']
        output_bucket = message_data['output_bucket']
        output_key = message_data['output_key']
        
        logger.info(f"Processing workflow {workflow_id}, stage {stage}")
        
        # Download input file from MinIO (JSON from Stage 1)
        json_content = download_from_minio(input_bucket, input_key)
        
        # Process and validate phones
        result = process_csv_data(json_content)
        
        # Upload result to MinIO as JSON
        result_json = json.dumps(result, indent=2)
        upload_to_minio(output_bucket, output_key, result_json)
        
        # Publish result to Kafka
        result_message = {
            'workflowId': workflow_id,
            'taskId': task_id,
            'stage': stage,
            'status': 'success',
            'output_bucket': output_bucket,
            'output_key': output_key,
            'processing_stats': result['stats']
        }
        
        result_topic = f'file-results-service{stage}'
        kafka_producer.send(result_topic, value=result_message)
        kafka_producer.flush()
        
        logger.info(f"Published result to {result_topic}")
        logger.info(f"Completed workflow {workflow_id}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        # Optionally publish error message
        raise


def main():
    """Main service loop"""
    logger.info(f"Starting {SERVICE_NAME}")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"MinIO: {MINIO_ENDPOINT}")
    logger.info(f"Stage: {STAGE}")
    
    # Wait for services to be ready
    logger.info("Waiting 10 seconds for services to initialize...")
    time.sleep(10)
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        'file-requests',
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id=f'{SERVICE_NAME}-group'
    )
    
    logger.info(f"Listening on 'file-requests' topic (filtering stage={STAGE})")
    
    # Consume and process messages
    for message in consumer:
        try:
            data = message.value
            logger.info(f"Received message: stage={data.get('stage')}")
            
            # Filter by stage
            if data.get('stage') == STAGE:
                logger.info(f"Processing message for stage {STAGE}")
                process_message(data)
            else:
                logger.debug(f"Skipping message for stage {data.get('stage')}")
                
        except Exception as e:
            logger.error(f"Error in message loop: {e}", exc_info=True)


if __name__ == '__main__':
    main()