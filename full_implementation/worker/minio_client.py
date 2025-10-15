#!/usr/bin/env python3
"""
MinIO Client Helper
Provides utilities for downloading/uploading files from/to MinIO
"""

import os
import json
import logging
from minio import Minio
from minio.error import S3Error
from io import BytesIO

logger = logging.getLogger(__name__)

class MinIOClient:
    """MinIO client wrapper for easy file operations"""
    
    def __init__(self):
        """Initialize MinIO client from environment variables"""
        self.endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        
        # Create MinIO client
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )
        
        logger.info(f"MinIO client initialized: {self.endpoint}")
    
    def ensure_bucket(self, bucket_name):
        """Create bucket if it doesn't exist"""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Created bucket: {bucket_name}")
            else:
                logger.info(f"Bucket already exists: {bucket_name}")
        except S3Error as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            raise
    
    def download_file(self, bucket_name, object_key):
        """
        Download file from MinIO and return content as string
        
        Args:
            bucket_name: Name of the bucket
            object_key: Path to the file in bucket
            
        Returns:
            str: File content as string
        """
        try:
            logger.info(f"Downloading from MinIO: {bucket_name}/{object_key}")
            
            response = self.client.get_object(bucket_name, object_key)
            content = response.read().decode('utf-8')
            response.close()
            response.release_conn()
            
            logger.info(f"Successfully downloaded: {bucket_name}/{object_key}")
            return content
            
        except S3Error as e:
            logger.error(f"Error downloading {bucket_name}/{object_key}: {e}")
            raise
    
    def upload_file(self, bucket_name, object_key, content, content_type='application/json'):
        """
        Upload content to MinIO
        
        Args:
            bucket_name: Name of the bucket
            object_key: Path where to save the file
            content: Content to upload (string or bytes)
            content_type: MIME type of content
            
        Returns:
            str: Object key of uploaded file
        """
        try:
            logger.info(f"Uploading to MinIO: {bucket_name}/{object_key}")
            
            # Convert string to bytes if needed
            if isinstance(content, str):
                content_bytes = content.encode('utf-8')
            else:
                content_bytes = content
            
            # Upload using BytesIO
            content_stream = BytesIO(content_bytes)
            self.client.put_object(
                bucket_name,
                object_key,
                content_stream,
                length=len(content_bytes),
                content_type=content_type
            )
            
            logger.info(f"Successfully uploaded: {bucket_name}/{object_key}")
            return object_key
            
        except S3Error as e:
            logger.error(f"Error uploading {bucket_name}/{object_key}: {e}")
            raise
    
    def download_json(self, bucket_name, object_key):
        """Download and parse JSON file from MinIO"""
        content = self.download_file(bucket_name, object_key)
        return json.loads(content)
    
    def upload_json(self, bucket_name, object_key, data):
        """Upload JSON data to MinIO"""
        content = json.dumps(data, indent=2)
        return self.upload_file(bucket_name, object_key, content, 'application/json')
    
    def list_objects(self, bucket_name, prefix=''):
        """List objects in bucket with optional prefix"""
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects in {bucket_name}: {e}")
            raise


# Singleton instance
_minio_client = None

def get_minio_client():
    """Get or create MinIO client singleton"""
    global _minio_client
    if _minio_client is None:
        _minio_client = MinIOClient()
    return _minio_client