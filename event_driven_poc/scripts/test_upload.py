#!/usr/bin/env python3
"""
Test Upload Script
Quick test to verify MinIO connection and upload sample data
"""

import os
import sys
import logging
from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

def test_minio_connection():
    """Test MinIO connection"""
    try:
        logger.info(f"🔌 Testing MinIO connection to {MINIO_ENDPOINT}")
        
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        
        # Test connection by listing buckets
        buckets = minio_client.list_buckets()
        logger.info(f"✅ MinIO connection successful!")
        logger.info(f"📦 Available buckets: {[bucket.name for bucket in buckets]}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ MinIO connection failed: {e}")
        logger.error("💡 Make sure MinIO is running: docker-compose up -d")
        return False

def main():
    """Main test function"""
    logger.info("🧪 MinIO Connection Test")
    logger.info("=" * 30)
    
    success = test_minio_connection()
    
    if success:
        logger.info("🎉 MinIO connection test passed!")
        logger.info("💡 You can now run: python3 scripts/upload_sample_data.py")
        return True
    else:
        logger.error("❌ MinIO connection test failed")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
