#!/usr/bin/env python3
"""
Upload Sample Data to MinIO
Uploads test CSV data to MinIO bucket for pipeline processing
"""

import os
import sys
from minio import Minio
from minio.error import S3Error

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'

# MinIO configuration
INPUT_BUCKET = 'input-data'
SAMPLE_FILE = '../sample_data/test_data.csv'
OBJECT_KEY = 'test_data.csv'


def upload_sample_data():
    """Upload sample CSV data to MinIO"""
    print(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
    
    # Initialize MinIO client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    
    try:
        # Check if bucket exists, create if not
        if not client.bucket_exists(INPUT_BUCKET):
            print(f"Creating bucket: {INPUT_BUCKET}")
            client.make_bucket(INPUT_BUCKET)
        else:
            print(f"Bucket {INPUT_BUCKET} already exists")
        
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        sample_file_path = os.path.join(script_dir, SAMPLE_FILE)
        
        # Check if sample file exists
        if not os.path.exists(sample_file_path):
            print(f"Error: Sample file not found at {sample_file_path}")
            sys.exit(1)
        
        # Upload the file
        print(f"Uploading {sample_file_path} to {INPUT_BUCKET}/{OBJECT_KEY}...")
        client.fput_object(
            INPUT_BUCKET,
            OBJECT_KEY,
            sample_file_path,
            content_type='text/csv'
        )
        
        print(f"✅ Successfully uploaded {OBJECT_KEY} to bucket {INPUT_BUCKET}")
        print(f"\nFile details:")
        print(f"  - Bucket: {INPUT_BUCKET}")
        print(f"  - Object Key: {OBJECT_KEY}")
        print(f"  - Endpoint: {MINIO_ENDPOINT}")
        
        # Get file stats
        stat = client.stat_object(INPUT_BUCKET, OBJECT_KEY)
        print(f"  - Size: {stat.size} bytes")
        print(f"  - Content Type: {stat.content_type}")
        
        return True
        
    except S3Error as e:
        print(f"❌ MinIO error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def create_additional_buckets():
    """Create intermediate and output buckets"""
    print("\nCreating additional buckets for pipeline...")
    
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    
    buckets = ['intermediate-data', 'output-data']
    
    for bucket in buckets:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"✅ Created bucket: {bucket}")
            else:
                print(f"✓ Bucket {bucket} already exists")
        except Exception as e:
            print(f"❌ Error creating bucket {bucket}: {e}")


if __name__ == '__main__':
    print("=" * 60)
    print("MinIO Sample Data Upload Script")
    print("=" * 60)
    print()
    
    # Upload sample data
    success = upload_sample_data()
    
    if success:
        # Create additional buckets
        create_additional_buckets()
        print()
        print("=" * 60)
        print("✅ Setup complete!")
        print("=" * 60)
        print("\nYou can now trigger the workflow with:")
        print(f"  - Input Bucket: {INPUT_BUCKET}")
        print(f"  - Input Key: {OBJECT_KEY}")
    else:
        print("\n❌ Failed to upload sample data")
        sys.exit(1)

