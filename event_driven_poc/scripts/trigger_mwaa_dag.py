#!/usr/bin/env python3
"""
Trigger AWS MWAA DAG using boto3 (no session cookies needed)
Uses AWS credentials from environment or AWS CLI configuration
"""

import boto3
import json
import sys
import os
from botocore.exceptions import ClientError, NoCredentialsError

def trigger_mwaa_dag(
    mwaa_env_name,
    dag_id,
    dag_run_id,
    conf,
    region='us-east-1'
):
    """
    Trigger a DAG run in AWS MWAA using boto3
    
    Args:
        mwaa_env_name: MWAA environment name
        dag_id: DAG ID to trigger
        dag_run_id: Unique DAG run ID
        conf: Configuration dictionary to pass to DAG
        region: AWS region (default: us-east-1)
    
    Returns:
        dict: Response from MWAA API
    """
    try:
        # Get credentials from environment or use default boto3 credential chain
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_session_token = os.getenv('AWS_SESSION_TOKEN')
        
        # Create MWAA client with credentials if provided
        if aws_access_key and aws_secret_key:
            client_kwargs = {
                'region_name': region,
                'aws_access_key_id': aws_access_key,
                'aws_secret_access_key': aws_secret_key
            }
            if aws_session_token:
                client_kwargs['aws_session_token'] = aws_session_token
            mwaa_client = boto3.client('mwaa', **client_kwargs)
            print("✅ Using provided AWS credentials")
        else:
            # Use default credential chain (AWS CLI, IAM role, etc.)
            mwaa_client = boto3.client('mwaa', region_name=region)
            print("✅ Using default AWS credential chain")
        
        # Get CLI token
        print(f"Getting CLI token for MWAA environment: {mwaa_env_name}")
        token_response = mwaa_client.create_cli_token(Name=mwaa_env_name)
        cli_token = token_response['CliToken']
        web_server_url = token_response['WebServerHostname']
        
        print(f"✅ Got CLI token. Web server: {web_server_url}")
        
        # Use requests library to make API call with token
        import requests
        
        # Construct the API endpoint
        api_url = f"https://{web_server_url}/api/v1/dags/{dag_id}/dagRuns"
        
        # Prepare the request
        headers = {
            'Authorization': f'Bearer {cli_token}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'dag_run_id': dag_run_id,
            'conf': conf
        }
        
        print(f"Triggering DAG: {dag_id}")
        print(f"DAG Run ID: {dag_run_id}")
        print(f"Configuration: {json.dumps(conf, indent=2)}")
        
        # First, try to get DAG info to verify access
        print("\nChecking DAG access...")
        dag_info_url = f"https://{web_server_url}/api/v1/dags/{dag_id}"
        info_response = requests.get(
            dag_info_url,
            headers=headers,
            timeout=30
        )
        print(f"DAG Info Status: {info_response.status_code}")
        if info_response.status_code == 200:
            dag_info = info_response.json()
            print(f"DAG State: {dag_info.get('dag', {}).get('is_paused', 'unknown')}")
        else:
            print(f"⚠️ Warning: Could not get DAG info: {info_response.text}")
        
        # Make the API call
        print(f"\nTriggering DAG run...")
        response = requests.post(
            api_url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        # Check response
        if response.status_code in [200, 201]:
            print(f"✅ DAG run triggered successfully!")
            print(f"HTTP Status: {response.status_code}")
            return response.json()
        else:
            print(f"❌ Failed to trigger DAG run")
            print(f"HTTP Status: {response.status_code}")
            print(f"Response: {response.text}")
            # Don't raise exception, just return None so we can see the error
            return None
            
    except NoCredentialsError:
        print("❌ ERROR: AWS credentials not found!")
        print("   Configure AWS credentials using:")
        print("   - aws configure")
        print("   - aws sso login")
        print("   - Or set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY environment variables")
        sys.exit(1)
    except ClientError as e:
        print(f"❌ AWS API Error: {e}")
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'AccessDeniedException':
            print("   Check IAM permissions for MWAA access")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Configuration
    MWAA_ENV_NAME = os.getenv('MWAA_ENV_NAME', 'your-mwaa-env-name')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    # DAG configuration
    DAG_ID = "nua-nameparse-process-stage-v02-00-06-tiny"
    DAG_RUN_ID = "1000861642-10220"
    
    # DAG run configuration
    CONF = {
        "jobid": "1000861642-10220",
        "metadata_url": "scp://abinitio@papdpsetld003l.intra.infousa.com//home/abinitio/UQU.devm/output/1000861642.WBNameParse.json",
        "execution_id": "WBNameParse",
        "stats_url": "scp://abinitio@papdpsetld003l.intra.infousa.com//abi/log/UQU_1000861642.10200.stats.jsonl"
    }
    
    # Allow override via command line args
    if len(sys.argv) > 1:
        MWAA_ENV_NAME = sys.argv[1]
    if len(sys.argv) > 2:
        DAG_RUN_ID = sys.argv[2]
    
    print("=" * 60)
    print("AWS MWAA DAG Trigger Script")
    print("=" * 60)
    print(f"MWAA Environment: {MWAA_ENV_NAME}")
    print(f"AWS Region: {AWS_REGION}")
    print(f"DAG ID: {DAG_ID}")
    print("=" * 60)
    print()
    
    # Trigger the DAG
    result = trigger_mwaa_dag(
        mwaa_env_name=MWAA_ENV_NAME,
        dag_id=DAG_ID,
        dag_run_id=DAG_RUN_ID,
        conf=CONF,
        region=AWS_REGION
    )
    
    if result:
        print("\n✅ Success! Response:")
        print(json.dumps(result, indent=2))

