#!/bin/bash

# Test script for simple 2-stage Kafka flow
set -e

CONDUCTOR_URL="http://localhost:8080/api"
WORKFLOW_NAME="simple_two_stage_flow"

echo "========================================"
echo "Simple 2-Stage Kafka Flow Test"
echo "========================================"
echo ""

# Function to wait for Conductor to be ready
wait_for_conductor() {
    echo "Waiting for Conductor to be ready..."
    for i in {1..60}; do
        if curl -sf "http://localhost:8080/health" > /dev/null 2>&1; then
            echo "✓ Conductor is ready!"
            return 0
        fi
        echo -n "."
        sleep 2
    done
    echo "✗ Conductor failed to start"
    exit 1
}

# Function to register task definitions
register_tasks() {
    echo ""
    echo "Registering task definitions..."
    
    curl -X POST "${CONDUCTOR_URL}/metadata/taskdefs" \
        -H "Content-Type: application/json" \
        -d @workflows/task_definitions.json
    
    echo "✓ Task definitions registered"
}

# Function to register workflow
register_workflow() {
    echo ""
    echo "Registering workflow..."
    
    curl -X POST "${CONDUCTOR_URL}/metadata/workflow" \
        -H "Content-Type: application/json" \
        -d @workflows/simple_two_stage.json
    
    echo "✓ Workflow registered"
}

# Function to start workflow
start_workflow() {
    echo ""
    echo "Starting workflow..."
    
    RESPONSE=$(curl -s -X POST "${CONDUCTOR_URL}/workflow/${WORKFLOW_NAME}" \
        -H "Content-Type: application/json" \
        -d '{
            "input_data": "Test data from client"
        }')
    
    echo "Response: $RESPONSE"
    
    # Extract workflow ID - it's just the plain text response
    WORKFLOW_ID=$(echo "$RESPONSE" | tr -d '"' | tr -d '\n')
    
    if [ -z "$WORKFLOW_ID" ]; then
        echo "✗ Failed to start workflow"
        exit 1
    fi
    
    echo "✓ Workflow started with ID: $WORKFLOW_ID"
    echo "$WORKFLOW_ID"
}

# Function to monitor workflow
monitor_workflow() {
    local workflow_id=$1
    echo ""
    echo "Monitoring workflow execution..."
    echo "Workflow ID: $workflow_id"
    echo ""
    
    for i in {1..60}; do
        STATUS=$(curl -s "${CONDUCTOR_URL}/workflow/${workflow_id}" | jq -r '.status')
        
        if [ "$STATUS" == "COMPLETED" ]; then
            echo ""
            echo "========================================"
            echo "✓ WORKFLOW COMPLETED SUCCESSFULLY!"
            echo "========================================"
            echo ""
            
            # Get workflow output
            echo "Final Output:"
            curl -s "${CONDUCTOR_URL}/workflow/${workflow_id}" | jq '.output'
            
            echo ""
            echo "Task Results:"
            curl -s "${CONDUCTOR_URL}/workflow/${workflow_id}" | jq '.tasks[] | {name: .taskType, referenceTaskName: .referenceTaskName, status: .status, output: .outputData}'
            
            return 0
        elif [ "$STATUS" == "FAILED" ] || [ "$STATUS" == "TIMED_OUT" ]; then
            echo ""
            echo "✗ Workflow failed with status: $STATUS"
            curl -s "${CONDUCTOR_URL}/workflow/${workflow_id}" | jq '.'
            exit 1
        else
            echo "[$i] Status: $STATUS"
            
            # Show current task
            CURRENT_TASK=$(curl -s "${CONDUCTOR_URL}/workflow/${workflow_id}" | jq -r '.tasks[-1] | "\(.referenceTaskName) - \(.status)"')
            echo "    Current: $CURRENT_TASK"
        fi
        
        sleep 5
    done
    
    echo "✗ Workflow timed out"
    exit 1
}

# Main execution
wait_for_conductor

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Warning: jq is not installed. Install it for better output formatting."
    echo "  macOS: brew install jq"
    echo "  Linux: apt-get install jq"
    echo ""
fi

register_tasks
register_workflow

WORKFLOW_ID=$(start_workflow)

monitor_workflow "$WORKFLOW_ID"

echo ""
echo "========================================"
echo "Test completed successfully!"
echo "========================================"
echo ""
echo "What happened:"
echo "1. Worker published message to 'stage1-requests' Kafka topic"
echo "2. Service 1 (email-validator) consumed, processed, published to 'stage1-results'"
echo "3. Worker received result from 'stage1-results' and completed task"
echo "4. Worker published message to 'stage2-requests' Kafka topic"
echo "5. Service 2 (phone-validator) consumed, processed, published to 'stage2-results'"
echo "6. Worker received result from 'stage2-results' and completed workflow"
echo ""
echo "Check Conductor UI: http://localhost:5000"
echo "Workflow ID: $WORKFLOW_ID"

