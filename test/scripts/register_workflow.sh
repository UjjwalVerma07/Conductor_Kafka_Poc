#!/bin/bash

# Register workflow and task definitions with Conductor

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

CONDUCTOR_URL="http://localhost:8080/api"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo -e "${YELLOW}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║   Registering Kafka-backed Workflow with Conductor       ║${NC}"
echo -e "${YELLOW}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check Conductor health
echo -e "${YELLOW}Checking Conductor health...${NC}"
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo -e "${RED}✗ Conductor is not running!${NC}"
    echo "Start it with: cd ../.. && docker-compose up -d"
    exit 1
fi
echo -e "${GREEN}✓ Conductor is running${NC}"
echo ""

# Register task definitions
echo -e "${YELLOW}Step 1: Registering task definitions...${NC}"
TASK_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${CONDUCTOR_URL}/metadata/taskdefs" \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/../workflows/task_definitions.json")

HTTP_CODE=$(echo "$TASK_RESPONSE" | tail -n1)
if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 204 ]; then
    echo -e "${GREEN}✓ Task definitions registered successfully${NC}"
    echo "  • validate_email"
    echo "  • enrich_data"
elif [ "$HTTP_CODE" -eq 409 ]; then
    echo -e "${YELLOW}✓ Task definitions already exist (this is fine)${NC}"
else
    echo -e "${RED}✗ Failed to register tasks (HTTP $HTTP_CODE)${NC}"
    exit 1
fi
echo ""

# Register workflow
echo -e "${YELLOW}Step 2: Registering workflow...${NC}"
WORKFLOW_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${CONDUCTOR_URL}/metadata/workflow" \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/../workflows/email_processing_workflow.json")

HTTP_CODE=$(echo "$WORKFLOW_RESPONSE" | tail -n1)
if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 204 ]; then
    echo -e "${GREEN}✓ Workflow registered successfully${NC}"
    echo "  Workflow: email_processing_workflow"
elif [ "$HTTP_CODE" -eq 409 ]; then
    echo -e "${YELLOW}✓ Workflow already exists (this is fine)${NC}"
else
    echo -e "${RED}✗ Failed to register workflow (HTTP $HTTP_CODE)${NC}"
    echo "$WORKFLOW_RESPONSE" | head -n -1
    exit 1
fi
echo ""

# Verify registration
echo -e "${YELLOW}Step 3: Verifying registration...${NC}"

# Check task definitions
TASK1=$(curl -s "${CONDUCTOR_URL}/metadata/taskdefs/validate_email" | grep -o '"name":"validate_email"' || echo "")
TASK2=$(curl -s "${CONDUCTOR_URL}/metadata/taskdefs/enrich_data" | grep -o '"name":"enrich_data"' || echo "")

if [ -n "$TASK1" ] && [ -n "$TASK2" ]; then
    echo -e "${GREEN}✓ Task definitions verified${NC}"
else
    echo -e "${RED}✗ Task definitions not found${NC}"
    exit 1
fi

# Check workflow
WORKFLOW=$(curl -s "${CONDUCTOR_URL}/metadata/workflow/email_processing_workflow?version=1" | grep -o '"name":"email_processing_workflow"' || echo "")

if [ -n "$WORKFLOW" ]; then
    echo -e "${GREEN}✓ Workflow verified${NC}"
else
    echo -e "${RED}✗ Workflow not found${NC}"
    exit 1
fi
echo ""

echo -e "${GREEN}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Registration Complete!                                  ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Kafka Topics will be created automatically:"
echo "  • _validate_email  (for email validation tasks)"
echo "  • _enrich_data     (for data enrichment tasks)"
echo ""
echo "Next steps:"
echo "  1. Start workers: ./start_workers.sh"
echo "  2. Trigger workflow: ./trigger_workflow.sh"
echo ""

