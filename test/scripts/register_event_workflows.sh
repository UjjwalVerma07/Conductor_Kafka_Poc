#!/bin/bash

# Register EVENT-based workflows with Conductor

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

CONDUCTOR_URL="http://localhost:8080/api"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Registering Kafka EVENT-based Workflows                        ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check Conductor health
echo -e "${YELLOW}Checking Conductor...${NC}"
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo -e "${RED}✗ Conductor is not running!${NC}"
    echo "Start it with: docker-compose up -d"
    exit 1
fi
echo -e "${GREEN}✓ Conductor is running${NC}"
echo ""

# Register task definitions
echo -e "${YELLOW}Step 1: Registering task definitions...${NC}"
curl -s -X POST "${CONDUCTOR_URL}/metadata/taskdefs" \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/../workflows/task_definitions.json" > /dev/null

echo -e "${GREEN}✓ Task definitions registered${NC}"
echo "  • validate_email"
echo "  • enrich_data"
echo ""

# Register hybrid EVENT + SIMPLE workflow
echo -e "${YELLOW}Step 2: Registering hybrid EVENT workflow...${NC}"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${CONDUCTOR_URL}/metadata/workflow" \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/../workflows/event_workflow.json")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 204 ] || [ "$HTTP_CODE" -eq 409 ]; then
    echo -e "${GREEN}✓ Hybrid EVENT workflow registered${NC}"
    echo "  Workflow: kafka_event_workflow"
else
    echo -e "${RED}✗ Failed (HTTP $HTTP_CODE)${NC}"
fi
echo ""

# Register pure EVENT workflow
echo -e "${YELLOW}Step 3: Registering pure EVENT workflow...${NC}"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${CONDUCTOR_URL}/metadata/workflow" \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/../workflows/pure_event_workflow.json")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 204 ] || [ "$HTTP_CODE" -eq 409 ]; then
    echo -e "${GREEN}✓ Pure EVENT workflow registered${NC}"
    echo "  Workflow: pure_kafka_event_workflow"
else
    echo -e "${RED}✗ Failed (HTTP $HTTP_CODE)${NC}"
fi
echo ""

# Register standard workflow
echo -e "${YELLOW}Step 4: Registering standard workflow...${NC}"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${CONDUCTOR_URL}/metadata/workflow" \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/../workflows/email_processing_workflow.json")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 204 ] || [ "$HTTP_CODE" -eq 409 ]; then
    echo -e "${GREEN}✓ Standard workflow registered${NC}"
    echo "  Workflow: email_processing_workflow"
else
    echo -e "${RED}✗ Failed (HTTP $HTTP_CODE)${NC}"
fi
echo ""

echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Registration Complete!                                          ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Registered Workflows:"
echo "  1. ${BLUE}kafka_event_workflow${NC} - Hybrid: EVENT + SIMPLE tasks"
echo "  2. ${BLUE}pure_kafka_event_workflow${NC} - Pure EVENT tasks only"
echo "  3. ${BLUE}email_processing_workflow${NC} - Standard SIMPLE tasks"
echo ""
echo "Kafka Topics that will be created:"
echo "  • conductor.email.validation.requests"
echo "  • conductor.data.enrichment.requests"
echo "  • conductor.workflow.completed"
echo "  • email_validation_events"
echo "  • data_enrichment_events"
echo "  • workflow_completion_events"
echo "  • _validate_email (task queue)"
echo "  • _enrich_data (task queue)"
echo ""
echo "Next steps:"
echo "  ${YELLOW}1.${NC} Start EVENT listener: python3 services/event_consumer/kafka_event_listener.py"
echo "  ${YELLOW}2.${NC} Start workers: python3 services/email_validator/worker.py & python3 services/data_enricher/worker.py"
echo "  ${YELLOW}3.${NC} Trigger workflow: ./scripts/trigger_event_workflow.sh"
echo ""

