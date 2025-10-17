#!/bin/bash

# Trigger EVENT-based workflows

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

CONDUCTOR_URL="http://localhost:8080/api"

# Default values
EMAIL=${1:-"test.user@example.com"}
WORKFLOW=${2:-"kafka_event_workflow"}

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Triggering Kafka EVENT Workflow                                ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check Conductor
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo -e "${RED}✗ Conductor is not running!${NC}"
    exit 1
fi

echo -e "${YELLOW}Workflow: ${WORKFLOW}${NC}"
echo -e "${YELLOW}Email:    ${EMAIL}${NC}"
echo ""

# Start workflow
echo -e "${YELLOW}Starting workflow...${NC}"
WORKFLOW_ID=$(curl -s -X POST "${CONDUCTOR_URL}/workflow/${WORKFLOW}" \
  -H "Content-Type: application/json" \
  -d "{
    \"email\": \"${EMAIL}\"
  }" | tr -d '"')

if [ -n "$WORKFLOW_ID" ] && [ "$WORKFLOW_ID" != "null" ]; then
    echo -e "${GREEN}✓ Workflow started${NC}"
    echo ""
    echo -e "${BLUE}Workflow ID: ${WORKFLOW_ID}${NC}"
    echo ""
    
    sleep 2
    
    # Get status
    STATUS=$(curl -s "${CONDUCTOR_URL}/workflow/${WORKFLOW_ID}" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
    echo -e "Status: ${GREEN}${STATUS}${NC}"
    echo ""
    
    # Display info
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║   Monitoring                                                      ║${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "📊 Conductor UI:"
    echo -e "   ${GREEN}http://localhost:8080${NC}"
    echo ""
    echo "📨 Check EVENT listener output for Kafka events"
    echo ""
    echo "🔍 View workflow:"
    echo -e "   ${BLUE}curl ${CONDUCTOR_URL}/workflow/${WORKFLOW_ID} | jq${NC}"
    echo ""
    echo "Available workflows:"
    echo "  • kafka_event_workflow (hybrid EVENT + SIMPLE)"
    echo "  • pure_kafka_event_workflow (pure EVENT)"
    echo "  • email_processing_workflow (standard SIMPLE)"
    echo ""
    echo "Trigger another workflow:"
    echo -e "  ${BLUE}./trigger_event_workflow.sh user@test.com pure_kafka_event_workflow${NC}"
    echo ""
else
    echo -e "${RED}✗ Failed to start workflow${NC}"
    exit 1
fi

