#!/bin/bash

# Trigger the email processing workflow

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

CONDUCTOR_URL="http://localhost:8080/api"

# Default email if none provided
EMAIL=${1:-"test.user@example.com"}

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Triggering Kafka-backed Workflow                       ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check Conductor
echo -e "${YELLOW}Checking Conductor...${NC}"
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo -e "${RED}✗ Conductor is not running!${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Conductor is healthy${NC}"
echo ""

# Start workflow
echo -e "${YELLOW}Starting workflow with email: ${EMAIL}${NC}"
WORKFLOW_ID=$(curl -s -X POST "${CONDUCTOR_URL}/workflow/email_processing_workflow" \
  -H "Content-Type: application/json" \
  -d "{
    \"email\": \"${EMAIL}\"
  }" | tr -d '"')

if [ -n "$WORKFLOW_ID" ] && [ "$WORKFLOW_ID" != "null" ]; then
    echo -e "${GREEN}✓ Workflow started successfully${NC}"
    echo ""
    echo -e "${BLUE}Workflow ID: ${WORKFLOW_ID}${NC}"
    echo ""
    
    # Wait a moment for initial processing
    sleep 2
    
    # Get workflow status
    echo -e "${YELLOW}Fetching workflow status...${NC}"
    STATUS=$(curl -s "${CONDUCTOR_URL}/workflow/${WORKFLOW_ID}" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
    echo -e "Status: ${GREEN}${STATUS}${NC}"
    echo ""
    
    # Display monitoring info
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║   Monitoring Information                                  ║${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "📊 Conductor UI:"
    echo -e "   ${GREEN}http://localhost:8080${NC}"
    echo -e "   Navigate to 'Executions' and search for: ${BLUE}${WORKFLOW_ID}${NC}"
    echo ""
    echo "📨 Kafka Topics (check these topics for messages):"
    echo "   • _validate_email"
    echo "   • _enrich_data"
    echo ""
    echo "🔍 View workflow details (JSON):"
    echo -e "   ${BLUE}curl ${CONDUCTOR_URL}/workflow/${WORKFLOW_ID} | jq${NC}"
    echo ""
    echo "📋 View task queue status:"
    echo -e "   ${BLUE}curl ${CONDUCTOR_URL}/tasks/queue/all${NC}"
    echo ""
    echo "⚠️  Make sure your workers are running!"
    echo "   If not started, run: ./start_workers.sh"
    echo ""
else
    echo -e "${RED}✗ Failed to start workflow${NC}"
    echo "Response: $WORKFLOW_ID"
    exit 1
fi

