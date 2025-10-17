#!/bin/bash

# Start all microservice workers

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Starting Microservice Workers                                  ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if Conductor is running
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠ Conductor is not running!${NC}"
    echo "Start it with: docker-compose up -d"
    exit 1
fi

echo -e "${GREEN}✓ Conductor is running${NC}"
echo ""

# Check Python dependencies
echo -e "${YELLOW}Checking Python dependencies...${NC}"
if ! python3 -c "import requests" 2>/dev/null; then
    echo -e "${YELLOW}Installing requirements...${NC}"
    pip3 install -r "${PROJECT_DIR}/services/requirements.txt"
fi
echo -e "${GREEN}✓ Dependencies OK${NC}"
echo ""

echo -e "${YELLOW}Starting workers in background...${NC}"
echo ""

# Start email validator
echo -e "Starting ${BLUE}Email Validator${NC}..."
python3 "${PROJECT_DIR}/services/email_validator/worker.py" > /tmp/email_validator.log 2>&1 &
EMAIL_PID=$!
echo -e "  PID: ${EMAIL_PID}"

# Start data enricher
echo -e "Starting ${BLUE}Data Enricher${NC}..."
python3 "${PROJECT_DIR}/services/data_enricher/worker.py" > /tmp/data_enricher.log 2>&1 &
ENRICHER_PID=$!
echo -e "  PID: ${ENRICHER_PID}"

# Save PIDs
echo "$EMAIL_PID" > /tmp/conductor_workers.pid
echo "$ENRICHER_PID" >> /tmp/conductor_workers.pid

echo ""
echo -e "${GREEN}✓ Workers started${NC}"
echo ""
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Workers Running                                                 ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "  1. Email Validator (PID: $EMAIL_PID)"
echo "  2. Data Enricher (PID: $ENRICHER_PID)"
echo ""
echo "View logs:"
echo "  ${YELLOW}tail -f /tmp/email_validator.log${NC}"
echo "  ${YELLOW}tail -f /tmp/data_enricher.log${NC}"
echo ""
echo "Stop workers:"
echo "  ${YELLOW}./scripts/stop_workers.sh${NC}"
echo ""
echo "Start EVENT listener:"
echo "  ${YELLOW}python3 services/event_consumer/kafka_event_listener.py${NC}"
echo ""

