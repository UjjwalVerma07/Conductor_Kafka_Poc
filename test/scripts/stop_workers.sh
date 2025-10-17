#!/bin/bash

# Stop all microservice workers

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Stopping workers...${NC}"

if [ -f /tmp/conductor_workers.pid ]; then
    while read pid; do
        if ps -p $pid > /dev/null 2>&1; then
            kill $pid
            echo -e "${GREEN}✓ Stopped worker (PID: $pid)${NC}"
        fi
    done < /tmp/conductor_workers.pid
    rm /tmp/conductor_workers.pid
    echo -e "${GREEN}All workers stopped${NC}"
else
    echo -e "${YELLOW}No workers running${NC}"
fi

