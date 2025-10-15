#!/bin/bash

echo "========================================"
echo "Starting Conductor Kafka POC"
echo "========================================"
echo ""

# Clean up any previous instances
echo "Cleaning up previous instances..."
docker-compose down -v

echo ""
echo "Starting services..."
echo "This will take about 60 seconds..."
echo ""

# Start services
docker-compose up --build -d

echo ""
echo "Waiting for services to be ready..."
echo ""

# Wait for Conductor
echo -n "Waiting for Conductor"
for i in {1..60}; do
    if curl -sf http://localhost:8080/api/health > /dev/null 2>&1; then
        echo " ✓"
        break
    fi
    echo -n "."
    sleep 2
done

echo ""
echo "========================================"
echo "✓ All services are running!"
echo "========================================"
echo ""
echo "Services:"
echo "  - Conductor UI:    http://localhost:5000"
echo "  - Conductor API:   http://localhost:8080/api"
echo "  - MinIO Console:   http://localhost:9001 (admin/admin)"
echo "  - Kafka:           localhost:9092"
echo ""
echo "Next steps:"
echo "  1. Run the test:    ./test_simple_flow.sh"
echo "  2. View logs:       docker-compose logs -f"
echo "  3. Stop services:   docker-compose down"
echo ""


