#!/bin/bash

# Check if an argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 {up|down}"
    exit 1
fi


# Set the Docker Compose file and environment file
# COMPOSE_FILE="de-identification.yml"
COMPOSE_FILE="workers.yaml"
ENV_FILE="de-identification.env"
PROJECT="portal"

# Execute the appropriate Docker Compose command based on the input argument
case "$1" in
    up)
        docker-compose -p "$PROJECT" -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d
        ;;
    down)
        docker-compose  -p "$PROJECT" -f "$COMPOSE_FILE" --env-file "$ENV_FILE" down
        ;;
    *)
        echo "Invalid option: $1"
        echo "Usage: $0 {up|down}"
        exit 1
        ;;
esac

# Start containers using the generated docker-compose file
#docker-compose -f $COMPOSE_FILE up -d

# Define the network name
NETWORK_NAME="deidentification"

# Connect core services
docker network connect $NETWORK_NAME api || true
docker network connect $NETWORK_NAME notebook || true

# Connect all 20 workers
for i in {1..20}; do
  CONTAINER_NAME="worker-$i"
  docker network connect $NETWORK_NAME $CONTAINER_NAME || true
done

echo "All containers connected to $NETWORK_NAME network successfully."
