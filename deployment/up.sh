#!/bin/bash

# Check if an argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 {up|down}"
    exit 1
fi

# Set the Docker Compose file and environment file
COMPOSE_FILE="de-identification.yml"
ENV_FILE="de-identification.env"
PROJECT="docker"
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
