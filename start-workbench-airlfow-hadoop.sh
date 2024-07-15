#!/bin/bash

# Function to make the script executable and re-run it
make_executable_and_rerun() {
    echo "Making the script executable..."
    chmod +x "$0"
    echo "Re-running the script..."
    exec "$0"
}

# Check if the script is executable
if [ ! -x "$0" ]; then
    make_executable_and_rerun
    exit 0
fi

# Navigate to the directory containing the docker-compose file
cd "$(dirname "$0")"

# Export the environment variables from .env file
if [ -f orchestrator/.env ]; then
    export $(cat orchestrator/.env | xargs)
fi

# Ensure the AIRFLOW_UID environment variable is set
if [ -z "$AIRFLOW_UID" ]; then
    echo "The AIRFLOW_UID variable is not set. Please set it in the .env file."
    exit 1
fi

#Create Docker network
docker network create my_shared_network


# Build the Docker images
echo "Building Docker images for "
docker-compose -f orchestrator/airflow-docker-config/docker-compose.yml build postgres namenode datanode

echo "Building Docker images for airflow"
docker-compose -f orchestrator/airflow-docker-config/docker-compose.yml build


# Start the Docker containers
echo "Starting Docker containers..."

docker-compose -f docker-compose.yml up -d postgres namenode datanode

docker-compose -f orchestrator/airflow-docker-config/docker-compose.yml up -d

# Check the status of the Docker containers
echo "Checking the status of Docker containers..."
docker-compose -f orchestrator/airflow-docker-config/docker-compose.yml ps
