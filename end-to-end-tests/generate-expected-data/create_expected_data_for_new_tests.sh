#!/bin/sh

cleanup_and_exit() {
    exit_code=$?;
    local error=${1:-Error};
    echo -e "\033[0;31m${error} (code ${exit_code})\033[0m"
    # Always bring down the Docker Compose setup
    docker compose down -v
    exit $exit_code
}

# Check if the directory exists
if [ -d "./generated-minio-data" ]; then
  echo "Removing existing ./generated-minio-data directory..."
  rm -rf ./generated-minio-data
  echo "./generated-minio-data directory removed."
else
  echo "./generated-minio-data directory does not exist."
fi

# Bring down any existing Docker Compose setup
docker compose down -v

# Run Docker Compose up with the specified options, capturing the exit code
docker compose build || cleanup_and_exit "docker build FAILED"
docker compose up -d || cleanup_and_exit "docker up (prerequisites) FAILED"
SERVICE=generate
docker compose up --exit-code-from $SERVICE --scale $SERVICE=1 $SERVICE
exit_code=$?

# Show the exit code of the 'generate-expected-data' container
echo "Exit code from generation container: $exit_code"

# Always bring down the Docker Compose setup
docker compose down -v

# Display colored text based on the exit code
if [ $exit_code -eq 0 ]; then
    echo -e "\033[0;32mGeneration was successful.\033[0m"
    echo "Don't forget to move the newly generated data to the correct location!"
else
    echo -e "\033[0;32mGeneration failed.\033[0m"
fi

# Exit with the exit code from the generate-expected-data container
exit $exit_code
