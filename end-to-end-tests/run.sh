#!/bin/sh
CI_JOB_ID=${CI_JOB_ID:-1}
if [[ -z "$VERSION" ]]; then
  export VERSION="1.1.1"
  echo "************************************************************************************************"
  echo "version was not provided, using 1.1.1 instead as default"
  echo "************************************************************************************************"
fi

cleanup_and_exit() {
    exit_code=$?;
    local error=${1:-Error};
    echo -e "\033[0;31m${error} (code ${exit_code})\033[0m"
    # Display logs before shutting down
    echo "Displaying logs before shutting down the services:"
    docker compose --project-name $CI_JOB_ID logs
    # Always bring down the Docker Compose setup
    docker compose --project-name $CI_JOB_ID down -v  --rmi local
    exit $exit_code
}

# Run Docker Compose up with the specified options, capturing the exit code
docker compose --project-name $CI_JOB_ID build || cleanup_and_exit 'Test BUILD failed'
docker compose --project-name $CI_JOB_ID up -d || cleanup_and_exit 'Test SETUP failed'

SERVICE=tests
docker compose --project-name $CI_JOB_ID up --exit-code-from $SERVICE --scale $SERVICE=1 $SERVICE
exit_code=$?

# Show the exit code of the 'tests' container
echo "Exit code from tests container: $exit_code"

# Display logs before shutting down
echo "Displaying logs before shutting down the services:"
docker compose --project-name $CI_JOB_ID logs

# Always bring down the Docker Compose setup
docker compose --project-name $CI_JOB_ID down -v  --rmi local

# Display colored text based on the exit code
if [ $exit_code -eq 0 ]; then
    echo -e "\033[0;32mTests were successful.\033[0m"
else
    echo -e "\033[0;31mTests failed.\033[0m"
fi

# Exit with the exit code from the tests container
exit $exit_code
