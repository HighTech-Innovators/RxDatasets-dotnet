#!/bin/bash
# THIS FILE IS MEANT TO BE RUN USING THE run_me.sh FILE
set -e

# Check if server addresses and options are provided
if [[ "$#" -lt 1 ]]; then
  echo "Usage: $0 <server1-address> [<server2-address> | --generate-data]"
  exit 1
fi

server1=$1
server2=
generate_data=false

# Check for --generate-data option
if [[ "$2" == "--generate-data" ]]; then
  generate_data=true
elif [[ -n "$2" ]]; then
  server2=$2
fi

if [[ -z "$server2" && $generate_data == false ]]; then
  echo "Server2 address must be provided unless --generate-data option is used."
  exit 1
fi

# Install curl
if ! command -v curl &> /dev/null
then
    echo "curl could not be found, installing..."
    sudo apt-get update && sudo apt-get install -y curl
fi

# Install grpcurl
if ! command -v grpcurl &> /dev/null
then
    echo "grpcurl could not be found, installing..."
    curl -sSL "https://github.com/fullstorydev/grpcurl/releases/download/v1.9.1/grpcurl_1.9.1_linux_arm64.tar.gz" | sudo tar -xz -C /usr/local/bin
fi

# Install jq
if ! command -v jq &> /dev/null
then
    echo "jq could not be found, installing..."
    sudo apt-get update && sudo apt-get install -y jq
fi

timeout=120 #seconds
start_time=$(date +%s)

# Function to check if a server is up
check_server() {
  local server=$1
  local server_host=${server%%:*}  # Extract the hostname from server (e.g., dtoserver-actual)

  while true; do
    grpcurl_ok=false
    healthz_ok=false

    # Check grpcurl readiness
    if grpcurl -plaintext $server list > /dev/null 2>&1; then
      grpcurl_ok=true
    else
      echo "grpcurl check failed for $server"
    fi

    # Check /healthz endpoint
    healthz_response=$(curl -s http://$server_host:18080/healthz || echo "")
    if [ "$healthz_response" == "Healthy" ]; then
      healthz_ok=true
    else
      echo "/healthz endpoint check failed for $server_host:18080"
    fi

    if $grpcurl_ok && $healthz_ok; then
      echo "Server $server is up and healthy."
      break
    fi

    current_time=$(date +%s)
    elapsed_time=$(( current_time - start_time ))
    if [ $elapsed_time -ge $timeout ]; then
      echo "Timeout waiting for $server to start and be healthy. Exiting with error."
      exit 1
    fi
    echo "Waiting for $server to start and be healthy..."
    sleep 5
  done

  echo "Server $server is responding to grpcurl and /healthz, waiting additional 5 seconds for full initialization..."
  sleep 5
}



check_server $server1
echo "Server1 is up and running."
sleep 5  # Add this line

# Check if server2 is provided and check its status
if [ -n "$server2" ]; then
  check_server $server2
  echo "Server2 is up and running."
  sleep 5  # Add this line
elif $generate_data; then
  echo "Running with --generate-data option, Server2 not provided."
else
  echo "Server2 not provided, running without server2."
fi

# Define the base request payload and lists of unique names and some_string values
BASE_PAYLOAD=$(cat "./put_request.json")
UNIQUE_NAMES=("unique_name1" "unique_name2" "unique_name3" "unique_name4" "unique_name5")
SOME_STRINGS=("deserunt1" "deserunt2" "deserunt3" "deserunt4" "deserunt5")

# Function to update the payload fields in the JSON payload
update_payload() {
  local payload=$1
  local new_name=$2
  local new_string=$3
  echo $payload | jq --arg new_name "$new_name" --arg new_string "$new_string" '.name = $new_name | .some_string = $new_string'
}

# Function to verify the response
verify_response() {
  local response=$1
  local expected_added=$2
  local expected_updated=$3
  local num_added=$(echo $response | jq '.num_added_entities')
  local num_updated=$(echo $response | jq '.num_updated_entities')

  if [[ $num_added -eq $expected_added && $num_updated -eq $expected_updated ]]; then
    echo "Response verification passed."
  else
    echo "Response verification failed."
    exit 1
  fi
}

# Function to sort arrays within a JSON object
sort_json_arrays() {
  echo $1 | jq 'def walk(f):
    . as $in
    | if type == "object" then
        reduce keys[] as $key
        ( {}; . + { ($key): ( $in[$key] | walk(f) ) } )
    elif type == "array" then
        map( walk(f) ) | sort
    else
        f
    end;
    walk(.)'
}

# Function to compare JSON objects ignoring order of fields and array elements
compare_json() {
  local json1=$(sort_json_arrays "$1")
  local json2=$(sort_json_arrays "$2")
  diff <(echo $json1 | jq -S .) <(echo $json2 | jq -S .) > /dev/null
}

# Function to compare responses from both servers
compare_server_responses() {
  local name=$1
  local request="{\"name\": \"$name\"}"
  
  local response1=$(grpcurl -plaintext -d "$request" $server1 hightechinnovators.dtos.demodto.v1.ReaderService/GetDemodto)
  local response2=$(grpcurl -plaintext -d "$request" $server2 hightechinnovators.dtos.demodto.v1.ReaderService/GetDemodto)

  if compare_json "$response1" "$response2"; then
    echo "GetDemodto response comparison for $name passed."
  else
    echo "GetDemodto response comparison for $name failed."
    echo "Response from $server1: $response1"
    echo "Response from $server2: $response2"
    exit 1
  fi
}

# Function to verify ListChanges response
verify_list_changes() {
  local request="{\"from_dataset_version\": \"3\"}"
  
  local response1=$(grpcurl -plaintext -d "$request" $server1 hightechinnovators.dtos.demodto.v1.ReaderService/ListChanges)
  local response2=$(grpcurl -plaintext -d "$request" $server2 hightechinnovators.dtos.demodto.v1.ReaderService/ListChanges)

  if compare_json "$response1" "$response2"; then
    echo "ListChanges response comparison passed."
  else
    echo "ListChanges response comparison failed."
    echo "Response from $server1: $response1"
    echo "Response from $server2: $response2"
    exit 1
  fi
}

# Iterate over the list of unique names and some_string values and make gRPC calls to server1
for i in "${!UNIQUE_NAMES[@]}"; do
  echo "Performing gRPC call with name=${UNIQUE_NAMES[$i]} and some_string=${SOME_STRINGS[$i]}..."
  UPDATED_PAYLOAD=$(update_payload "$BASE_PAYLOAD" "${UNIQUE_NAMES[$i]}" "${SOME_STRINGS[$i]}")
  
  # Make the gRPC call and capture the response
  RESPONSE=$(grpcurl -plaintext -d "$UPDATED_PAYLOAD" $server1 hightechinnovators.dtos.demodto.v1.WriterService/Put)
  echo "Response: $RESPONSE"

  # Verify the response
  verify_response "$RESPONSE" 1 0
done

# Iterate over the list of unique names and modified some_string values to test updates
for i in "${!UNIQUE_NAMES[@]}"; do
  NEW_STRING="updated_${SOME_STRINGS[$i]}"
  echo "Performing gRPC call with name=${UNIQUE_NAMES[$i]} and some_string=$NEW_STRING..."
  UPDATED_PAYLOAD=$(update_payload "$BASE_PAYLOAD" "${UNIQUE_NAMES[$i]}" "$NEW_STRING")
  
  # Make the gRPC call and capture the response
  RESPONSE=$(grpcurl -plaintext -d "$UPDATED_PAYLOAD" $server1 hightechinnovators.dtos.demodto.v1.WriterService/Put)
  echo "Response: $RESPONSE"

  # Verify the response
  verify_response "$RESPONSE" 0 1

  # Compare the GetDemodto responses from both servers, if server2 is provided
  if [ -n "$server2" ]; then
    compare_server_responses "${UNIQUE_NAMES[$i]}"
  fi
done

# Verify ListChanges response from both servers, if server2 is provided
if [ -n "$server2" ]; then
  verify_list_changes
fi

echo "All tests passed successfully."
