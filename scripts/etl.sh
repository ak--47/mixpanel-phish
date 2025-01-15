#!/bin/bash
rm -rf ./tmp/*
export NODE_OPTIONS="--max-old-space-size=32768"
export NODE_ENV="dev"
# Set script to exit on any error
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to run and time a script
run_script() {
    local script=$1
	shift

    
    echo -e "\n${BLUE}Starting $script with ...${NC}"
    # shellcheck disable=SC2155
    local start_time=$(date +%s)
    
    node "$script" "$@"
    
    # shellcheck disable=SC2155
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    echo -e "${GREEN}Finished $script - Duration: $duration seconds${NC}"
}

echo -e "\n${BLUE}Starting ETL pipeline...${NC}\n"
total_start_time=$(date +%s)

# Extract
run_script "./components/extract.js" "full"

# Transform
run_script "./components/transform.js"

# Load
run_script "./components/load.js"

total_end_time=$(date +%s)
total_duration=$((total_end_time - total_start_time))

echo -e "\n${GREEN}ETL pipeline completed successfully${NC}\n"
echo -e "\n${GREEN}Total duration: $total_duration seconds${NC}\n"

# rm -rf ./tmp/*
 