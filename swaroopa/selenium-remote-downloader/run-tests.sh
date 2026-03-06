#!/bin/bash

# Remote File Download Cucumber Test Runner
# Usage: ./run-tests.sh [options]

set -e

# Default values
GRID_URL="${GRID_URL:-http://localhost:4444/wd/hub}"
LOCAL_DOWNLOAD_PATH="${LOCAL_DOWNLOAD_PATH:-./downloads}"
DOWNLOAD_TIMEOUT="${DOWNLOAD_TIMEOUT:-60}"
TAGS="${TAGS:-@download}"
HEADLESS="${HEADLESS:-true}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Remote File Download Cucumber Tests${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Display configuration
echo -e "${YELLOW}Configuration:${NC}"
echo "  Grid URL: $GRID_URL"
echo "  Local Download Path: $LOCAL_DOWNLOAD_PATH"
echo "  Timeout: $DOWNLOAD_TIMEOUT seconds"
echo "  Tags: $TAGS"
echo "  Headless: $HEADLESS"
echo ""

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo -e "${RED}Error: Maven is not installed${NC}"
    echo "Please install Maven first: https://maven.apache.org/install.html"
    exit 1
fi

# Create download directory
mkdir -p "$LOCAL_DOWNLOAD_PATH"
echo -e "${GREEN}✓${NC} Download directory created: $LOCAL_DOWNLOAD_PATH"

# Run tests
echo ""
echo -e "${YELLOW}Running tests...${NC}"
echo ""

mvn clean test \
  -Dgrid.url="$GRID_URL" \
  -Dlocal.download.path="$LOCAL_DOWNLOAD_PATH" \
  -Ddownload.timeout="$DOWNLOAD_TIMEOUT" \
  -Dcucumber.filter.tags="$TAGS" \
  -Dheadless="$HEADLESS"

TEST_EXIT_CODE=$?

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✓ Tests completed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
else
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}✗ Tests failed!${NC}"
    echo -e "${RED}========================================${NC}"
fi

# Show report location
echo ""
echo -e "${YELLOW}Reports:${NC}"
echo "  HTML: target/cucumber-reports/cucumber.html"
echo "  Extent: test-output/ExtentReports/ExtentReport.html"
echo ""

exit $TEST_EXIT_CODE
