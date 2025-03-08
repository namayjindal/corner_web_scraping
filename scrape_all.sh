#!/bin/bash

# One-click Web Scraping Pipeline for Corner Project
# This script orchestrates all scraping modules in the correct sequence

# Set up colors for better readability
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display section headers
section() {
    echo -e "\n${BLUE}===================================================="
    echo -e "  $1"
    echo -e "====================================================${NC}\n"
}

# Function to run a command and check for errors
run_command() {
    echo -e "${YELLOW}Running: $1${NC}"
    eval $1
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Command completed successfully${NC}"
    else
        echo -e "${RED}✗ Command failed with error code $?${NC}"
        if [ "$2" = "critical" ]; then
            echo -e "${RED}This is a critical error. Pipeline cannot continue.${NC}"
            exit 1
        fi
    fi
    echo ""
}

# Create necessary directories if they don't exist
mkdir -p logs

# Start time
START_TIME=$(date +%s)
section "Starting Web Scraping Pipeline ($(date))"

# Check for Python environment
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python 3 is not installed. Please install Python 3 to continue.${NC}"
    exit 1
fi

# Check for pip and install required packages
section "Checking and installing dependencies"
if ! command -v pip3 &> /dev/null; then
    echo -e "${RED}pip3 is not installed. Please install pip3 to continue.${NC}"
    exit 1
fi

# Install required Python packages
echo -e "${YELLOW}Installing required Python packages...${NC}"
pip3 install pandas requests selenium scrapy beautifulsoup4 psycopg2-binary python-dotenv spacy requests-html > logs/pip_install.log 2>&1
echo -e "${GREEN}✓ Dependencies installed${NC}"

# Download spaCy model if needed
if [ ! -d "$(python3 -m spacy info en_core_web_sm 2>/dev/null | grep Location | cut -d' ' -f2)/en_core_web_sm" ]; then
    echo -e "${YELLOW}Downloading spaCy English model...${NC}"
    python3 -m spacy download en_core_web_sm > logs/spacy_download.log 2>&1
    echo -e "${GREEN}✓ spaCy model downloaded${NC}"
fi

# Check for PostgreSQL
if ! command -v psql &> /dev/null; then
    echo -e "${YELLOW}PostgreSQL client not found. Database operations may fail.${NC}"
    echo -e "${YELLOW}Recommend installing PostgreSQL before continuing.${NC}"
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check for .env file
if [ ! -f ".env" ]; then
    section "Creating .env file"
    echo -e "${YELLOW}No .env file found. Creating a template...${NC}"
    cat > .env << EOL
# API Keys
OPENAI_KEY=your_openai_key_here

# Database Configuration
DB_USER=namayjindal
DB_PASSWORD=
DB_HOST=localhost
DB_NAME=corner_db
EOL
    echo -e "${GREEN}✓ .env template created${NC}"
    echo -e "${YELLOW}Please edit the .env file with your actual API keys and database credentials${NC}"
    read -p "Edit now and continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
    ${EDITOR:-nano} .env
fi

# 1. Initialize Database
section "Initializing PostgreSQL Database"
run_command "psql -c 'CREATE DATABASE corner_db;' || true" "non-critical"
run_command "python3 postgres_migration.py setup_only" "critical"

# 2. Run Google Places Scraper
section "Running Google Places Scraper"
run_command "cd google_places && python3 google_scraper.py" "non-critical"
echo -e "${GREEN}✓ Google Places data has been scraped${NC}"

# 3. Run OpenTable Scraper
section "Running OpenTable Scraper"
run_command "cd opentable && python3 scrape.py" "non-critical"
echo -e "${GREEN}✓ OpenTable data has been scraped${NC}"

# 4. Run Resy Scraper
section "Running Resy Scraper"
run_command "python3 resy/scrape.py" "non-critical"
echo -e "${GREEN}✓ Resy data has been scraped${NC}"

# 5. Run OpenStreetMap Scraper
section "Running OpenStreetMap Scraper"
run_command "cd osm && python3 scrape.py" "non-critical"
echo -e "${GREEN}✓ OpenStreetMap data has been scraped${NC}"

# 6. Run Website Scraper
section "Running Website Content Scraper"
run_command "python3 website_scraper.py" "non-critical"
echo -e "${GREEN}✓ Website content has been scraped${NC}"

# 7. Process all data and migrate to database
section "Migrating all data to PostgreSQL database"
run_command "python3 postgres_migration.py" "critical"
echo -e "${GREEN}✓ All data has been migrated to the database${NC}"

# 8. Generate embeddings for vector search
section "Generating embeddings for vector search"
run_command "python3 generate_embeddings.py" "non-critical"
echo -e "${GREEN}✓ Vector embeddings have been generated${NC}"

# 9. Run data validation
section "Validating data in the database"
run_command "python3 data_validation.py" "non-critical"
echo -e "${GREEN}✓ Data validation completed${NC}"

# End time and summary
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

section "Pipeline Completed Successfully!"
echo -e "${GREEN}Total execution time: ${MINUTES}m ${SECONDS}s${NC}"
echo -e "${GREEN}All data has been scraped and stored in the PostgreSQL database.${NC}"
echo -e "\n${YELLOW}To explore the database, run:${NC}"
echo -e "  psql corner_db"
echo -e "\n${YELLOW}To test the embeddings-based vector search, run:${NC}"
echo -e "  python3 search_test.py \"your search query here\""
echo -e "\n${GREEN}Data files generated:${NC}"
echo -e "  - places_with_google_data.csv: Google Places data"
echo -e "  - opentable_results.csv: OpenTable restaurant data"
echo -e "  - resy_data.json: Resy restaurant data"
echo -e "  - places_with_osm.csv: OpenStreetMap location data"
echo -e "  - scraped_data.json: Website metadata"
echo -e "  - combined_data.json: All data merged"

# Print a fun message
echo -e "\n${GREEN}============================================="
echo -e "   🎉 Happy exploring your restaurant data! 🎉"
echo -e "==============================================${NC}"